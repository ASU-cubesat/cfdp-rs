use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use error::DaemonResult;
use log::{error, info, warn};
use tokio::{select, task::JoinHandle, time::MissedTickBehavior};

// Re-exported for convenience
pub use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};

use cfdp_core::{
    daemon::{EntityConfig, Indication, PutRequest, Report, UserPrimitive},
    filestore::FileStore,
    pdu::{
        Direction, EntityID, FileSizeFlag, NakOrKeepAlive, PDUHeader, SegmentedData,
        TransactionSeqNum, VariableID, PDU,
    },
    transaction::{Metadata, TransactionConfig, TransactionID, TransactionState},
};

pub use cfdp_core::*;

pub mod error;
pub(crate) mod segments;
pub(crate) mod timer;
pub mod transaction;
pub mod transport;

use self::error::DaemonError;

use self::transport::PDUTransport;
use transaction::{recv::RecvTransaction, send::SendTransaction, TransactionError};

/// Lightweight commands the Daemon send to each Transaction
#[derive(Debug)]
pub enum Command {
    Pdu(PDU),
    Cancel,
    Suspend,
    Resume,
    Report(oneshot::Sender<Report>),
    Prompt(NakOrKeepAlive),
    // may find a use for abandon in the future.
    #[allow(unused)]
    Abandon,
}
impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

fn construct_metadata<T: FileStore + Send + 'static>(
    filestore: &Arc<T>,
    req: PutRequest,
    config: EntityConfig,
) -> DaemonResult<Metadata> {
    let file_size = match req.source_filename.file_name().is_none() {
        true => 0_u64,
        false => filestore
            .get_size(&req.source_filename)
            .map_err(DaemonError::SpawnSend)?,
    };
    Ok(Metadata {
        source_filename: req.source_filename,
        destination_filename: req.destination_filename,
        file_size,
        filestore_requests: req.filestore_requests,
        message_to_user: req.message_to_user,
        closure_requested: config.closure_requested,
        checksum_type: config.checksum_type,
    })
}

type RecvSpawnerTuple = (
    TransactionID,
    Sender<Command>,
    JoinHandle<Result<TransactionID, TransactionError>>,
);

type SendSpawnerTuple = (
    Sender<Command>,
    JoinHandle<Result<TransactionID, TransactionError>>,
);

/// The CFDP Daemon is responsible for connecting [PDUTransport](crate::transport::PDUTransport) implementation
/// with each individual [SendTransaction](crate::transaction::SendTransaction) and [RecvTransaction](crate::transaction::RecvTransaction).
/// When a PDUTransport implementation
/// sends a PDU through a channel, the Daemon distributes the PDU to the necessary Transaction.
/// PDUs are sent from each Transaction directly to their respective PDUTransport implementations.
pub struct Daemon<T: FileStore + Send + 'static> {
    // The collection of all current transactions
    transaction_handles: Vec<JoinHandle<Result<TransactionID, TransactionError>>>,
    // Mapping of unique transaction ids to channels used to talk to each transaction
    transaction_channels: HashMap<TransactionID, Sender<Command>>,
    // the vector of transportation tx channel connections
    transport_tx_map: HashMap<EntityID, Sender<(VariableID, PDU)>>,
    // the transport PDU rx channel connection
    transport_rx: Receiver<PDU>,
    // the underlying filestore used by this Daemon
    filestore: Arc<T>,
    // message sender channel used to send Indications from Transactions to the User
    indication_tx: Sender<Indication>,
    // a mapping of individual fault handler actions per remote entity
    entity_configs: HashMap<VariableID, EntityConfig>,
    // the default fault handling configuration
    default_config: EntityConfig,
    // the entity ID of this daemon
    entity_id: EntityID,
    // current running count of the sequence numbers of transaction initiated by this entity
    sequence_num: TransactionSeqNum,
    // termination signal sent to children threads
    terminate: Arc<AtomicBool>,
    // channel to receive user primitives from the implemented User
    primitive_rx: Receiver<UserPrimitive>,
}
impl<T: FileStore + Send + Sync + 'static> Daemon<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        entity_id: EntityID,
        sequence_num: TransactionSeqNum,
        transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
        filestore: Arc<T>,
        entity_configs: HashMap<VariableID, EntityConfig>,
        default_config: EntityConfig,
        primitive_rx: Receiver<UserPrimitive>,
        indication_tx: Sender<Indication>,
    ) -> Self {
        let mut transport_tx_map: HashMap<EntityID, Sender<(VariableID, PDU)>> = HashMap::new();
        let (pdu_send, pdu_receive) = channel(100);
        let terminate = Arc::new(AtomicBool::new(false));
        for (vec, mut transport) in transport_map.into_iter() {
            let (remote_send, remote_receive) = channel(1);

            vec.iter().for_each(|id| {
                transport_tx_map.insert(*id, remote_send.clone());
            });

            let signal = terminate.clone();
            let sender = pdu_send.clone();
            tokio::task::spawn(async move {
                transport.pdu_handler(signal, sender, remote_receive).await
            });
        }
        Self {
            transaction_handles: vec![],
            transaction_channels: HashMap::new(),
            transport_tx_map,
            transport_rx: pdu_receive,
            filestore,
            indication_tx,
            entity_configs,
            default_config,
            entity_id,
            sequence_num,
            terminate,
            primitive_rx,
        }
    }

    fn spawn_receive_transaction(
        header: &PDUHeader,
        transport_tx: Sender<(VariableID, PDU)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        indication_tx: Sender<Indication>,
    ) -> RecvSpawnerTuple {
        let (transaction_tx, mut transaction_rx) = channel(100);

        let config = TransactionConfig {
            source_entity_id: header.source_entity_id,
            destination_entity_id: header.destination_entity_id,
            transmission_mode: header.transmission_mode,
            sequence_number: header.transaction_sequence_number,
            file_size_flag: header.large_file_flag,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: header.crc_flag,
            segment_metadata_flag: header.segment_metadata_flag,
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.inactivity_timeout,
            ack_timeout: entity_config.ack_timeout,
            nak_timeout: entity_config.nak_timeout,
        };
        /*  let name = format!(
            "({}, {})",
            &config.source_entity_id, &config.sequence_number
        );*/
        let mut transaction = RecvTransaction::new(
            config,
            entity_config.nak_procedure,
            filestore,
            indication_tx,
        );
        let id = transaction.id();

        // tokio tasks can have names but that seems an unsable feature
        let handle = tokio::task::spawn(async move {
            transaction.send_report(None)?;

            while transaction.get_state() != TransactionState::Terminated {
                let timeout = transaction.until_timeout();
                select! {
                    Ok(permit) = transport_tx.reserve(), if transaction.has_pdu_to_send() => {
                        transaction.send_pdu(permit)?
                    },
                    Some(command) = transaction_rx.recv() => {
                        match command {
                            Command::Pdu(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(err @ TransactionError::UnexpectedPDU(..)) => {
                                        info!("Transaction {} Received Unexpected PDU: {err}", transaction.id());
                                        // log some info on the unexpected PDU?
                                    }
                                    Err(err) => return Err(err),
                                }
                            }
                            Command::Resume => transaction.resume()?,
                            Command::Cancel => transaction.cancel()?,
                            Command::Suspend => transaction.suspend()?,
                            Command::Abandon => transaction.shutdown(),
                            Command::Report(sender) => {
                                transaction.send_report(Some(sender))?
                            }
                            Command::Prompt(_) =>{
                                // prompt is a no-op for a receive transaction.
                            }
                        }
                    }
                    _ = tokio::time::sleep(timeout) => {
                        transaction.handle_timeout()?;
                    }
                    else => {
                        if transport_tx.is_closed(){
                            log::error!("Channel to transport unexpectedly severed for transaction {}.", transaction.id());
                        }

                        break;
                    }
                };
            }

            transaction.send_report(None)?;
            Ok(transaction.id())
        });

        (id, transaction_tx, handle)
    }

    fn spawn_send_transaction(
        request: PutRequest,
        transaction_id: TransactionID,
        transport_tx: Sender<(EntityID, PDU)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        indication_tx: Sender<Indication>,
    ) -> DaemonResult<SendSpawnerTuple> {
        let (transaction_tx, mut transaction_rx) = channel(10);

        let destination_entity_id = request.destination_entity_id;
        let transmission_mode = request.transmission_mode;
        let mut config = TransactionConfig {
            source_entity_id: transaction_id.0,
            destination_entity_id,
            transmission_mode,
            sequence_number: transaction_id.1,
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: entity_config.crc_flag,
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.inactivity_timeout,
            ack_timeout: entity_config.ack_timeout,
            nak_timeout: entity_config.nak_timeout,
        };
        let metadata = construct_metadata(&filestore, request, entity_config)?;

        let handle = tokio::task::spawn(async move {
            config.file_size_flag = match metadata.file_size <= u32::MAX.into() {
                true => FileSizeFlag::Small,
                false => FileSizeFlag::Large,
            };

            let mut transaction = SendTransaction::new(config, metadata, filestore, indication_tx)?;
            transaction.send_report(None)?;

            while transaction.get_state() != TransactionState::Terminated {
                let timeout = transaction.until_timeout();

                select! {
                    Ok(permit) = transport_tx.reserve(), if transaction.has_pdu_to_send()  => {
                        transaction.send_pdu(permit)?;
                    },

                    Some(command) = transaction_rx.recv() => {
                        match command {
                            Command::Pdu(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(
                                        err @ TransactionError::UnexpectedPDU(..),
                                    ) => {
                                        info!("Recieved Unexpected PDU: {err}");
                                        // log some info on the unexpected PDU?
                                    }
                                    Err(err) => {
                                        return Err(err);
                                    }
                                }
                            }
                            Command::Resume => transaction.resume()?,
                            Command::Cancel => transaction.cancel()?,
                            Command::Suspend => transaction.suspend()?,
                            Command::Abandon => transaction.shutdown(),
                            Command::Report(sender) => {
                                transaction.send_report(Some(sender))?
                            }
                            Command::Prompt(option) => {
                                transaction.prepare_prompt(option)
                            }
                        }
                    },
                    _ = tokio::time::sleep(timeout) => {
                        transaction.handle_timeout()?;
                    },
                    else => {
                        if transport_tx.is_closed(){
                            log::error!("Connection to transport unexpectedly severed for transaction {}.", transaction.id());
                        }
                        break;
                    }
                };
            }
            transaction.send_report(None)?;
            Ok(transaction_id)
        });
        Ok((transaction_tx, handle))
    }

    async fn process_primitive(&mut self, primitive: UserPrimitive) -> DaemonResult<()> {
        match primitive {
            UserPrimitive::Put(request, put_sender) => {
                let sequence_number = self.sequence_num.get_and_increment();

                let entity_config = self
                    .entity_configs
                    .get(&request.destination_entity_id)
                    .unwrap_or(&self.default_config)
                    .clone();

                if let Some(transport_tx) = self
                    .transport_tx_map
                    .get(&request.destination_entity_id)
                    .cloned()
                {
                    let id = TransactionID(self.entity_id, sequence_number);
                    let (sender, handle) = Self::spawn_send_transaction(
                        request,
                        id,
                        transport_tx,
                        entity_config,
                        self.filestore.clone(),
                        self.indication_tx.clone(),
                    )?;
                    self.transaction_handles.push(handle);
                    self.transaction_channels.insert(id, sender);

                    // ignore the possible error if the user disconnected;
                    let _ = put_sender.send(id);
                } else {
                    warn!(
                        "No Transport available for EntityID: {}. Skipping transaction creation.",
                        request.destination_entity_id
                    )
                }
            }
            UserPrimitive::Cancel(id) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Cancel)
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
            UserPrimitive::Suspend(id) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Suspend)
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
            UserPrimitive::Resume(id) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Resume)
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
            UserPrimitive::Report(id, report_sender) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Report(report_sender))
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
            UserPrimitive::Prompt(id, option) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Prompt(option))
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
        };
        Ok(())
    }

    async fn forward_pdu(&mut self, pdu: PDU) -> DaemonResult<()> {
        // find the entity this entity will be sending too.
        // If this PDU is to the sender, we send to the destination
        // if this PDU is to the receiver, we send to the source
        let transport_entity = match &pdu.header.direction {
            Direction::ToSender => pdu.header.destination_entity_id,
            Direction::ToReceiver => pdu.header.source_entity_id,
        };

        let key = TransactionID(
            pdu.header.source_entity_id,
            pdu.header.transaction_sequence_number,
        );
        // hand pdu off to transaction
        let channel = match self.transaction_channels.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if let Some(transport) = self.transport_tx_map.get(&transport_entity).cloned() {
                    // if this key is not in the channel list
                    // create a new transaction
                    let entity_config = self
                        .entity_configs
                        .get(&key.0)
                        .unwrap_or(&self.default_config)
                        .clone();
                    let (_id, channel, handle) = Self::spawn_receive_transaction(
                        &pdu.header,
                        transport,
                        entity_config,
                        self.filestore.clone(),
                        self.indication_tx.clone(),
                    );

                    self.transaction_handles.push(handle);
                    entry.insert(channel)
                } else {
                    warn!(
                        "No Transport available for EntityID: {}. Skipping Transaction creation.",
                        transport_entity
                    );
                    // skip to the next loop iteration
                    return Ok(());
                }
            }
        };

        if channel.send(Command::Pdu(pdu.clone())).await.is_err() {
            // the transaction is completed.
            // spawn a new one
            // this is very unlikely and only results
            // if a sender is re-using a transaction id
            let entity_config = self
                .entity_configs
                .get(&key.0)
                .unwrap_or(&self.default_config)
                .clone();
            if let Some(transport) = self.transport_tx_map.get(&transport_entity).cloned() {
                let (id, new_channel, handle) = Self::spawn_receive_transaction(
                    &pdu.header,
                    transport,
                    entity_config,
                    self.filestore.clone(),
                    self.indication_tx.clone(),
                );
                self.transaction_handles.push(handle);
                new_channel
                    .send(Command::Pdu(pdu.clone()))
                    .await
                    .map_err(|err| DaemonError::from((id, err)))?;
                // update the dict to have the new channel
                self.transaction_channels.insert(key, new_channel);
            } else {
                warn!(
                    "No Transport available for EntityID: {}. Skipping Transaction creation.",
                    transport_entity
                )
            }
        }
        Ok(())
    }

    async fn cleanup_transactions(&mut self) {
        // join any handles that have completed
        let mut ind = 0;
        while ind < self.transaction_handles.len() {
            if self.transaction_handles[ind].is_finished() {
                let handle = self.transaction_handles.remove(ind);
                match handle.await {
                    Ok(Ok(id)) => {
                        // remove the channel for this transaction if it is complete
                        let _ = self.transaction_channels.remove(&id);
                    }
                    Ok(Err(err)) => {
                        info!("Error occurred during transaction: {}", err)
                    }
                    Err(_) => error!("Unable to join handle!"),
                };
            } else {
                ind += 1;
            }
        }
    }

    /// This function will consist of the main logic loop in any daemon process.
    pub async fn manage_transactions(&mut self) -> DaemonResult<()> {
        let cleanup = {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            // Don't start counting another tick until the currrent one has been processed.
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        };
        tokio::pin!(cleanup);

        loop {
            select! {
                pdu = self.transport_rx.recv() => match pdu {
                    Some(pdu) => match self.forward_pdu(pdu).await{
                        Ok(_) => {},
                        Err(error @ DaemonError::TransactionCommuncation(_, _)) => {
                            // This occcurs most likely if a user is attempting to
                            // interact with a transaction that is already finished.
                            warn!("{error}")
                        }
                        Err(err) => return Err(err),
                    },
                    None => {
                        if !self.terminate.load(Ordering::Relaxed) {
                            error!("Transport unexpectedly disconnected from daemon.");
                            self.terminate.store(true, Ordering::Relaxed);
                        }
                        break;
                    }
                },
                primitive = self.primitive_rx.recv() => match primitive {
                    Some(primitive) => match self.process_primitive(primitive).await{
                        Ok(_) => {},
                        Err(error @ DaemonError::TransactionCommuncation(_, _)) => {
                            // This occcurs most likely if a user is attempting to
                            // interact with a transaction that is already finished.
                            warn!("{error}")
                        }
                        Err(err) => return Err(err),
                    },
                    None => {
                        info!("User triggered daemon shutdown.");
                        if !self.terminate.load(Ordering::Relaxed) {
                            self.terminate.store(true, Ordering::Relaxed);
                        }
                        break;
                    }
                },
                _ = cleanup.tick() => self.cleanup_transactions().await,
            };
        }

        // a final cleanup
        while let Some(handle) = self.transaction_handles.pop() {
            match handle.await {
                Ok(Ok(id)) => {
                    // remove the channel for this transaction if it is complete
                    let _ = self.transaction_channels.remove(&id);
                }
                Ok(Err(err)) => {
                    info!("Error occurred during transaction: {}", err)
                }
                Err(_) => error!("Unable to join handle!"),
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[macro_export]
    macro_rules! assert_err{
        ($expression:expr, $($pattern:tt)+) => {
            match $expression {
                $($pattern)+ => {},
                ref e => panic!("expected {} but got {:?}", stringify!($($pattern)+), e)
            }
        }
    }
}
