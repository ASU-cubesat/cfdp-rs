use std::{
    collections::{hash_map::Entry, HashMap},
    io::{Error as IOError, Read},
    string::FromUtf8Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use camino::Utf8PathBuf;
use log::{error, info, warn};
use num_traits::FromPrimitive;
use thiserror::Error;
use tokio::{
    select,
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
    time::MissedTickBehavior,
};

use crate::{
    filestore::{ChecksumType, FileStore},
    pdu::{
        error::PDUError, CRCFlag, Condition, DeliveryCode, Direction, EntityID, FaultHandlerAction,
        FileSizeFlag, FileStatusCode, FileStoreRequest, FileStoreResponse, MessageToUser,
        NakOrKeepAlive, PDUEncode, PDUHeader, SegmentedData, TransactionSeqNum, TransactionStatus,
        TransmissionMode, VariableID, PDU,
    },
    transaction::{
        Metadata, RecvTransaction, SendTransaction, TransactionConfig, TransactionError,
        TransactionID, TransactionState,
    },
    transport::PDUTransport,
};

#[derive(Error, Debug)]
pub enum PrimitiveError {
    #[error("IO Error During Primitive execution: {0}")]
    IO(#[from] IOError),

    #[error("Unexpected value for User Primitive.")]
    UnexpextedPrimitive,

    #[error("Error (en)de-coding PDU. {0}")]
    Encode(Box<PDUError>),

    #[error("Error (en)de-coding Metadata. {0}")]
    Metadata(Box<TransactionError>),

    #[error("Invalid file path encoding. {0}")]
    Utf8(#[from] FromUtf8Error),
}

impl From<PDUError> for PrimitiveError {
    fn from(err: PDUError) -> Self {
        Self::Encode(Box::new(err))
    }
}
impl From<TransactionError> for PrimitiveError {
    fn from(err: TransactionError) -> Self {
        Self::Metadata(Box::new(err))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Necessary Configuration for a Put.Request operation
pub struct PutRequest {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// Destination ID of the Request
    pub destination_entity_id: EntityID,
    /// Whether to send in acknowledged or unacknowledged mode
    pub transmission_mode: TransmissionMode,
    /// List of any filestore requests to take after transaction is complete
    pub filestore_requests: Vec<FileStoreRequest>,
    /// Any Messages to user received either from the metadataPDU or as input
    pub message_to_user: Vec<MessageToUser>,
}

fn construct_metadata(req: PutRequest, config: EntityConfig, file_size: u64) -> Metadata {
    Metadata {
        source_filename: req.source_filename,
        destination_filename: req.destination_filename,
        file_size,
        filestore_requests: req.filestore_requests,
        message_to_user: req.message_to_user,
        closure_requested: config.closure_requested,
        checksum_type: config.checksum_type,
    }
}

#[derive(Debug)]
/// Possible User Primitives sent from a end user application via the user primitive channel
pub enum UserPrimitive {
    /// Initiate a Put transaction with the specified [PutRequest] configuration.
    Put(PutRequest, oneshot::Sender<TransactionID>),
    /// Cancel the give transaction.
    Cancel(TransactionID),
    /// Suspend operations of the given transaction.
    Suspend(TransactionID),
    /// Resume operations of the given transaction.
    Resume(TransactionID),
    /// Report progress of the given transaction.
    Report(TransactionID, oneshot::Sender<Report>),
    /// Send the designated PromptPDU from the given transaction.
    /// This primitive is only valid for [Send](crate::transaction::SendTransaction) transactions
    Prompt(TransactionID, NakOrKeepAlive),
}

/// Lightweight commands
#[derive(Debug)]
enum Command {
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

/// Simple Status Report
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Report {
    pub id: TransactionID,
    pub state: TransactionState,
    pub status: TransactionStatus,
    pub condition: Condition,
}
impl Report {
    pub fn encode(self) -> Vec<u8> {
        let mut buff = self.id.0.encode();
        buff.extend(self.id.1.encode());
        buff.push(self.state as u8);
        buff.push(self.status as u8);
        buff.push(self.condition as u8);
        buff
    }

    pub fn decode<T: Read>(buffer: &mut T) -> Result<Self, Box<dyn std::error::Error>> {
        let id = {
            let entity_id = EntityID::decode(buffer)?;
            let sequence_num = TransactionSeqNum::decode(buffer)?;

            TransactionID(entity_id, sequence_num)
        };

        let mut u8_buff = [0_u8; 1];

        let state = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            TransactionState::from_u8(possible).ok_or(TransactionError::InvalidStatus(possible))?
        };

        let status = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            TransactionStatus::from_u8(possible)
                .ok_or(PDUError::InvalidTransactionStatus(possible))?
        };

        let condition = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            Condition::from_u8(possible).ok_or(PDUError::InvalidCondition(possible))?
        };

        Ok(Self {
            id,
            state,
            status,
            condition,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MetadataRecvIndication {
    pub id: TransactionID,
    pub source_filename: Utf8PathBuf,
    pub destination_filename: Utf8PathBuf,
    pub file_size: u64,
    pub transmission_mode: TransmissionMode,
    pub user_messages: Vec<MessageToUser>,
}

#[derive(Debug, Clone)]
pub struct FileSegmentIndication {
    pub id: TransactionID,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct FinishedIndication {
    pub id: TransactionID,
    pub report: Report,
    pub file_status: FileStatusCode,
    pub delivery_code: DeliveryCode,
    pub filestore_responses: Vec<FileStoreResponse>,
}

#[derive(Debug, Clone)]
pub struct SuspendIndication {
    pub id: TransactionID,
    pub condition: Condition,
}

#[derive(Debug, Clone)]
pub struct ResumeIndication {
    pub id: TransactionID,
    pub progress: u64,
}

#[derive(Debug, Clone)]
pub struct FaultIndication {
    pub id: TransactionID,
    pub condition: Condition,
    pub progress: u64,
}

#[derive(Debug, Clone)]
/// Indications how the Daemon and Transactions relay information back to the User application.
/// Indications are issued at necessary points in each Transaction's lifetime.
pub enum Indication {
    /// A new transaction has been initiated as a result of a [PutRequest]
    Transaction(TransactionID),
    /// End of File has been Sent
    EoFSent(TransactionID),
    /// End of File PDU has been received
    EoFRecv(TransactionID),
    /// A running transaction has reached the Finished state.
    /// Receipt of this indications starts and post transaction actions.
    Finished(FinishedIndication),
    /// Metadata has been received for a [RecvTransaction]
    MetadataRecv(MetadataRecvIndication),
    /// A new file segment has been received
    FileSegmentRecv(FileSegmentIndication),
    /// The associated Transaction has been suspended at the given progress point.
    Suspended(SuspendIndication),
    /// The associated Transaction has been resumed at the given progress point.
    Resumed(ResumeIndication),
    /// Last known status for the given transaction
    Report(Report),
    /// A Fault has been initiated for the given transaction
    Fault(FaultIndication),
    /// An Abandon Fault has been initiated for the given transaction
    Abandon(FaultIndication),
}

/// The way the Nak procedure is implemented is the following:
///  - In Immediate mode, upon reception of each file data PDU, if the received segment is at the end of the file and there is a gap
///    between the previously received segment and the new segment, a nak is sent with the new gap.
///    If the NAK timer has timed out, the nak sent covers the gaps from the entire file, not only the last gap.
///    After the EOF is received, the procedure is the same as in deferred mode.
///  - In Deferred mode, a nak covering the gaps from the entire file is sent immediately after EOF and each time the nak timer times out.
///  - at any time a Prompt NAK can trigger the sending of the complete Nak list.
///
/// NAK timer:
/// - In Immediate mode the NAK timer is started at the beginning of the transaction.
/// - In Deferred mode  the NAK timer is started after EOF is received.
/// - If the NAK timer times out and it is determined that new data has been received since the last nak sending, the timer counter is reset to 0.
/// - If the NAK timer expired more than the predefined limit (without any new data being received), the NakLimitReached fault will be raised.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NakProcedure {
    Immediate,
    Deferred,
}

#[derive(Clone)]
/// Configuration parameters for transactions which may change based on the receiving entity.
pub struct EntityConfig {
    /// Mapping to decide how each fault type should be handled
    pub fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// Maximum file size fragment this entity can receive
    pub file_size_segment: u16,
    // The number of timeouts before a fault is issued on a transaction
    pub default_transaction_max_count: u32,
    // number of seconds for inactivity timers to wait
    pub inactivity_timeout: i64,
    // number of seconds for ACK timers to wait
    pub ack_timeout: i64,
    // number of seconds for NAK timers to wait
    pub nak_timeout: i64,
    /// Flag to determine if the CRC protocol should be used
    pub crc_flag: CRCFlag,
    /// Whether closure whould be requested on Unacknowledged transactions.
    pub closure_requested: bool,
    /// The default ChecksumType to use for file transfers
    pub checksum_type: ChecksumType,
    // for recv transactions - when to send the NAKs (immediately when detected or after EOF)
    pub nak_procedure: NakProcedure,
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
/// with each individual [Transaction](crate::transaction::Transaction). When a PDUTransport implementation
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
    ) -> Result<RecvSpawnerTuple, Box<dyn std::error::Error>> {
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

        Ok((id, transaction_tx, handle))
    }

    fn spawn_send_transaction(
        request: PutRequest,
        transaction_id: TransactionID,
        transport_tx: Sender<(EntityID, PDU)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        indication_tx: Sender<Indication>,
    ) -> Result<SendSpawnerTuple, Box<dyn std::error::Error>> {
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
        let mut metadata = construct_metadata(request, entity_config, 0_u64);

        let handle = tokio::task::spawn(async move {
            let file_size = match &metadata.source_filename.file_name().is_none() {
                true => 0_u64,
                false => filestore.get_size(&metadata.source_filename)?,
            };

            metadata.file_size = file_size;
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

    async fn process_primitive(
        &mut self,
        primitive: UserPrimitive,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                    channel.send(Command::Cancel).await?;
                }
            }
            UserPrimitive::Suspend(id) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel.send(Command::Suspend).await?;
                }
            }
            UserPrimitive::Resume(id) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel.send(Command::Resume).await?;
                }
            }
            UserPrimitive::Report(id, report_sender) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    // It's possible for the user to ask for a report after the Transaction is finished
                    // but before the channel is cleaned up.
                    // for now ignore errors until a better solution is found.
                    // maybe possible to trigger a cleanup immediately after a transaction finishes?
                    let _ = channel.send(Command::Report(report_sender)).await;
                }
            }
            UserPrimitive::Prompt(id, option) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel.send(Command::Prompt(option)).await?;
                }
            }
        };
        Ok(())
    }

    async fn forward_pdu(&mut self, pdu: PDU) -> Result<(), Box<dyn std::error::Error>> {
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
                    )
                    .expect("Cannot spawn new Transaction.");

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
                let (_id, new_channel, handle) = Self::spawn_receive_transaction(
                    &pdu.header,
                    transport,
                    entity_config,
                    self.filestore.clone(),
                    self.indication_tx.clone(),
                )?;
                self.transaction_handles.push(handle);
                new_channel.send(Command::Pdu(pdu.clone())).await?;
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
    pub async fn manage_transactions(&mut self) -> Result<(), Box<dyn std::error::Error + '_>> {
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
                    Some(pdu) =>self.forward_pdu(pdu).await?,
                    None => {
                        if !self.terminate.load(Ordering::Relaxed) {
                            error!("Transport unexpectedly disconnected from daemon.");
                            self.terminate.store(true, Ordering::Relaxed);
                        }
                        break;
                    }
                },
                primitive = self.primitive_rx.recv() => match primitive {
                    Some(primitive) => self.process_primitive(primitive).await?,
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
