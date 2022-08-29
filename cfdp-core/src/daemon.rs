use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{unbounded, Receiver, Select, SendTimeoutError, Sender, TryRecvError};
use signal_hook::{consts::TERM_SIGNALS, flag};

use crate::{
    filestore::FileStore,
    pdu::{
        error::PDUError, Condition, EntityID, FaultHandlerAction, MessageToUser, PDUEncode,
        PDUHeader, TransactionSeqNum, TransactionStatus, TransmissionMode, UserOperation,
        VariableID, PDU,
    },
    transaction::{Action, Metadata, Transaction, TransactionConfig, TransactionError},
};

pub enum Command {
    PDU(PDU),
    Cancel,
    Suspend,
    Resume,
    Abandon,
}
pub enum UserPrimitive {
    Put,
    Cancel,
    Suspend,
    Resume,
    Report,
}
pub struct EntityConfig {
    fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    file_size_segment: u16,
    // The number of timeouts before a fault is issued on a transaction
    default_transaction_max_count: u32,
    // default number of seconds for transaction timers to wait
    default_inactivity_timeout: i64,
}

type SpawnerTuple = (
    (EntityID, TransactionSeqNum),
    Sender<Command>,
    JoinHandle<Result<(), TransactionError>>,
);

/// The CFDP Daemon is responsible for connecting [PDUTransport](crate::transport::PDUTransport) implementation
/// with each individual [Transaction](crate::transaction::Transaction). When a PDUTransport implementation
/// sends a PDU through a channel, the Daemon distributes the PDU to the necessary Transaction.
/// PDUs are sent from each Transaction directly to their respective PDUTransport implementations.
pub struct Daemon<T: FileStore + Send + 'static> {
    // The collection of all current transactions
    transaction_handles: Vec<JoinHandle<Result<(), TransactionError>>>,
    // the vector of transportation tx channel connections
    transport_tx_vec: Vec<Sender<(VariableID, PDU)>>,
    // the vector of transportation rx channel connections
    transport_rx_vec: Vec<Receiver<(VariableID, PDU)>>,
    // // mapping of unique transaction ids to channels used to talk to each transaction
    // transaction_channels: HashMap<(EntityID, Vec<u8>), Sender<Command>>,
    // the underlying filestore used by this Daemon
    filestore: Arc<Mutex<T>>,
    // message reciept channel used to execute User Operations
    message_rx: Receiver<MessageToUser>,
    // message sender channel used to execute User Operations by Transactions
    message_tx: Sender<MessageToUser>,
    // a mapping of individual fault handler actions per remote entity
    entity_configs: HashMap<VariableID, EntityConfig>,
    // the default fault handling configuration
    default_config: EntityConfig,
}
impl<T: FileStore + Send + 'static> Daemon<T> {
    fn spawn_receive_transaction(
        header: &PDUHeader,
        transport_tx: Sender<(VariableID, PDU)>,
        entity_config: &EntityConfig,
        filestore: Arc<Mutex<T>>,
        message_tx: Sender<MessageToUser>,
    ) -> SpawnerTuple {
        let (transaction_tx, transaction_rx) = unbounded();

        let config = TransactionConfig {
            action_type: Action::Receive,
            source_entity_id: header.source_entity_id.clone(),
            destination_entity_id: header.destination_entity_id.clone(),
            transmission_mode: header.transmission_mode.clone(),
            sequence_number: header.transaction_sequence_number.clone(),
            file_size_flag: header.large_file_flag,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: header.crc_flag.clone(),
            segment_metadata_flag: header.segment_metadata_flag.clone(),
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.default_inactivity_timeout,
        };
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let id = transaction.id();

        let handle = thread::spawn(move || {
            while transaction.get_status() != &TransactionStatus::Terminated {
                // this function handles any timeouts and resends
                transaction.monitor_timeout()?;
                // if instant mode send naks

                // if outside prompt send naks

                match transaction_rx.try_recv() {
                    Ok(command) => {
                        match command {
                            Command::PDU(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(crate::transaction::TransactionError::UnexpectedPDU(
                                        _info,
                                    )) => {
                                        // log some info on the unexpected PDU?
                                    }
                                    Err(err) => return Err(err),
                                }
                            }
                            Command::Resume => transaction.resume(),
                            Command::Cancel => transaction.cancel()?,
                            Command::Suspend => transaction.suspend(),
                            Command::Abandon => transaction.abandon(),
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        // nothing for us at this time just sleep
                    }
                    Err(TryRecvError::Disconnected) => {
                        // Really do not expect to be in this situation
                        // probably the thread should exit
                        panic!(
                            "Connection to Daemon Severed for Transaction {:?}",
                            transaction.id()
                        )
                    }
                }
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });

        (id, transaction_tx, handle)
    }

    fn spawn_send_transaction(
        header: &PDUHeader,
        metadata: Metadata,
        transport_tx: Sender<(VariableID, PDU)>,
        entity_config: &EntityConfig,
        filestore: Arc<Mutex<T>>,
        message_tx: Sender<MessageToUser>,
    ) -> SpawnerTuple {
        let (transaction_tx, transaction_rx) = unbounded();

        let config = TransactionConfig {
            action_type: Action::Send,
            source_entity_id: header.source_entity_id.clone(),
            destination_entity_id: header.destination_entity_id.clone(),
            transmission_mode: header.transmission_mode.clone(),
            sequence_number: header.transaction_sequence_number.clone(),
            file_size_flag: header.large_file_flag,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: header.crc_flag.clone(),
            segment_metadata_flag: header.segment_metadata_flag.clone(),
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.default_inactivity_timeout,
        };
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let id = transaction.id();

        let handle = thread::spawn(move || {
            transaction.put(metadata)?;
            while transaction.get_status() != &TransactionStatus::Terminated {
                // this function handles any timeouts and resends
                transaction.monitor_timeout()?;
                // if we have recieved a NAK send the missing data
                transaction.send_missing_data()?;
                // send the next data segment for the first time
                match transaction.all_data_sent()? {
                    false => transaction.send_file_segment(None, None)?,
                    true => {
                        // if all data has been sent (for the first time)
                        // send eof
                        transaction.send_eof(None)?;

                        match transaction.get_mode() {
                            // for unacknowledged transactions.
                            // this is the end
                            TransmissionMode::Unacknowledged => transaction.abandon(),
                            TransmissionMode::Acknowledged => {}
                        }
                    }
                };
                // Handle any messages that are waiting to be processed
                match transaction_rx.try_recv() {
                    Ok(command) => {
                        match command {
                            Command::PDU(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(crate::transaction::TransactionError::UnexpectedPDU(
                                        _info,
                                    )) => {
                                        // log some info on the unexpected PDU?
                                    }
                                    Err(err) => return Err(err),
                                }
                            }
                            Command::Resume => transaction.resume(),
                            Command::Cancel => transaction.cancel()?,
                            Command::Suspend => transaction.suspend(),
                            Command::Abandon => transaction.abandon(),
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        // nothing for us at this time just sleep
                    }
                    Err(TryRecvError::Disconnected) => {
                        // Really do not expect to be in this situation
                        // probably the thread should exit
                        panic!(
                            "Connection to Daemon Severed for Transaction {:?}",
                            transaction.id()
                        )
                    }
                }
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });
        (id, transaction_tx, handle)
    }

    pub fn put(
        &self,
        // source_filename: PathBuf,
        // destination_filename: PathBuf,
        // destination_entity_id: EntityID,
    ) {
        todo!()
        // self.spawn_send_transaction(header, metadata, self.transport_tx)
    }

    pub fn manage_transactions(
        &mut self,
        // message_rx: Receiver<MessageToUser>,
        // transport_rx_vec: &[Receiver<(EntityID, PDU)>],
        // transport_tx_vec: &[Sender<(EntityID, PDU)>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Boolean to track if a kill signal is received
        let terminate = Arc::new(AtomicBool::new(false));

        for sig in TERM_SIGNALS {
            // When terminated by a second term signal, exit with exit code 1.
            // This will do nothing the first time (because term_now is false).
            flag::register_conditional_shutdown(*sig, 1, Arc::clone(&terminate))
                .expect("Unable to register termination signals.");
            // But this will "arm" the above for the second time, by setting it to true.
            // The order of registering these is important, if you put this one first, it will
            // first arm and then terminate ‒ all in the first round.
            flag::register(*sig, Arc::clone(&terminate))
                .expect("Unable to register termination signals.");
        }

        // Create the selection object to check if any messages are available.
        // the returned index will be used to determine which action to take.
        let mut selector = Select::new();

        selector.recv(&self.message_rx);

        for rx in self.transport_rx_vec.iter() {
            selector.recv(rx);
        }

        // mapping of unique transaction ids to channels used to talk to each transaction
        let mut transaction_channels: HashMap<(EntityID, TransactionSeqNum), Sender<Command>> =
            HashMap::new();

        while !terminate.load(Ordering::Relaxed) {
            match selector.ready_timeout(Duration::from_millis(500)) {
                Ok(val) if val == 0 => {
                    // this is a message to user
                    match self.message_rx.try_recv() {
                        Ok(msg) => {
                            // decode UserOperation
                            match UserOperation::decode(&mut msg.message_text.as_slice()) {
                                Ok(operation) => {
                                    // match and perform operation
                                    match operation {
                                        UserOperation::OriginatingTransactionIDMessage(_) => {
                                            todo!()
                                        }
                                        UserOperation::ProxyPutRequest(_) => todo!(),
                                        UserOperation::ProxyPutResponse(_) => todo!(),
                                        UserOperation::ProxyMessageToUser(_) => todo!(),
                                        UserOperation::ProxyFileStoreRequest(_) => todo!(),
                                        UserOperation::ProxyFileStoreResponse(_) => todo!(),
                                        UserOperation::ProxyFaultHandlerOverride(_) => todo!(),
                                        UserOperation::ProxyTransmissionMode(_) => todo!(),
                                        UserOperation::ProxyFlowLabel(_) => todo!(),
                                        UserOperation::ProxySegmentationControl(_) => todo!(),
                                        UserOperation::ProxyPutCancel => todo!(),
                                        UserOperation::DirectoryListingRequest(_) => todo!(),
                                        UserOperation::DirectoryListingResponse(_) => todo!(),
                                        UserOperation::RemoteStatusReportRequest(_) => todo!(),
                                        UserOperation::RemoteStatusReportResponse(_) => todo!(),
                                        UserOperation::RemoteSuspendRequest(_) => todo!(),
                                        UserOperation::RemoteSuspendResponse(_) => todo!(),
                                        UserOperation::RemoteResumeRequest(_) => todo!(),
                                        UserOperation::RemoteResumeResponse(_) => todo!(),
                                        UserOperation::SFORequest(_) => todo!(),
                                        UserOperation::SFOMessageToUser(_) => todo!(),
                                        UserOperation::SFOFlowLabel(_) => todo!(),
                                        UserOperation::SFOFaultHandlerOverride(_) => todo!(),
                                        UserOperation::SFOFileStoreRequest(_) => todo!(),
                                        UserOperation::SFOFileStoreResponse(_) => todo!(),
                                        UserOperation::SFOReport(_) => todo!(),
                                    }
                                }
                                Err(PDUError::UnexpectedIdentifier(_recv, _expected)) => {
                                    // try to print out message
                                }
                                Err(_error) => {
                                    // error handling message decoding.
                                    // what to do here?
                                }
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            // was not actually ready, go back to selection
                        }
                        Err(TryRecvError::Disconnected) => {
                            // The transport instance disconnected?
                            // what do we do?
                            // remove from possible selections
                            selector.remove(val);
                        }
                    }
                }
                Ok(val) if val > 0 => {
                    // this is a pdu from a transport
                    // subtract 1 because the transport indices start at 1 for the selector
                    // but are 0 indexed in the vec
                    let rx = &self.transport_rx_vec[val - 1];
                    match rx.try_recv() {
                        Ok((_, pdu)) => {
                            let key = (
                                pdu.header.source_entity_id.clone(),
                                pdu.header.transaction_sequence_number.clone(),
                            );
                            // hand pdu off to transaction
                            let channel = transaction_channels
                                .entry(key.clone())
                                // if this key is not in the channel list
                                // create a new transaction
                                .or_insert_with(|| {
                                    let entity_config = self
                                        .entity_configs
                                        .get(&key.0)
                                        .unwrap_or(&self.default_config);

                                    let (_id, channel, handle) = Self::spawn_receive_transaction(
                                        &pdu.header,
                                        self.transport_tx_vec[val - 1].clone(),
                                        entity_config,
                                        self.filestore.clone(),
                                        self.message_tx.clone(),
                                    );

                                    self.transaction_handles.push(handle);
                                    channel
                                });

                            match channel
                                .send_timeout(Command::PDU(pdu.clone()), Duration::from_millis(500))
                            {
                                Ok(()) => {}
                                Err(SendTimeoutError::Timeout(msg))
                                | Err(SendTimeoutError::Disconnected(msg)) => {
                                    // the transaction is completed.
                                    // spawn a new one
                                    // this is very unlikely and only results
                                    // if a sender is re-using a transaction id
                                    let entity_config = self
                                        .entity_configs
                                        .get(&key.0)
                                        .unwrap_or(&self.default_config);

                                    let (_id, new_channel, handle) =
                                        Self::spawn_receive_transaction(
                                            &pdu.header,
                                            self.transport_tx_vec[val - 1].clone(),
                                            entity_config,
                                            self.filestore.clone(),
                                            self.message_tx.clone(),
                                        );

                                    self.transaction_handles.push(handle);
                                    new_channel.send(msg)?;
                                    // update the dict to have the new channel
                                    transaction_channels.insert(key, new_channel);
                                }
                            };
                        }
                        Err(TryRecvError::Empty) => {
                            // was not actually ready, go back to selection
                        }
                        Err(TryRecvError::Disconnected) => {
                            // The transport instance disconnected?
                            // what do we do?
                            // remove from possible selections
                            selector.remove(val);
                        }
                    }
                }
                Err(_) => {
                    // timeout occurred
                }
                Ok(_) => unreachable!(),
            }

            // process any message from users from the outside application API
            // interprocess looks like a good choice for this

            // join any handles that have completed
            let mut ind = 0;
            while ind < self.transaction_handles.len() {
                if self.transaction_handles[ind].is_finished() {
                    let handle = self.transaction_handles.remove(ind);
                    match handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            println!("Error occured during transaction: {}", err)
                        }
                        Err(_) => println!("Unable to join handle!"),
                    };
                } else {
                    ind += 1;
                }
            }
        }
        Ok(())
    }
}

impl<T: FileStore + Send + 'static> Drop for Daemon<T> {
    fn drop(&mut self) {
        for ind in 0..self.transaction_handles.len() {
            let handle = self.transaction_handles.remove(ind);
            handle
                .join()
                .expect("Unable to join thread.")
                .expect("Error during threaded transaction handle.");
        }
    }
}
