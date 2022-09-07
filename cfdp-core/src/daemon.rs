use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    fs::OpenOptions,
    io::{Error as IOError, Read, Write},
    string::FromUtf8Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use camino::Utf8PathBuf;
use crossbeam_channel::{unbounded, Receiver, Select, SendTimeoutError, Sender, TryRecvError};
use interprocess::local_socket::LocalSocketListener;
use itertools::{Either, Itertools};
use log::{error, info};
use num_traits::FromPrimitive;
use signal_hook::{consts::TERM_SIGNALS, flag};

use crate::{
    filestore::{ChecksumType, FileStore},
    pdu::{
        error::PDUError, CRCFlag, Condition, DirectoryListingResponse, EntityID,
        FaultHandlerAction, FileSizeFlag, FileSizeSensitive, FileStoreRequest, ListingResponseCode,
        MessageToUser, OriginatingTransactionIDMessage, PDUEncode, PDUHeader, ProxyOperation,
        ProxyPutRequest, RemoteStatusReportResponse, RemoteSuspendResponse, SegmentedData,
        TransactionSeqNum, TransactionStatus, TransmissionMode, UserOperation, UserRequest,
        UserResponse, VariableID, PDU,
    },
    transaction::{
        Action, Metadata, Transaction, TransactionConfig, TransactionError, TransactionID,
    },
};

#[derive(Debug)]
pub enum PrimitiveError {
    IO(IOError),
    UnexpextedPrimitive,
    Encode(PDUError),
    Metadata(TransactionError),
    Utf8(FromUtf8Error),
}
impl From<IOError> for PrimitiveError {
    fn from(err: IOError) -> Self {
        Self::IO(err)
    }
}
impl From<PDUError> for PrimitiveError {
    fn from(err: PDUError) -> Self {
        Self::Encode(err)
    }
}
impl From<TransactionError> for PrimitiveError {
    fn from(err: TransactionError) -> Self {
        Self::Metadata(err)
    }
}

impl From<FromUtf8Error> for PrimitiveError {
    fn from(err: FromUtf8Error) -> Self {
        Self::Utf8(err)
    }
}
impl Display for PrimitiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(err) => err.fmt(f),
            Self::UnexpextedPrimitive => write!(f, "Unexpected value for User Primitive."),
            Self::Encode(err) => err.fmt(f),
            Self::Metadata(err) => write!(f, "Error (en)de-coding Metadata. {}", err),
            Self::Utf8(error) => write!(f, "Invalid file path encoding. {}", error),
        }
    }
}
impl Error for PrimitiveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IO(source) => Some(source),
            Self::UnexpextedPrimitive => None,
            Self::Encode(source) => Some(source),
            Self::Metadata(source) => Some(source),
            Self::Utf8(source) => Some(source),
        }
    }
}

#[cfg_attr(test, derive(Debug, Clone, PartialEq, Eq))]
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
impl PutRequest {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        {
            let vec = self.source_filename.as_str().as_bytes();
            buffer.push(vec.len() as u8);
            buffer.extend_from_slice(vec)
        }
        {
            let vec = self.destination_filename.as_str().as_bytes();
            buffer.push(vec.len() as u8);
            buffer.extend_from_slice(vec)
        }
        buffer.push(self.destination_entity_id.get_len());
        buffer.extend(self.destination_entity_id.to_be_bytes());

        buffer.push(self.transmission_mode as u8);

        buffer.push(self.filestore_requests.len() as u8);
        self.filestore_requests
            .into_iter()
            .for_each(|req| buffer.extend(req.encode()));

        buffer.push(self.message_to_user.len() as u8);
        self.message_to_user
            .into_iter()
            .for_each(|msg| buffer.extend(msg.encode()));

        buffer
    }

    pub(crate) fn decode<T: Read>(buffer: &mut T) -> PrimitiveResult<Self> {
        let mut u8buff = [0_u8; 1];

        let source_filename = {
            buffer.read_exact(&mut u8buff)?;
            let mut vec = vec![0_u8; u8buff[0] as usize];
            buffer.read_exact(vec.as_mut_slice())?;
            Utf8PathBuf::from(String::from_utf8(vec)?)
        };
        let destination_filename = {
            buffer.read_exact(&mut u8buff)?;
            let mut vec = vec![0_u8; u8buff[0] as usize];
            buffer.read_exact(vec.as_mut_slice())?;
            Utf8PathBuf::from(String::from_utf8(vec)?)
        };

        let destination_entity_id = {
            buffer.read_exact(&mut u8buff)?;
            let mut vec = vec![0_u8; u8buff[0] as usize];
            buffer.read_exact(vec.as_mut_slice())?;
            EntityID::try_from(vec)?
        };

        let transmission_mode = {
            buffer.read_exact(&mut u8buff)?;
            let possible = u8buff[0];
            TransmissionMode::from_u8(possible)
                .ok_or(PDUError::InvalidTransmissionMode(possible))?
        };

        let filestore_requests = {
            buffer.read_exact(&mut u8buff)?;
            let mut vec = Vec::with_capacity(u8buff[0] as usize);
            for _ind in 0..vec.capacity() {
                vec.push(FileStoreRequest::decode(buffer)?)
            }
            vec
        };

        let message_to_user = {
            buffer.read_exact(&mut u8buff)?;
            let mut vec = Vec::with_capacity(u8buff[0] as usize);
            for _ind in 0..vec.capacity() {
                vec.push(MessageToUser::decode(buffer)?)
            }
            vec
        };

        Ok(Self {
            source_filename,
            destination_filename,
            destination_entity_id,
            transmission_mode,
            filestore_requests,
            message_to_user,
        })
    }
}

fn construct_metadata(
    req: PutRequest,
    config: &EntityConfig,
    file_size: FileSizeSensitive,
) -> Metadata {
    Metadata {
        source_filename: req.source_filename,
        destination_filename: req.destination_filename,
        file_size,
        filestore_requests: req.filestore_requests,
        message_to_user: req.message_to_user,
        closure_requested: config.closure_requested,
        checksum_type: config.checksum_type.clone(),
    }
}

type PrimitiveResult<T> = Result<T, PrimitiveError>;

#[cfg_attr(test, derive(Debug, Clone, PartialEq, Eq))]
/// Possible User Primitives sent from a end user application to the
/// interprocess pipe.
pub enum UserPrimitive {
    /// Initiate a Put transaction with the specificed [PutRequest] configuration.
    Put(PutRequest),
    /// Cancel the give transaction.
    Cancel(EntityID, TransactionSeqNum),
    /// Suspend operations of the given transaction.
    Suspend(EntityID, TransactionSeqNum),
    /// Resume operations of the given transaction.
    Resume(EntityID, TransactionSeqNum),
    /// Report progress of the given transaction.
    Report(EntityID, TransactionSeqNum),
}
impl UserPrimitive {
    pub fn encode(self) -> Vec<u8> {
        match self {
            Self::Put(request) => {
                let mut buff: Vec<u8> = vec![0_u8];
                buff.extend(request.encode());
                buff
            }
            Self::Cancel(id, seq) => {
                let mut buff: Vec<u8> = vec![1_u8];
                buff.extend(id.encode());
                buff.extend(seq.encode());
                buff
            }
            Self::Suspend(id, seq) => {
                let mut buff: Vec<u8> = vec![2_u8];
                buff.extend(id.encode());
                buff.extend(seq.encode());
                buff
            }
            Self::Resume(id, seq) => {
                let mut buff: Vec<u8> = vec![3_u8];
                buff.extend(id.encode());
                buff.extend(seq.encode());
                buff
            }
            Self::Report(id, seq) => {
                let mut buff: Vec<u8> = vec![4_u8];
                buff.extend(id.encode());
                buff.extend(seq.encode());
                buff
            }
        }
    }

    pub fn decode<T: Read>(buffer: &mut T) -> PrimitiveResult<Self> {
        let mut u8buff = [0_u8];
        buffer.read_exact(&mut u8buff)?;
        match u8buff[0] {
            0 => {
                let request = PutRequest::decode(buffer)?;
                Ok(Self::Put(request))
            }
            1 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Cancel(id, seq))
            }
            2 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Suspend(id, seq))
            }
            3 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Resume(id, seq))
            }
            4 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Report(id, seq))
            }
            _ => Err(PrimitiveError::UnexpextedPrimitive),
        }
    }
}

/// Lightweight commands
enum Command {
    Pdu(PDU),
    Cancel,
    Suspend,
    Resume,
    // may find a use for abandon in the future.
    #[allow(unused)]
    Abandon,
}

/// Configuration parameters for transactions which may change based on the receiving entity.
pub struct EntityConfig {
    /// Mapping to decide how each fault type should be handled
    fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// Maximum file size fragment this entity can receive
    file_size_segment: u16,
    // The number of timeouts before a fault is issued on a transaction
    default_transaction_max_count: u32,
    // default number of seconds for transaction timers to wait
    default_inactivity_timeout: i64,
    /// Flag to determine if the CRC protocol should be used
    crc_flag: CRCFlag,
    /// Whether closure whould be requested on Unacknowledged transactions.
    closure_requested: bool,
    /// The default ChecksumType to use for file transfers
    checksum_type: ChecksumType,
}

type SpawnerTuple = (
    (EntityID, TransactionSeqNum),
    Sender<Command>,
    JoinHandle<Result<TransactionID, TransactionError>>,
);

fn get_proxy_request(origin_id: &TransactionID, messages: &[ProxyOperation]) -> Vec<PutRequest> {
    let mut out = vec![];
    let proxy_puts: Vec<ProxyPutRequest> = messages
        .iter()
        .filter_map(|msg| match msg {
            ProxyOperation::ProxyPutRequest(req) => Some(req.clone()),
            _ => None,
        })
        .collect();

    for put in proxy_puts {
        let transmission_mode = messages
            .iter()
            .find_map(|msg| match msg {
                ProxyOperation::ProxyTransmissionMode(mode) => Some(mode.clone()),
                _ => None,
            })
            .unwrap_or(TransmissionMode::Unacknowledged);

        let filestore_requests = messages
            .iter()
            .filter_map(|msg| match msg {
                ProxyOperation::ProxyFileStoreRequest(req) => Some(req.clone()),
                _ => None,
            })
            .collect();

        let mut message_to_user: Vec<MessageToUser> = messages
            .iter()
            .filter_map(|msg| match msg {
                ProxyOperation::ProxyMessageToUser(req) => Some(req.clone()),
                _ => None,
            })
            .collect();

        // Should include an originating TransactionIDMessage
        // But if the implementation doesn't let's not worry about it too much
        message_to_user.push(MessageToUser::from(
            UserOperation::OriginatingTransactionIDMessage(OriginatingTransactionIDMessage {
                source_entity_id: origin_id.0.clone(),
                transaction_sequence_number: origin_id.1.clone(),
            }),
        ));

        let req = PutRequest {
            source_filename: put.source_filename,
            destination_filename: put.destination_filename,
            destination_entity_id: put.destination_entity_id,
            transmission_mode,
            filestore_requests,
            message_to_user,
        };
        out.push(req)
    }

    out
}

type UserMessageCategories = (
    // proxy operations
    Vec<PutRequest>,
    // user requests
    Vec<UserRequest>,
    // responses to log
    Vec<UserResponse>,
    // Transaction ID to cancel
    Option<TransactionID>,
    // Others
    Vec<MessageToUser>,
);

fn categorize_user_msg(
    origin_id: &TransactionID,
    messages: Vec<MessageToUser>,
) -> UserMessageCategories {
    let (user_ops, other_messages): (Vec<UserOperation>, Vec<MessageToUser>) =
        messages.into_iter().partition_map(|msg| {
            match UserOperation::decode(&mut msg.message_text.as_slice()) {
                Ok(operation) => Either::Left(operation),
                Err(_) => Either::Right(msg),
            }
        });

    let cancel_id = user_ops
        .iter()
        .find(|&msg| msg == &UserOperation::ProxyOperation(ProxyOperation::ProxyPutCancel))
        .and_then(|_| {
            user_ops.iter().find_map(|msg| {
                if let UserOperation::OriginatingTransactionIDMessage(origin) = msg {
                    Some((
                        origin.source_entity_id.clone(),
                        origin.transaction_sequence_number.clone(),
                    ))
                } else {
                    None
                }
            })
        });
    let proxy_ops: Vec<ProxyOperation> = user_ops
        .iter()
        .filter_map(|req| {
            if let UserOperation::ProxyOperation(op) = req {
                Some(op.clone())
            } else {
                None
            }
        })
        .collect();

    let other_reqs = user_ops
        .iter()
        .filter_map(|req| {
            if let UserOperation::Request(request) = req {
                Some(request.clone())
            } else {
                None
            }
        })
        .collect();

    let responses = user_ops
        .iter()
        .filter_map(|req| {
            if let UserOperation::Response(response) = req {
                Some(response.clone())
            } else {
                None
            }
        })
        .collect();
    let proxy_reqs = get_proxy_request(origin_id, proxy_ops.as_slice());
    (proxy_reqs, other_reqs, responses, cancel_id, other_messages)
}

/// The CFDP Daemon is responsible for connecting [PDUTransport](crate::transport::PDUTransport) implementation
/// with each individual [Transaction](crate::transaction::Transaction). When a PDUTransport implementation
/// sends a PDU through a channel, the Daemon distributes the PDU to the necessary Transaction.
/// PDUs are sent from each Transaction directly to their respective PDUTransport implementations.
pub struct Daemon<T: FileStore + Send + 'static> {
    // The collection of all current transactions
    transaction_handles: Vec<JoinHandle<Result<TransactionID, TransactionError>>>,
    // the vector of transportation tx channel connections
    transport_tx_map: HashMap<EntityID, Sender<(VariableID, PDU)>>,
    // the vector of transportation rx channel connections
    transport_rx_vec: Vec<Receiver<PDU>>,
    // // mapping of unique transaction ids to channels used to talk to each transaction
    // transaction_channels: HashMap<(EntityID, Vec<u8>), Sender<Command>>,
    // the underlying filestore used by this Daemon
    filestore: Arc<Mutex<T>>,
    // message reciept channel used to execute User Operations
    message_rx: Receiver<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
    // message sender channel used to execute User Operations by Transactions
    message_tx: Sender<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
    // a mapping of individual fault handler actions per remote entity
    entity_configs: HashMap<VariableID, EntityConfig>,
    // the default fault handling configuration
    default_config: EntityConfig,
    // the entity ID of this daemon
    entity_id: EntityID,
    // current running count of the sequence numbers of transaction initiated by this entity
    sequence_num: TransactionSeqNum,
    // a mapping of originating transaction IDs to currently running Proxy Transactions
    proxy_id_map: HashMap<TransactionID, TransactionID>,
}
impl<T: FileStore + Send + 'static> Daemon<T> {
    fn spawn_receive_transaction(
        header: &PDUHeader,
        transport_tx: Sender<(VariableID, PDU)>,
        entity_config: &EntityConfig,
        filestore: Arc<Mutex<T>>,
        message_tx: Sender<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
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
            send_proxy_response: false,
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
                            Command::Pdu(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(crate::transaction::TransactionError::UnexpectedPDU(
                                        info,
                                    )) => {
                                        info!("Recieved Unexpected PDU: {:?}", info);
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
                        info!(
                            "Connection to Daemon Severed for Transaction {:?}",
                            transaction.id()
                        )
                    }
                }
                thread::sleep(Duration::from_millis(1));
            }
            Ok(transaction.id())
        });

        (id, transaction_tx, handle)
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_send_transaction(
        request: PutRequest,
        sequence_number: TransactionSeqNum,
        source_entity_id: EntityID,
        transport_tx: Sender<(EntityID, PDU)>,
        entity_config: &EntityConfig,
        filestore: Arc<Mutex<T>>,
        message_tx: Sender<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
        send_proxy_response: bool,
    ) -> Result<SpawnerTuple, Box<dyn std::error::Error + '_>> {
        let (transaction_tx, transaction_rx) = unbounded();
        let id = (source_entity_id.clone(), sequence_number.clone());

        let destination_entity_id = request.destination_entity_id.clone();
        let transmission_mode = request.transmission_mode.clone();
        let mut config = TransactionConfig {
            action_type: Action::Send,
            source_entity_id,
            destination_entity_id,
            transmission_mode,
            sequence_number,
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: entity_config.crc_flag.clone(),
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.default_inactivity_timeout,
            send_proxy_response,
        };
        let mut metadata = construct_metadata(request, entity_config, FileSizeSensitive::Small(0));

        let handle = thread::spawn(move || {
            let file_size = match filestore.lock()?.get_size(&metadata.source_filename)? {
                val if val <= u32::MAX as u64 => FileSizeSensitive::Small(val as u32),
                val => FileSizeSensitive::Large(val),
            };
            metadata.file_size = file_size;
            config.file_size_flag = match &metadata.file_size {
                FileSizeSensitive::Small(_) => FileSizeFlag::Small,
                FileSizeSensitive::Large(_) => FileSizeFlag::Large,
            };

            let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
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
                            Command::Pdu(pdu) => {
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
            Ok(transaction.id())
        });
        Ok((id, transaction_tx, handle))
    }

    /// This function will consist of the main logic loop in any daemon process.
    pub fn manage_transactions(&mut self) -> Result<(), Box<dyn std::error::Error + '_>> {
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

        let listener = LocalSocketListener::bind("/tmp/cdfp.socket")?;
        // setting to non-blocking lets us grab conections that are open
        // without blocking the entire thread.
        listener.set_nonblocking(true)?;

        let mut sequence_num = self.sequence_num.clone();
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

        let mut cleanup = Instant::now();

        while !terminate.load(Ordering::Relaxed) {
            match selector.ready_timeout(Duration::from_millis(500)) {
                Ok(val) if val == 0 => {
                    // this is a message to user
                    match self.message_rx.try_recv() {
                        Ok((origin_id, tx_mode, messages)) => {
                            let (put_requests, user_reqs, responses, cancel_id, other_messages) =
                                categorize_user_msg(&origin_id, messages);

                            for request in put_requests {
                                let sequence_number = sequence_num.get_and_increment();

                                let entity_config = self
                                    .entity_configs
                                    .get(&request.destination_entity_id)
                                    .unwrap_or(&self.default_config);

                                let transport_tx = self
                                    .transport_tx_map
                                    .get(&request.destination_entity_id)
                                    .expect("No transport for Entity ID.")
                                    .clone();

                                let (id, sender, handle) = Self::spawn_send_transaction(
                                    request,
                                    sequence_number,
                                    self.entity_id.clone(),
                                    transport_tx,
                                    entity_config,
                                    self.filestore.clone(),
                                    self.message_tx.clone(),
                                    true,
                                )?;
                                self.proxy_id_map.insert(origin_id.clone(), id.clone());
                                self.transaction_handles.push(handle);
                                transaction_channels.insert(id, sender);
                            }
                            if let Some(id) = cancel_id {
                                // Check if we have a running ID corresponding to the Originating ID in the cancel
                                if let Some(running_id) = self.proxy_id_map.get(&id) {
                                    // If the channel is still open to that transaction send a cancel.
                                    if let Some(channel) = transaction_channels.get(running_id) {
                                        channel.send(Command::Cancel)?;
                                    }
                                }
                            }

                            for req in user_reqs.into_iter() {
                                match req {
                                    UserRequest::DirectoryListing(directory_request) => {
                                        let request = match self.filestore.lock().map(|fs| {
                                            fs.list_directory(&directory_request.directory_name)
                                        }) {
                                            Ok(Ok(listing)) => {
                                                let outfile = directory_request
                                                    .directory_name
                                                    .as_path()
                                                    .with_extension(".listing");

                                                let response_code =match
                                                    self.filestore.lock().map(|fs| {
                                                        fs
                                                            .open(
                                                                &outfile,
                                                                OpenOptions::new()
                                                                    .create(true)
                                                                    .truncate(true)
                                                                    .write(true),
                                                            )
                                                            .map(|mut handle| {
                                                                handle.write_all(
                                                                    listing.as_bytes(),
                                                                )
                                                            })
                                                    }){
                                                        Ok(Ok(Ok(()))) => ListingResponseCode::Successful,
                                                        _ => ListingResponseCode::Unsuccessful,
                                                    };
                                                PutRequest {
                                                source_filename: outfile,
                                                destination_filename: directory_request.directory_filename.clone(),
                                                destination_entity_id: origin_id.0.clone(),
                                                transmission_mode: tx_mode.clone(),
                                                filestore_requests: vec![],
                                                message_to_user: vec![
                                                    MessageToUser::from(
                                                        UserOperation::OriginatingTransactionIDMessage(
                                                            OriginatingTransactionIDMessage{
                                                                source_entity_id: origin_id.0.clone(),
                                                                transaction_sequence_number: origin_id.1.clone(),
                                                            }
                                                        )
                                                    ),
                                                    MessageToUser::from(
                                                        UserOperation::Response(UserResponse::DirectoryListing(
                                                            DirectoryListingResponse{
                                                                response_code,
                                                                directory_name: directory_request.directory_name,
                                                                directory_filename: directory_request.directory_filename,
                                                            }
                                                        ))
                                                    ),
                                                ],
                                            }
                                            }
                                            Err(_) | Ok(Err(_)) => {

                                                PutRequest {
                                                source_filename: "".into(),
                                                destination_filename: "".into(),
                                                destination_entity_id: origin_id.0.clone(),
                                                transmission_mode: tx_mode.clone(),
                                                filestore_requests: vec![],
                                                message_to_user: vec![
                                                    MessageToUser::from(
                                                        UserOperation::OriginatingTransactionIDMessage(
                                                            OriginatingTransactionIDMessage{
                                                                source_entity_id: origin_id.0.clone(),
                                                                transaction_sequence_number: origin_id.1.clone(),
                                                            }
                                                        )
                                                    ),
                                                    MessageToUser::from(
                                                        UserOperation::Response(UserResponse::DirectoryListing(
                                                            DirectoryListingResponse{
                                                                response_code: ListingResponseCode::Unsuccessful,
                                                                directory_name: directory_request.directory_name,
                                                                directory_filename: directory_request.directory_filename,
                                                            }
                                                        ))
                                                    ),
                                                ],
                                            }
                                            }
                                        };

                                        {
                                            let sequence_number = sequence_num.get_and_increment();
                                            let entity_config = self
                                                .entity_configs
                                                .get(&request.destination_entity_id)
                                                .unwrap_or(&self.default_config);

                                            let transport_tx = self
                                                .transport_tx_map
                                                .get(&request.destination_entity_id)
                                                .expect("No transport for Entity ID.")
                                                .clone();

                                            let (id, sender, handle) =
                                                Self::spawn_send_transaction(
                                                    request,
                                                    sequence_number,
                                                    self.entity_id.clone(),
                                                    transport_tx,
                                                    entity_config,
                                                    self.filestore.clone(),
                                                    self.message_tx.clone(),
                                                    false,
                                                )?;
                                            self.transaction_handles.push(handle);
                                            transaction_channels.insert(id, sender);
                                        }
                                    }
                                    UserRequest::RemoteStatusReport(report_request) => {
                                        // TODO currently it is not possible to get the status of the working threads
                                        let request = PutRequest {
                                            source_filename: "".into(),
                                            destination_filename: "".into(),
                                            destination_entity_id: origin_id.0.clone(),
                                            transmission_mode: tx_mode.clone(),
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0.clone(),
                                                            transaction_sequence_number: origin_id
                                                                .1
                                                                .clone(),
                                                        },
                                                    ),
                                                ),
                                                MessageToUser::from(UserOperation::Response(
                                                    UserResponse::RemoteStatusReport(
                                                        RemoteStatusReportResponse {
                                                            transaction_status:
                                                                TransactionStatus::Unrecognized,
                                                            source_entity_id: report_request
                                                                .source_entity_id,
                                                            transaction_sequence_number:
                                                                report_request
                                                                    .transaction_sequence_number,
                                                            response_code: false,
                                                        },
                                                    ),
                                                )),
                                            ],
                                        };

                                        let sequence_number = sequence_num.get_and_increment();
                                        let entity_config = self
                                            .entity_configs
                                            .get(&request.destination_entity_id)
                                            .unwrap_or(&self.default_config);

                                        let transport_tx = self
                                            .transport_tx_map
                                            .get(&request.destination_entity_id)
                                            .expect("No transport for Entity ID.")
                                            .clone();

                                        let (id, sender, handle) = Self::spawn_send_transaction(
                                            request,
                                            sequence_number,
                                            self.entity_id.clone(),
                                            transport_tx,
                                            entity_config,
                                            self.filestore.clone(),
                                            self.message_tx.clone(),
                                            false,
                                        )?;
                                        self.transaction_handles.push(handle);
                                        transaction_channels.insert(id, sender);
                                    }
                                    UserRequest::RemoteSuspend(suspend_req) => {
                                        let suspend_indication = match transaction_channels.get(&(
                                            suspend_req.source_entity_id.clone(),
                                            suspend_req.transaction_sequence_number.clone(),
                                        )) {
                                            Some(chan) => chan.send(Command::Suspend).is_ok(),
                                            None => false,
                                        };

                                        let request = PutRequest {
                                            source_filename: "".into(),
                                            destination_filename: "".into(),
                                            destination_entity_id: origin_id.0.clone(),
                                            transmission_mode: tx_mode.clone(),
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0.clone(),
                                                            transaction_sequence_number: origin_id
                                                                .1
                                                                .clone(),
                                                        },
                                                    ),
                                                ),
                                                MessageToUser::from(UserOperation::Response(
                                                    UserResponse::RemoteSuspend(
                                                        RemoteSuspendResponse {
                                                            suspend_indication,
                                                            transaction_status:
                                                                TransactionStatus::Unrecognized,
                                                            source_entity_id: suspend_req
                                                                .source_entity_id,
                                                            transaction_sequence_number:
                                                                suspend_req
                                                                    .transaction_sequence_number,
                                                        },
                                                    ),
                                                )),
                                            ],
                                        };

                                        let sequence_number = sequence_num.get_and_increment();
                                        let entity_config = self
                                            .entity_configs
                                            .get(&request.destination_entity_id)
                                            .unwrap_or(&self.default_config);

                                        let transport_tx = self
                                            .transport_tx_map
                                            .get(&request.destination_entity_id)
                                            .expect("No transport for Entity ID.")
                                            .clone();

                                        let (id, sender, handle) = Self::spawn_send_transaction(
                                            request,
                                            sequence_number,
                                            self.entity_id.clone(),
                                            transport_tx,
                                            entity_config,
                                            self.filestore.clone(),
                                            self.message_tx.clone(),
                                            false,
                                        )?;
                                        self.transaction_handles.push(handle);
                                        transaction_channels.insert(id, sender);
                                    }
                                    UserRequest::RemoteResume(resume_request) => {
                                        let suspend_indication = match transaction_channels.get(&(
                                            resume_request.source_entity_id.clone(),
                                            resume_request.transaction_sequence_number.clone(),
                                        )) {
                                            Some(chan) => chan.send(Command::Resume).is_err(),
                                            None => true,
                                        };

                                        let request = PutRequest {
                                            source_filename: "".into(),
                                            destination_filename: "".into(),
                                            destination_entity_id: origin_id.0.clone(),
                                            transmission_mode: tx_mode.clone(),
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0.clone(),
                                                            transaction_sequence_number: origin_id
                                                                .1
                                                                .clone(),
                                                        },
                                                    ),
                                                ),
                                                MessageToUser::from(UserOperation::Response(
                                                    UserResponse::RemoteSuspend(
                                                        RemoteSuspendResponse {
                                                            suspend_indication,
                                                            transaction_status:
                                                                TransactionStatus::Unrecognized,
                                                            source_entity_id: resume_request
                                                                .source_entity_id,
                                                            transaction_sequence_number:
                                                                resume_request
                                                                    .transaction_sequence_number,
                                                        },
                                                    ),
                                                )),
                                            ],
                                        };

                                        let sequence_number = sequence_num.get_and_increment();
                                        let entity_config = self
                                            .entity_configs
                                            .get(&request.destination_entity_id)
                                            .unwrap_or(&self.default_config);

                                        let transport_tx = self
                                            .transport_tx_map
                                            .get(&request.destination_entity_id)
                                            .expect("No transport for Entity ID.")
                                            .clone();

                                        let (id, sender, handle) = Self::spawn_send_transaction(
                                            request,
                                            sequence_number,
                                            self.entity_id.clone(),
                                            transport_tx,
                                            entity_config,
                                            self.filestore.clone(),
                                            self.message_tx.clone(),
                                            false,
                                        )?;
                                        self.transaction_handles.push(handle);
                                        transaction_channels.insert(id, sender);
                                    }
                                }
                            }
                            for response in responses {
                                // log indication of the response received!
                                info!("Received User Operation Response: {:?}", response);
                            }
                            for message in other_messages {
                                // also log this!
                                info!("Received Messages I can't decifer {:?}", message);
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
                    };
                }
                Ok(val) if val > 0 => {
                    // this is a pdu from a transport
                    // subtract 1 because the transport indices start at 1 for the selector
                    // but are 0 indexed in the vec
                    let rx = &self.transport_rx_vec[val - 1];
                    match rx.try_recv() {
                        Ok(pdu) => {
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
                                        // TODO! Fill in this error
                                        self.transport_tx_map
                                            .get(&key.0)
                                            .expect("No transport for Entity ID.")
                                            .clone(),
                                        entity_config,
                                        self.filestore.clone(),
                                        self.message_tx.clone(),
                                    );

                                    self.transaction_handles.push(handle);
                                    channel
                                });

                            match channel
                                .send_timeout(Command::Pdu(pdu.clone()), Duration::from_millis(500))
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
                                            self.transport_tx_map
                                                .get(&key.0)
                                                .expect("No transport for Entity ID.")
                                                .clone(),
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

            for mut conn in listener.incoming().filter_map(Result::ok) {
                let mut buffer = Vec::<u8>::new();
                let _nread = conn.read_to_end(&mut buffer)?;
                let primitive = UserPrimitive::decode(&mut &buffer[..])?;
                match primitive {
                    UserPrimitive::Put(request) => {
                        let sequence_number = sequence_num.get_and_increment();

                        let entity_config = self
                            .entity_configs
                            .get(&request.destination_entity_id)
                            .unwrap_or(&self.default_config);

                        let transport_tx = self
                            .transport_tx_map
                            .get(&request.destination_entity_id)
                            .expect("No transport for Entity ID.")
                            .clone();

                        let (id, sender, handle) = Self::spawn_send_transaction(
                            request,
                            sequence_number,
                            self.entity_id.clone(),
                            transport_tx,
                            entity_config,
                            self.filestore.clone(),
                            self.message_tx.clone(),
                            false,
                        )?;
                        self.transaction_handles.push(handle);
                        transaction_channels.insert(id, sender);
                    }
                    UserPrimitive::Cancel(id, seq) => match transaction_channels.get(&(id, seq)) {
                        Some(channel) => channel.send(Command::Cancel)?,
                        None => {}
                    },
                    UserPrimitive::Suspend(id, seq) => match transaction_channels.get(&(id, seq)) {
                        Some(channel) => channel.send(Command::Suspend)?,
                        None => {}
                    },
                    UserPrimitive::Resume(id, seq) => match transaction_channels.get(&(id, seq)) {
                        Some(channel) => channel.send(Command::Resume)?,
                        None => {}
                    },
                    UserPrimitive::Report(_id, _seq) => todo!(),
                }
            }
            // join any handles that have completed
            // maybe should only run every so often?
            if cleanup.elapsed() >= Duration::from_secs(10) {
                let mut ind = 0;
                while ind < self.transaction_handles.len() {
                    if self.transaction_handles[ind].is_finished() {
                        let handle = self.transaction_handles.remove(ind);
                        match handle.join() {
                            Ok(Ok(id)) => {
                                // remove the channel for this transaction if it is complete
                                let _ = transaction_channels.remove(&id);
                                // keep all proxy id maps where the finished transaction ID is not the entry
                                self.proxy_id_map.retain(|_, value| *value != id);
                            }
                            Ok(Err(err)) => {
                                info!("Error occured during transaction: {}", err)
                            }
                            Err(_) => error!("Unable to join handle!"),
                        };
                    } else {
                        ind += 1;
                    }
                }

                cleanup = Instant::now();
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

#[cfg(test)]
mod test {
    use super::*;

    use crate::pdu::{
        DeliveryCode, DirectoryListingRequest, FileStatusCode, FileStoreAction, FileStoreRequest,
        ProxyPutResponse, RemoteResumeRequest,
    };

    use rstest::rstest;

    #[rstest]
    #[case("", "")]
    #[case("a_first/name.txt", "")]
    #[case("a_first/name.txt", "b/second/name.txt")]
    #[case("", "b/second/name.txt")]
    fn put_encode(
        #[case] source_filename: Utf8PathBuf,
        #[case] destination_filename: Utf8PathBuf,
        #[values(vec![], vec![
            FileStoreRequest{
                action_code: FileStoreAction::AppendFile,
                first_filename: "some_name_here.txt".as_bytes().to_vec(),
                second_filename: "another_name.txt".as_bytes().to_vec(),
            },
            FileStoreRequest{
                action_code: FileStoreAction::RenameFile,
                first_filename: "some_name_here.txt".as_bytes().to_vec(),
                second_filename: "another_name.txt".as_bytes().to_vec(),
            }
        ])]
        filestore_requests: Vec<FileStoreRequest>,
        #[values(vec![], vec![
            MessageToUser{message_text: "some text billy!".as_bytes().to_vec()},
            MessageToUser{message_text: "cfdp \nmessage here!".as_bytes().to_vec()}

        ])]
        message_to_user: Vec<MessageToUser>,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
        #[values(
            EntityID::from(1_u8),
            EntityID::from(300_u16),
            EntityID::from(105748_u32),
            EntityID::from(846372858564_u64)
        )]
        destination_entity_id: EntityID,
    ) {
        let expected = PutRequest {
            source_filename,
            destination_filename,
            filestore_requests,
            message_to_user,
            destination_entity_id,
            transmission_mode,
        };

        let buffer = expected.clone().encode();
        let recovered = PutRequest::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn primitive_encode(
        #[values(
            UserPrimitive::Put(
                PutRequest{
                    source_filename: "test".into(),
                    destination_filename: "out_file".into(),
                    destination_entity_id: EntityID::from(32_u32),
                    transmission_mode: TransmissionMode::Acknowledged,
                    filestore_requests: vec![
                        FileStoreRequest{
                            action_code:FileStoreAction::CreateDirectory,
                            first_filename:"/tmp/help".as_bytes().to_vec(),
                            second_filename:vec![]}],
                    message_to_user: vec![MessageToUser{message_text: "do something".as_bytes().to_vec()}],
                }
            ),
            UserPrimitive::Cancel(EntityID::from(1_u8), TransactionSeqNum::from(3_u8)),
            UserPrimitive::Suspend(EntityID::from(10_u16), TransactionSeqNum::from(400_u16)),
            UserPrimitive::Resume(
                EntityID::from(871838474_u64),
                TransactionSeqNum::from(871838447_u64)
            ),
            UserPrimitive::Report(EntityID::from(12_u16), TransactionSeqNum::from(33_u16))
        )]
        expected: UserPrimitive,
    ) {
        let buffer = expected.clone().encode();
        let recovered = UserPrimitive::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn proxy_req(#[values(true, false)] use_mode: bool) {
        let origin_id = (EntityID::from(55_u16), TransactionSeqNum::from(12_u16));
        let mut messages = vec![
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::CreateDirectory,
                first_filename: "/tmp".as_bytes().to_vec(),
                second_filename: vec![],
            }),
            ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                destination_entity_id: EntityID::from(3_u16),
                source_filename: "test_file".into(),
                destination_filename: "out_file".into(),
            }),
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: "first_file".as_bytes().to_vec(),
                second_filename: "second_file".as_bytes().to_vec(),
            }),
            ProxyOperation::ProxyMessageToUser(MessageToUser {
                message_text: "help".as_bytes().to_vec(),
            }),
        ];

        if use_mode {
            messages.push(ProxyOperation::ProxyTransmissionMode(
                TransmissionMode::Acknowledged,
            ));
        }

        let recovered = get_proxy_request(&origin_id, messages.as_slice());

        let expected = PutRequest {
            source_filename: "test_file".into(),
            destination_filename: "out_file".into(),
            destination_entity_id: EntityID::from(3_u16),
            transmission_mode: if use_mode {
                TransmissionMode::Acknowledged
            } else {
                TransmissionMode::Unacknowledged
            },
            filestore_requests: vec![
                FileStoreRequest {
                    action_code: FileStoreAction::CreateDirectory,
                    first_filename: "/tmp".as_bytes().to_vec(),
                    second_filename: vec![],
                },
                FileStoreRequest {
                    action_code: FileStoreAction::AppendFile,
                    first_filename: "first_file".as_bytes().to_vec(),
                    second_filename: "second_file".as_bytes().to_vec(),
                },
            ],
            message_to_user: vec![
                MessageToUser {
                    message_text: "help".as_bytes().to_vec(),
                },
                MessageToUser {
                    message_text: UserOperation::OriginatingTransactionIDMessage(
                        OriginatingTransactionIDMessage {
                            source_entity_id: origin_id.0.clone(),
                            transaction_sequence_number: origin_id.1,
                        },
                    )
                    .encode(),
                },
            ],
        };
        assert_eq!(1, recovered.len());
        assert_eq!(expected, recovered[0])
    }

    #[test]
    fn categorize_user_message() {
        let origin_id = (EntityID::from(55_u16), TransactionSeqNum::from(12_u16));
        let proxy_ops = vec![
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::CreateDirectory,
                first_filename: "/tmp".as_bytes().to_vec(),
                second_filename: vec![],
            }),
            ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                destination_entity_id: EntityID::from(3_u16),
                source_filename: "test_file".into(),
                destination_filename: "out_file".into(),
            }),
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: "first_file".as_bytes().to_vec(),
                second_filename: "second_file".as_bytes().to_vec(),
            }),
            ProxyOperation::ProxyMessageToUser(MessageToUser {
                message_text: "help".as_bytes().to_vec(),
            }),
            ProxyOperation::ProxyTransmissionMode(TransmissionMode::Acknowledged),
        ];

        let put_requests = vec![PutRequest {
            source_filename: "test_file".into(),
            destination_filename: "out_file".into(),
            destination_entity_id: EntityID::from(3_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![
                FileStoreRequest {
                    action_code: FileStoreAction::CreateDirectory,
                    first_filename: "/tmp".as_bytes().to_vec(),
                    second_filename: vec![],
                },
                FileStoreRequest {
                    action_code: FileStoreAction::AppendFile,
                    first_filename: "first_file".as_bytes().to_vec(),
                    second_filename: "second_file".as_bytes().to_vec(),
                },
            ],
            message_to_user: vec![
                MessageToUser {
                    message_text: "help".as_bytes().to_vec(),
                },
                MessageToUser::from(UserOperation::OriginatingTransactionIDMessage(
                    OriginatingTransactionIDMessage {
                        source_entity_id: EntityID::from(55_u16),
                        transaction_sequence_number: TransactionSeqNum::from(12_u16),
                    },
                )),
            ],
        }];

        let requests = vec![
            UserRequest::DirectoryListing(DirectoryListingRequest {
                directory_name: "/home/do".into(),
                directory_filename: "/home/do.listing".into(),
            }),
            UserRequest::RemoteResume(RemoteResumeRequest {
                source_entity_id: EntityID::from(1_u16),
                transaction_sequence_number: TransactionSeqNum::from(2_u16),
            }),
        ];

        let responses = vec![UserResponse::ProxyPut(ProxyPutResponse {
            condition: Condition::FileChecksumFailure,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
        })];

        let other_message = vec![MessageToUser {
            message_text: "help".as_bytes().to_vec(),
        }];

        let cancel_id: TransactionID = (EntityID::from(16_u16), TransactionSeqNum::from(3_u32));

        let mut user_messages: Vec<MessageToUser> = proxy_ops
            .iter()
            .map(|msg| MessageToUser::from(UserOperation::ProxyOperation(msg.clone())))
            .chain(
                responses
                    .iter()
                    .map(|resp| MessageToUser::from(UserOperation::Response(resp.clone()))),
            )
            .chain(
                requests
                    .iter()
                    .map(|req| MessageToUser::from(UserOperation::Request(req.clone()))),
            )
            .chain(other_message.clone().into_iter())
            .collect();
        user_messages.extend(vec![
            MessageToUser::from(UserOperation::ProxyOperation(
                ProxyOperation::ProxyPutCancel,
            )),
            MessageToUser::from(UserOperation::OriginatingTransactionIDMessage(
                OriginatingTransactionIDMessage {
                    source_entity_id: EntityID::from(16_u16),
                    transaction_sequence_number: TransactionSeqNum::from(3_u32),
                },
            )),
        ]);

        let (proxy, req, resp, cancel, message) = categorize_user_msg(&origin_id, user_messages);

        assert_eq!(put_requests, proxy);
        assert_eq!(requests, req);
        assert_eq!(responses, resp);
        assert_eq!(cancel_id, cancel.unwrap());
        assert_eq!(other_message, message)
    }
}
