use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{Error as IOError, Read, Write},
    string::FromUtf8Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use camino::Utf8PathBuf;
use crossbeam_channel::{bounded, unbounded, Receiver, Select, Sender, TryRecvError};
use itertools::{Either, Itertools};
use log::{error, info};
use num_traits::FromPrimitive;
use thiserror::Error;

use crate::{
    filestore::{ChecksumType, FileStore},
    pdu::{
        error::PDUError, CRCFlag, Condition, Direction, DirectoryListingResponse, EntityID,
        FaultHandlerAction, FileSizeFlag, FileStoreRequest, ListingResponseCode, MessageToUser,
        OriginatingTransactionIDMessage, PDUEncode, PDUHeader, ProxyOperation, ProxyPutRequest,
        RemoteStatusReportResponse, RemoteSuspendResponse, SegmentedData, TransactionSeqNum,
        TransactionStatus, TransmissionMode, UserOperation, UserRequest, UserResponse, VariableID,
        PDU,
    },
    transaction::{
        Metadata, RecvTransaction, SendTransaction, TransactionConfig, TransactionError,
        TransactionID, TransactionState,
    },
    transport::PDUTransport,
    user::{User, UserReturn},
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
        buffer.push(self.destination_entity_id.encoded_len() as u8);
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

type PrimitiveResult<T> = Result<T, PrimitiveError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Possible User Primitives sent from a end user application to the
/// interprocess pipe.
pub enum UserPrimitive {
    /// Initiate a Put transaction with the specificed [PutRequest] configuration.
    Put(PutRequest),
    /// Cancel the give transaction.
    Cancel(TransactionID),
    /// Suspend operations of the given transaction.
    Suspend(TransactionID),
    /// Resume operations of the given transaction.
    Resume(TransactionID),
    /// Report progress of the given transaction.
    Report(TransactionID),
}
impl UserPrimitive {
    pub fn encode(self) -> Vec<u8> {
        match self {
            Self::Put(request) => {
                let mut buff: Vec<u8> = vec![0_u8];
                buff.extend(request.encode());
                buff
            }
            Self::Cancel(id) => {
                let mut buff: Vec<u8> = vec![1_u8];
                buff.extend(id.0.encode());
                buff.extend(id.1.encode());
                buff
            }
            Self::Suspend(id) => {
                let mut buff: Vec<u8> = vec![2_u8];
                buff.extend(id.0.encode());
                buff.extend(id.1.encode());
                buff
            }
            Self::Resume(id) => {
                let mut buff: Vec<u8> = vec![3_u8];
                buff.extend(id.0.encode());
                buff.extend(id.1.encode());
                buff
            }
            Self::Report(id) => {
                let mut buff: Vec<u8> = vec![4_u8];
                buff.extend(id.0.encode());
                buff.extend(id.1.encode());
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
                Ok(Self::Cancel(TransactionID(id, seq)))
            }
            2 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Suspend(TransactionID(id, seq)))
            }
            3 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Resume(TransactionID(id, seq)))
            }
            4 => {
                let id = VariableID::decode(buffer)?;
                let seq = VariableID::decode(buffer)?;
                Ok(Self::Report(TransactionID(id, seq)))
            }
            _ => Err(PrimitiveError::UnexpextedPrimitive),
        }
    }
}

/// Lightweight commands
#[derive(Debug)]
enum Command {
    Pdu(PDU),
    Cancel,
    Suspend,
    Resume,
    Report(Sender<Report>),
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

type SpawnerTuple = (
    TransactionID,
    Sender<Command>,
    JoinHandle<Result<Report, TransactionError>>,
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
                ProxyOperation::ProxyTransmissionMode(mode) => Some(*mode),
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
                source_entity_id: origin_id.0,
                transaction_sequence_number: origin_id.1,
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
                    Some(TransactionID(
                        origin.source_entity_id,
                        origin.transaction_sequence_number,
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
    transaction_handles: Vec<JoinHandle<Result<Report, TransactionError>>>,
    // the vector of transportation tx channel connections
    transport_tx_map: HashMap<EntityID, Sender<(VariableID, PDU)>>,
    // the transport PDU rx channel connection
    transport_rx: Receiver<PDU>,
    // // mapping of unique transaction ids to channels used to talk to each transaction
    // transaction_channels: HashMap<(EntityID, Vec<u8>), Sender<Command>>,
    // the underlying filestore used by this Daemon
    filestore: Arc<T>,
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
    // termination signal sent to children threads
    terminate: Arc<AtomicBool>,
    // channel to receive user primitives from the implemented User
    primitive_rx: Receiver<(UserPrimitive, Sender<UserReturn>)>,
    // channel to send user primitives from the implemented User
    // this is passed to SendTransactions in order to propagate up ProxyPutRequests
    primitive_tx: Sender<(UserPrimitive, Sender<UserReturn>)>,
    // history of transactions this daemon has participated in
    history: HashMap<TransactionID, Report>,
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
        terminate: Arc<AtomicBool>,
        user_interface: Box<dyn User + Send>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (primitive_tx, primitive_rx) = bounded(1);

        let signal = terminate.clone();
        let mut user_interface = user_interface;
        let user_tx = primitive_tx.clone();
        thread::spawn(move || user_interface.primitive_handler(signal, user_tx));

        let (message_tx, message_rx) = unbounded();
        let mut transport_tx_map: HashMap<EntityID, Sender<(VariableID, PDU)>> = HashMap::new();

        let (pdu_send, pdu_receive) = unbounded();
        for (vec, mut transport) in transport_map.into_iter() {
            let (remote_send, remote_receive) = bounded(1);

            vec.iter().for_each(|id| {
                transport_tx_map.insert(*id, remote_send.clone());
            });

            let signal = terminate.clone();
            let sender = pdu_send.clone();
            thread::spawn(move || transport.pdu_handler(signal, sender, remote_receive));
        }
        Ok(Self {
            transaction_handles: vec![],
            transport_tx_map,
            transport_rx: pdu_receive,
            filestore,
            message_rx,
            message_tx,
            entity_configs,
            default_config,
            entity_id,
            sequence_num,
            proxy_id_map: HashMap::new(),
            terminate,
            primitive_rx,
            primitive_tx,
            history: HashMap::new(),
        })
    }
    fn spawn_receive_transaction(
        header: &PDUHeader,
        transport_tx: Sender<(VariableID, PDU)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        message_tx: Sender<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
    ) -> Result<SpawnerTuple, Box<dyn std::error::Error>> {
        let (transaction_tx, transaction_rx) = unbounded();

        let config = TransactionConfig {
            source_entity_id: header.source_entity_id,
            destination_entity_id: header.destination_entity_id,
            transmission_mode: header.transmission_mode,
            sequence_number: header.transaction_sequence_number,
            file_size_flag: header.large_file_flag,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: header.crc_flag,
            segment_metadata_flag: header.segment_metadata_flag.clone(),
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.inactivity_timeout,
            ack_timeout: entity_config.ack_timeout,
            nak_timeout: entity_config.nak_timeout,
            send_proxy_response: false,
        };
        let name = format!(
            "({:?}, {:?})",
            &config.source_entity_id, &config.sequence_number
        );
        let mut transaction =
            RecvTransaction::new(config, entity_config.nak_procedure, filestore, message_tx);
        let id = transaction.id();

        let handle = thread::Builder::new().name(name).spawn(move || {
            let mut sel = Select::new();
            let rx_select_id = sel.recv(&transaction_rx);

            let mut tx_select_id = Option::<usize>::None;

            while transaction.get_state() != &TransactionState::Terminated {
                if transaction.has_pdu_to_send() {
                    tx_select_id.get_or_insert_with(||sel.send(&transport_tx));
                } else if let Some(idx) = tx_select_id.take() {
                    sel.remove(idx);
                }

                let timeout = transaction.until_timeout();
                let oper = sel.ready_timeout(timeout);
                match oper {
                    Err(_) => {
                        transaction.handle_timeout()?;
                    }
                    Ok(id) => {
                        if tx_select_id == Some(id) {
                            transaction.send_pdu(&transport_tx)?;
                        } else if id == rx_select_id {
                            match transaction_rx.try_recv() {
                                Ok(command) => {
                                    match command {
                                        Command::Pdu(pdu) => {
                                            match transaction.process_pdu(pdu) {
                                                Ok(()) => {}
                                                Err(err @ TransactionError::UnexpectedPDU(..)) => {
                                                    info!("Transaction {:?} Received Unexpected PDU: {err}", transaction.id());
                                                    // log some info on the unexpected PDU?
                                                }
                                                Err(err) => return Err(err),
                                            }
                                        }
                                        Command::Resume => transaction.resume(),
                                        Command::Cancel => transaction.cancel()?,
                                        Command::Suspend => transaction.suspend(),
                                        Command::Abandon => transaction.shutdown(),
                                        Command::Report(sender) => {
                                            sender.send(transaction.generate_report())?
                                        }
                                    }
                                }
                                Err(TryRecvError::Empty) => {
                                    // this normally should not happen
                                }
                                Err(TryRecvError::Disconnected) => {
                                    // Really do not expect to be in this situation
                                    // probably the thread should exit
                                    info!(
                                        "Connection to Daemon Severed for Transaction {:?}",
                                        transaction.id()
                                    )
                                }
                            };
                        }
                    }
                }
            }
            Ok(transaction.generate_report())
        })?;

        Ok((id, transaction_tx, handle))
    }

    fn get_report(
        id: TransactionID,
        channels: &HashMap<TransactionID, Sender<Command>>,
    ) -> Option<Report> {
        channels
            .get(&id)
            .and_then(|chan| {
                let (tx, rx) = bounded(1);
                chan.send(Command::Report(tx)).map(|_| rx.recv().ok()).ok()
            })
            .flatten()
    }
    #[allow(clippy::too_many_arguments)]
    fn spawn_send_transaction(
        request: PutRequest,
        sequence_number: TransactionSeqNum,
        source_entity_id: EntityID,
        transport_tx: Sender<(EntityID, PDU)>,
        primtive_tx: Sender<(UserPrimitive, Sender<UserReturn>)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        send_proxy_response: bool,
    ) -> Result<SpawnerTuple, Box<dyn std::error::Error>> {
        let (transaction_tx, transaction_rx) = unbounded();
        let id = TransactionID(source_entity_id, sequence_number);

        let destination_entity_id = request.destination_entity_id;
        let transmission_mode = request.transmission_mode;
        let mut config = TransactionConfig {
            source_entity_id,
            destination_entity_id,
            transmission_mode,
            sequence_number,
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: entity_config.crc_flag,
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.inactivity_timeout,
            ack_timeout: entity_config.ack_timeout,
            nak_timeout: entity_config.nak_timeout,
            send_proxy_response,
        };
        let mut metadata = construct_metadata(request, entity_config, 0_u64);

        let handle = thread::Builder::new()
            .name(format!(
                "({:?}, {:?})",
                config.source_entity_id, config.sequence_number
            ))
            .spawn(move || {
                let file_size = match &metadata.source_filename.file_name().is_none() {
                    true => 0_u64,
                    false => filestore.get_size(&metadata.source_filename)?,
                };

                metadata.file_size = file_size;
                config.file_size_flag = match metadata.file_size <= u32::MAX.into() {
                    true => FileSizeFlag::Small,
                    false => FileSizeFlag::Large,
                };

                let mut transaction =
                    SendTransaction::new(config, metadata, filestore, primtive_tx);
                let mut sel = Select::new();
                let rx_select_id = sel.recv(&transaction_rx);

                let mut tx_select_id = Option::<usize>::None;

                while transaction.get_state() != &TransactionState::Terminated {
                    if transaction.has_pdu_to_send() {
                        if tx_select_id.is_none() {
                            tx_select_id = Some(sel.send(&transport_tx));
                        }
                    } else if let Some(idx) = tx_select_id {
                        sel.remove(idx);
                        tx_select_id = None;
                    }

                    let timeout = transaction.until_timeout();
                    let oper = sel.ready_timeout(timeout);

                    match oper {
                        Err(_) => {
                            transaction.handle_timeout()?;
                        }
                        Ok(id) => {
                            if tx_select_id == Some(id) {
                                // println!("transport_tx capacity :{}", transport_tx.len());
                                transaction.send_pdu(&transport_tx)?;
                                // println!("dupa transport_tx capacity :{}", transport_tx.len());
                            } else if id == rx_select_id {
                                match transaction_rx.try_recv() {
                                    Ok(command) => {
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
                                            Command::Resume => transaction.resume(),
                                            Command::Cancel => transaction.cancel()?,
                                            Command::Suspend => transaction.suspend(),
                                            Command::Abandon => transaction.shutdown(),
                                            Command::Report(sender) => {
                                                sender.send(transaction.generate_report())?
                                            }
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
                            }
                        }
                    }
                }
                Ok(transaction.generate_report())
            })?;
        Ok((id, transaction_tx, handle))
    }

    /// This function will consist of the main logic loop in any daemon process.
    pub fn manage_transactions(&mut self) -> Result<(), Box<dyn std::error::Error + '_>> {
        let mut sequence_num = self.sequence_num;

        // Create the selection object to check if any messages are available.
        // the returned index will be used to determine which action to take.

        let mut selector = Select::new();
        selector.recv(&self.message_rx);
        selector.recv(&self.transport_rx);
        selector.recv(&self.primitive_rx);

        // mapping of unique transaction ids to channels used to talk to each transaction
        let mut transaction_channels: HashMap<TransactionID, Sender<Command>> = HashMap::new();

        let mut cleanup = Instant::now();

        while !self.terminate.load(Ordering::Relaxed) {
            match selector.select_timeout(Duration::from_millis(1000)) {
                Ok(oper) if oper.index() == 0 => {
                    // this is a message to user
                    match oper.recv(&self.message_rx) {
                        Ok((origin_id, tx_mode, messages)) => {
                            let (put_requests, user_reqs, responses, cancel_id, other_messages) =
                                categorize_user_msg(&origin_id, messages);

                            for request in put_requests {
                                let sequence_number = sequence_num.get_and_increment();

                                let entity_config = self
                                    .entity_configs
                                    .get(&request.destination_entity_id)
                                    .unwrap_or(&self.default_config)
                                    .clone();

                                let transport_tx = self
                                    .transport_tx_map
                                    .get(&request.destination_entity_id)
                                    .expect("No transport for Entity ID.")
                                    .clone();

                                let (id, sender, handle) = Self::spawn_send_transaction(
                                    request,
                                    sequence_number,
                                    self.entity_id,
                                    transport_tx,
                                    self.primitive_tx.clone(),
                                    entity_config,
                                    self.filestore.clone(),
                                    true,
                                )?;
                                self.proxy_id_map.insert(origin_id, id);

                                let response = Self::get_report(id, &transaction_channels);
                                if let Some(report) = response {
                                    self.history.insert(id, report);
                                }

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
                                        let request = match self
                                            .filestore
                                            .list_directory(&directory_request.directory_name)
                                        {
                                            Ok(listing) => {
                                                let outfile = directory_request
                                                    .directory_name
                                                    .as_path()
                                                    .with_extension(".listing");

                                                let response_code = match self
                                                    .filestore
                                                    .open(
                                                        &outfile,
                                                        OpenOptions::new().create(true).truncate(true).write(true),
                                                    )
                                                    .map(|mut handle| handle.write_all(listing.as_bytes()))
                                                {
                                                    Ok(Ok(())) => ListingResponseCode::Successful,
                                                    _ => ListingResponseCode::Unsuccessful,
                                                };
                                                PutRequest {
                                                    source_filename: outfile,
                                                    destination_filename: directory_request.directory_filename.clone(),
                                                    destination_entity_id: origin_id.0,
                                                    transmission_mode: tx_mode,
                                                    filestore_requests: vec![],
                                                    message_to_user: vec![
                                                        MessageToUser::from(
                                                            UserOperation::OriginatingTransactionIDMessage(
                                                                OriginatingTransactionIDMessage {
                                                                    source_entity_id: origin_id.0,
                                                                    transaction_sequence_number: origin_id.1,
                                                                },
                                                            ),
                                                        ),
                                                        MessageToUser::from(UserOperation::Response(
                                                            UserResponse::DirectoryListing(DirectoryListingResponse {
                                                                response_code,
                                                                directory_name: directory_request.directory_name,
                                                                directory_filename: directory_request
                                                                    .directory_filename,
                                                            }),
                                                        )),
                                                    ],
                                                }
                                            }
                                            Err(_) => PutRequest {
                                                source_filename: "".into(),
                                                destination_filename: "".into(),
                                                destination_entity_id: origin_id.0,
                                                transmission_mode: tx_mode,
                                                filestore_requests: vec![],
                                                message_to_user: vec![
                                                    MessageToUser::from(
                                                        UserOperation::OriginatingTransactionIDMessage(
                                                            OriginatingTransactionIDMessage {
                                                                source_entity_id: origin_id.0,
                                                                transaction_sequence_number: origin_id.1,
                                                            },
                                                        ),
                                                    ),
                                                    MessageToUser::from(UserOperation::Response(
                                                        UserResponse::DirectoryListing(DirectoryListingResponse {
                                                            response_code: ListingResponseCode::Unsuccessful,
                                                            directory_name: directory_request.directory_name,
                                                            directory_filename: directory_request.directory_filename,
                                                        }),
                                                    )),
                                                ],
                                            },
                                        };

                                        {
                                            let sequence_number = sequence_num.get_and_increment();
                                            let entity_config = self
                                                .entity_configs
                                                .get(&request.destination_entity_id)
                                                .unwrap_or(&self.default_config)
                                                .clone();

                                            let transport_tx = self
                                                .transport_tx_map
                                                .get(&request.destination_entity_id)
                                                .expect("No transport for Entity ID.")
                                                .clone();

                                            let (id, sender, handle) =
                                                Self::spawn_send_transaction(
                                                    request,
                                                    sequence_number,
                                                    self.entity_id,
                                                    transport_tx,
                                                    self.primitive_tx.clone(),
                                                    entity_config,
                                                    self.filestore.clone(),
                                                    false,
                                                )?;

                                            let response =
                                                Self::get_report(id, &transaction_channels);
                                            if let Some(report) = response {
                                                self.history.insert(id, report);
                                            }
                                            self.transaction_handles.push(handle);
                                            transaction_channels.insert(id, sender);
                                        }
                                    }
                                    UserRequest::RemoteStatusReport(report_request) => {
                                        let report = Self::get_report(
                                            TransactionID(
                                                report_request.source_entity_id,
                                                report_request.transaction_sequence_number,
                                            ),
                                            &transaction_channels,
                                        );

                                        let response = {
                                            match report {
                                                Some(data) => RemoteStatusReportResponse {
                                                    transaction_status: data.status,
                                                    source_entity_id: data.id.0,
                                                    transaction_sequence_number: data.id.1,
                                                    response_code: true,
                                                },
                                                None => RemoteStatusReportResponse {
                                                    transaction_status:
                                                        TransactionStatus::Unrecognized,
                                                    source_entity_id: report_request
                                                        .source_entity_id,
                                                    transaction_sequence_number: report_request
                                                        .transaction_sequence_number,
                                                    response_code: false,
                                                },
                                            }
                                        };
                                        let request = PutRequest {
                                            source_filename: "".into(),
                                            destination_filename: "".into(),
                                            destination_entity_id: origin_id.0,
                                            transmission_mode: tx_mode,
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0,
                                                            transaction_sequence_number: origin_id
                                                                .1,
                                                        },
                                                    ),
                                                ),
                                                MessageToUser::from(UserOperation::Response(
                                                    UserResponse::RemoteStatusReport(response),
                                                )),
                                            ],
                                        };

                                        let sequence_number = sequence_num.get_and_increment();
                                        let entity_config = self
                                            .entity_configs
                                            .get(&request.destination_entity_id)
                                            .unwrap_or(&self.default_config)
                                            .clone();

                                        let transport_tx = self
                                            .transport_tx_map
                                            .get(&request.destination_entity_id)
                                            .expect("No transport for Entity ID.")
                                            .clone();

                                        let (id, sender, handle) = Self::spawn_send_transaction(
                                            request,
                                            sequence_number,
                                            self.entity_id,
                                            transport_tx,
                                            self.primitive_tx.clone(),
                                            entity_config,
                                            self.filestore.clone(),
                                            false,
                                        )?;

                                        let response = Self::get_report(id, &transaction_channels);
                                        if let Some(report) = response {
                                            self.history.insert(id, report);
                                        }
                                        self.transaction_handles.push(handle);
                                        transaction_channels.insert(id, sender);
                                    }
                                    UserRequest::RemoteSuspend(suspend_req) => {
                                        let suspend_indication =
                                            match transaction_channels.get(&TransactionID(
                                                suspend_req.source_entity_id,
                                                suspend_req.transaction_sequence_number,
                                            )) {
                                                Some(chan) => chan.send(Command::Suspend).is_ok(),
                                                None => false,
                                            };

                                        let request = PutRequest {
                                            source_filename: "".into(),
                                            destination_filename: "".into(),
                                            destination_entity_id: origin_id.0,
                                            transmission_mode: tx_mode,
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0,
                                                            transaction_sequence_number: origin_id
                                                                .1,
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
                                            .unwrap_or(&self.default_config)
                                            .clone();

                                        let transport_tx = self
                                            .transport_tx_map
                                            .get(&request.destination_entity_id)
                                            .expect("No transport for Entity ID.")
                                            .clone();

                                        let (id, sender, handle) = Self::spawn_send_transaction(
                                            request,
                                            sequence_number,
                                            self.entity_id,
                                            transport_tx,
                                            self.primitive_tx.clone(),
                                            entity_config,
                                            self.filestore.clone(),
                                            false,
                                        )?;

                                        let response = Self::get_report(id, &transaction_channels);
                                        if let Some(report) = response {
                                            self.history.insert(id, report);
                                        }
                                        self.transaction_handles.push(handle);
                                        transaction_channels.insert(id, sender);
                                    }
                                    UserRequest::RemoteResume(resume_request) => {
                                        let suspend_indication =
                                            match transaction_channels.get(&TransactionID(
                                                resume_request.source_entity_id,
                                                resume_request.transaction_sequence_number,
                                            )) {
                                                Some(chan) => chan.send(Command::Resume).is_err(),
                                                None => true,
                                            };

                                        let request = PutRequest {
                                            source_filename: "".into(),
                                            destination_filename: "".into(),
                                            destination_entity_id: origin_id.0,
                                            transmission_mode: tx_mode,
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0,
                                                            transaction_sequence_number: origin_id
                                                                .1,
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
                                            .unwrap_or(&self.default_config)
                                            .clone();

                                        let transport_tx = self
                                            .transport_tx_map
                                            .get(&request.destination_entity_id)
                                            .expect("No transport for Entity ID.")
                                            .clone();

                                        let (id, sender, handle) = Self::spawn_send_transaction(
                                            request,
                                            sequence_number,
                                            self.entity_id,
                                            transport_tx,
                                            self.primitive_tx.clone(),
                                            entity_config,
                                            self.filestore.clone(),
                                            false,
                                        )?;

                                        let response = Self::get_report(id, &transaction_channels);
                                        if let Some(report) = response {
                                            self.history.insert(id, report);
                                        }
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
                        // Err(TryRecvError::Empty) => {
                        //     // was not actually ready, go back to selection
                        // }
                        // Err(TryRecvError::Disconnected) => {
                        //     // The transport instance disconnected?
                        //     // what do we do?
                        //     // remove from possible selections
                        //     selector.remove(oper.index());
                        // }
                        Err(err) => {
                            info!("Error on user msg {err}")
                        }
                    };
                }
                Ok(oper) if oper.index() == 1 => {
                    match oper.recv(&self.transport_rx) {
                        Ok(pdu) => {
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
                            let channel = transaction_channels
                                .entry(key)
                                // if this key is not in the channel list
                                // create a new transaction
                                .or_insert_with(|| {
                                    let entity_config = self
                                        .entity_configs
                                        .get(&key.0)
                                        .unwrap_or(&self.default_config)
                                        .clone();

                                    let (id, channel, handle) = Self::spawn_receive_transaction(
                                        &pdu.header,
                                        // TODO! Fill in this error
                                        self.transport_tx_map
                                            .get(&transport_entity)
                                            .expect("No transport for Entity ID.")
                                            .clone(),
                                        entity_config,
                                        self.filestore.clone(),
                                        self.message_tx.clone(),
                                    )
                                    .expect("Cannot spawn new Transaction.");

                                    // can't use the get_report function here due to double borrow
                                    let (tx, rx) = bounded(1);
                                    let response = channel
                                        .send(Command::Report(tx))
                                        .map(|_| rx.recv().ok())
                                        .ok()
                                        .flatten();

                                    if let Some(report) = response {
                                        self.history.insert(id, report);
                                    }
                                    self.transaction_handles.push(handle);
                                    channel
                                });

                            match channel.send(Command::Pdu(pdu.clone())) {
                                Ok(()) => {}
                                Err(_) => {
                                    // the transaction is completed.
                                    // spawn a new one
                                    // this is very unlikely and only results
                                    // if a sender is re-using a transaction id
                                    let entity_config = self
                                        .entity_configs
                                        .get(&key.0)
                                        .unwrap_or(&self.default_config)
                                        .clone();

                                    let (id, new_channel, handle) =
                                        Self::spawn_receive_transaction(
                                            &pdu.header,
                                            self.transport_tx_map
                                                .get(&transport_entity)
                                                .expect("No transport for Entity ID.")
                                                .clone(),
                                            entity_config,
                                            self.filestore.clone(),
                                            self.message_tx.clone(),
                                        )?;

                                    let response = Self::get_report(id, &transaction_channels);
                                    if let Some(report) = response {
                                        self.history.insert(id, report);
                                    }
                                    self.transaction_handles.push(handle);
                                    new_channel.send(Command::Pdu(pdu.clone()))?;
                                    // update the dict to have the new channel
                                    transaction_channels.insert(key, new_channel);
                                }
                            };
                        }
                        Err(_err) => {
                            // the channel is empty and disconnected
                            // this should only happen when we are cleaning up.
                        }
                    };
                }
                // received a UserPrimitive from the user implementation
                Ok(oper) if oper.index() == 2 => {
                    match oper.recv(&self.primitive_rx) {
                        Ok((primitive, internal_return)) => {
                            match primitive {
                                UserPrimitive::Put(request) => {
                                    let sequence_number = sequence_num.get_and_increment();

                                    let entity_config = self
                                        .entity_configs
                                        .get(&request.destination_entity_id)
                                        .unwrap_or(&self.default_config)
                                        .clone();

                                    let transport_tx = self
                                        .transport_tx_map
                                        .get(&request.destination_entity_id)
                                        .expect("No transport for Entity ID.")
                                        .clone();
                                    let (id, sender, handle) = Self::spawn_send_transaction(
                                        request,
                                        sequence_number,
                                        self.entity_id,
                                        transport_tx,
                                        self.primitive_tx.clone(),
                                        entity_config,
                                        self.filestore.clone(),
                                        false,
                                    )?;

                                    self.transaction_handles.push(handle);
                                    transaction_channels.insert(id, sender);

                                    let response = Self::get_report(id, &transaction_channels);
                                    if let Some(report) = response {
                                        self.history.insert(id, report.clone());
                                        // // Send back the initial report with the ID and state.
                                        // // but abandon if the user ends up busy
                                        // self.report_tx
                                        //     .send_timeout(report, Duration::from_millis(100))?;
                                    }

                                    // ignore the possible error if the user disconnected;
                                    let _ = internal_return.send(UserReturn::ID(id));
                                }
                                UserPrimitive::Cancel(id) => {
                                    if let Some(channel) = transaction_channels.get(&id) {
                                        channel.send(Command::Cancel)?;
                                    }
                                }
                                UserPrimitive::Suspend(id) => {
                                    if let Some(channel) = transaction_channels.get(&id) {
                                        channel.send(Command::Suspend)?;
                                    }
                                }
                                UserPrimitive::Resume(id) => {
                                    if let Some(channel) = transaction_channels.get(&id) {
                                        channel.send(Command::Resume)?;
                                    }
                                }
                                UserPrimitive::Report(id) => {
                                    let report = Self::get_report(id, &transaction_channels);
                                    let response = match report {
                                        Some(data) => {
                                            info!("Status of Transaction {}. State: {:?}. Status: {:?}. Condition: {:?}.", id, data.state, data.status, data.condition);
                                            self.history.insert(data.id, data.clone());
                                            Some(data)
                                        }
                                        None => match self.history.get(&id) {
                                            Some(data) => {
                                                info!("Status of Transaction {}. State: {:?}. Status: {:?}. Condition: {:?}.", id, data.state, data.status, data.condition);
                                                Some(data.clone())
                                            }
                                            None => {
                                                {
                                                    thread::sleep(Duration::from_millis(5));
                                                    // force a cleanup check then try again
                                                    let mut ind = 0;
                                                    while ind < self.transaction_handles.len() {
                                                        if self.transaction_handles[ind]
                                                            .is_finished()
                                                        {
                                                            let handle = self
                                                                .transaction_handles
                                                                .remove(ind);
                                                            match handle.join() {
                                                                Ok(Ok(inner_report)) => {
                                                                    // remove the channel for this transaction if it is complete
                                                                    let _ = transaction_channels
                                                                        .remove(&inner_report.id);
                                                                    // keep all proxy id maps where the finished transaction ID is not the entry
                                                                    self.proxy_id_map.retain(
                                                                        |_, value| {
                                                                            *value
                                                                                != inner_report.id
                                                                        },
                                                                    );
                                                                    self.history.insert(
                                                                        inner_report.id,
                                                                        inner_report,
                                                                    );
                                                                }
                                                                Ok(Err(err)) => {
                                                                    info!("Error occured during transaction: {err}");
                                                                }
                                                                Err(_err) => {
                                                                    error!(
                                                                        "Unable to join handle!"
                                                                    );
                                                                }
                                                            };
                                                        } else {
                                                            ind += 1;
                                                        }
                                                    }

                                                    cleanup = Instant::now();
                                                }
                                                match self.history.get(&id) {
                                                    Some(data) => {
                                                        info!("Status of Transaction {}. State: {:?}. Status: {:?}. Condition: {:?}.", id, data.state, data.status, data.condition);
                                                        Some(data.clone())
                                                    }
                                                    None => {
                                                        info!("Cannot find information on requested transaction.");
                                                        None
                                                    }
                                                }
                                            }
                                        },
                                    };
                                    // ignore the possible error if the user disconnected;
                                    let _ = internal_return.send(UserReturn::Report(response));
                                }
                            };
                        }
                        Err(_err) => {
                            // The channel is disconnected
                            error!("User interface disconnected from daemon.");
                        }
                    }
                }
                Err(_) => {
                    // timeout occurred
                }
                Ok(_) => unreachable!(),
            };

            // join any handles that have completed
            // maybe should only run every so often?
            if cleanup.elapsed() >= Duration::from_secs(1) {
                let mut ind = 0;
                while ind < self.transaction_handles.len() {
                    if self.transaction_handles[ind].is_finished() {
                        let handle = self.transaction_handles.remove(ind);
                        match handle.join() {
                            Ok(Ok(report)) => {
                                // remove the channel for this transaction if it is complete
                                let _ = transaction_channels.remove(&report.id);
                                // keep all proxy id maps where the finished transaction ID is not the entry
                                self.proxy_id_map.retain(|_, value| *value != report.id);
                                self.history.insert(report.id, report);
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

        // a final cleanup
        while let Some(handle) = self.transaction_handles.pop() {
            match handle.join() {
                Ok(Ok(report)) => {
                    // remove the channel for this transaction if it is complete
                    let _ = transaction_channels.remove(&report.id);
                    // keep all proxy id maps where the finished transaction ID is not the entry
                    self.proxy_id_map.retain(|_, value| *value != report.id);
                    self.history.insert(report.id, report);
                }
                Ok(Err(err)) => {
                    info!("Error occured during transaction: {}", err)
                }
                Err(_) => error!("Unable to join handle!"),
            };
        }
        Ok(())
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
                first_filename: "some_name_here.txt".into(),
                second_filename: "another_name.txt".into(),
            },
            FileStoreRequest{
                action_code: FileStoreAction::RenameFile,
                first_filename: "some_name_here.txt".into(),
                second_filename: "another_name.txt".into(),
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
                            first_filename:"/tmp/help".into(),
                            second_filename:"".into()}],
                    message_to_user: vec![MessageToUser{message_text: "do something".as_bytes().to_vec()}],
                }
            ),
            UserPrimitive::Cancel(TransactionID::from(1_u8, 3_u8)),
            UserPrimitive::Suspend(TransactionID::from(10_u16, 400_u16)),
            UserPrimitive::Resume(TransactionID::from(871838474_u64, 871838447_u64)),
            UserPrimitive::Report(TransactionID::from(12_u16, 33_u16))
        )]
        expected: UserPrimitive,
    ) {
        let buffer = expected.clone().encode();
        let recovered = UserPrimitive::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn proxy_req(#[values(true, false)] use_mode: bool) {
        let origin_id = TransactionID::from(55_u16, 12_u16);
        let mut messages = vec![
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::CreateDirectory,
                first_filename: "/tmp".into(),
                second_filename: "".into(),
            }),
            ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                destination_entity_id: EntityID::from(3_u16),
                source_filename: "test_file".into(),
                destination_filename: "out_file".into(),
            }),
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: "first_file".into(),
                second_filename: "second_file".into(),
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
                    first_filename: "/tmp".into(),
                    second_filename: "".into(),
                },
                FileStoreRequest {
                    action_code: FileStoreAction::AppendFile,
                    first_filename: "first_file".into(),
                    second_filename: "second_file".into(),
                },
            ],
            message_to_user: vec![
                MessageToUser {
                    message_text: "help".as_bytes().to_vec(),
                },
                MessageToUser {
                    message_text: UserOperation::OriginatingTransactionIDMessage(
                        OriginatingTransactionIDMessage {
                            source_entity_id: origin_id.0,
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
        let origin_id = TransactionID::from(55_u16, 12_u16);
        let proxy_ops = vec![
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::CreateDirectory,
                first_filename: "/tmp".into(),
                second_filename: "".into(),
            }),
            ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                destination_entity_id: EntityID::from(3_u16),
                source_filename: "test_file".into(),
                destination_filename: "out_file".into(),
            }),
            ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: "first_file".into(),
                second_filename: "second_file".into(),
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
                    first_filename: "/tmp".into(),
                    second_filename: "".into(),
                },
                FileStoreRequest {
                    action_code: FileStoreAction::AppendFile,
                    first_filename: "first_file".into(),
                    second_filename: "second_file".into(),
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

        let cancel_id = TransactionID::from(16_u16, 3_u32);

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
