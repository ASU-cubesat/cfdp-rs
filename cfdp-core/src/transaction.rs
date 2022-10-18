use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    error::Error,
    fmt::{self, Display},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    sync::{Arc, PoisonError},
};

use camino::Utf8PathBuf;
use crossbeam_channel::{SendError, Sender};
use interprocess::local_socket::LocalSocketStream;
use log::info;
use num_derive::FromPrimitive;

use crate::{
    daemon::{PutRequest, Report, SOCKET_ADDR},
    filestore::{ChecksumType, FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, CRCFlag, Condition, DeliveryCode, Direction, EndOfFile, EntityID,
        FaultHandlerAction, FileDataPDU, FileSizeFlag, FileSizeSensitive, FileStatusCode,
        FileStoreRequest, FileStoreResponse, Finished, KeepAlivePDU, MessageToUser, MetadataPDU,
        MetadataTLV, NakOrKeepAlive, NegativeAcknowldegmentPDU, Operations, PDUDirective,
        PDUEncode, PDUHeader, PDUPayload, PDUType, PositiveAcknowledgePDU, ProxyPutResponse,
        SegmentRequestForm, SegmentationControl, SegmentedData, TransactionSeqNum,
        TransactionStatus, TransmissionMode, UnsegmentedFileData, UserOperation, UserResponse,
        VariableID, PDU, U3,
    },
    timer::Timer,
};

pub type TransactionID = (EntityID, TransactionSeqNum);
pub type TransactionResult<T> = Result<T, TransactionError>;
#[derive(Debug)]
pub enum TransactionError {
    FileStore(FileStoreError),
    Transport(SendError<(VariableID, PDU)>),
    UserMessage(SendError<(TransactionID, TransmissionMode, Vec<MessageToUser>)>),
    NoFile(TransactionID),
    Daemon(String),
    Poison,
    IntConverstion(TryFromIntError),
    UnexpectedPDU((TransactionSeqNum, Action, TransmissionMode, String)),
    MissingMetadata(TransactionID),
    MissingNak,
    NoChecksum,
    Report(SendError<Report>),
    InvalidStatus(u8),
}
impl From<FileStoreError> for TransactionError {
    fn from(error: FileStoreError) -> Self {
        Self::FileStore(error)
    }
}
impl From<TryFromIntError> for TransactionError {
    fn from(error: TryFromIntError) -> Self {
        Self::IntConverstion(error)
    }
}
impl From<SendError<(VariableID, PDU)>> for TransactionError {
    fn from(error: SendError<(VariableID, PDU)>) -> Self {
        Self::Transport(error)
    }
}
impl From<SendError<(TransactionID, TransmissionMode, Vec<MessageToUser>)>> for TransactionError {
    fn from(error: SendError<(TransactionID, TransmissionMode, Vec<MessageToUser>)>) -> Self {
        Self::UserMessage(error)
    }
}
impl From<SendError<Report>> for TransactionError {
    fn from(error: SendError<Report>) -> Self {
        Self::Report(error)
    }
}
impl<T> From<PoisonError<T>> for TransactionError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileStore(error) => error.fmt(f),
            Self::Transport(error) => error.fmt(f),
            Self::UserMessage(error) => error.fmt(f),
            Self::NoFile(id) => write!(f, "No open file in transaction: {id:?}."),
            Self::Daemon(error) => write!(f, "Error during daemon thread listening. {}", error),
            Self::Poison => write!(
                f,
                "Error during File manipulation. Unable to obtain File lock."
            ),
            Self::IntConverstion(error) => error.fmt(f),
            Self::UnexpectedPDU((id, transaction_type, mode, pdu_type)) => write!(
                f,
                "Transaction (ID: {:?},  Type: {:?}, Mode: {:?}) received unexpected PDU {:}.",
                id, transaction_type, mode, pdu_type
            ),
            Self::MissingMetadata(id) => write!(f, "Metadata missing for transaction: {id:?}."),
            Self::MissingNak => write!(f, "No NAKs present. Cannot send missing data."),
            Self::NoChecksum => write!(f, "No Checksum received. Cannot verify file integrity."),
            Self::Report(report) => {
                write!(f, "Unable to send report to Daemon process. {:?}.", report)
            }
            Self::InvalidStatus(val) => write!(f, "No Transaction status for code {}.", val),
        }
    }
}
impl Error for TransactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FileStore(source) => Some(source),
            Self::UserMessage(source) => Some(source),
            Self::Transport(source) => Some(source),
            Self::NoFile(_) => None,
            Self::Daemon(_) => None,
            Self::Poison => None,
            Self::IntConverstion(source) => Some(source),
            Self::UnexpectedPDU(_) => None,
            Self::MissingMetadata(_) => None,
            Self::MissingNak => None,
            Self::NoChecksum => None,
            Self::Report(_) => None,
            Self::InvalidStatus(_) => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
enum WaitingOn {
    None,
    AckEof,
    AckFin,
    Nak,
    MissingData,
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, FromPrimitive)]
pub enum TransactionState {
    Active,
    Suspended,
    Terminated,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
/// The Action type of a [Transaction] entity.
pub enum Action {
    /// Sending a file to another Entity.
    Send,
    /// Receiving a file from another Entity.
    Receive,
}

#[cfg_attr(test, derive(Debug, Clone, PartialEq))]
pub(crate) struct Metadata {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// The size of the file being transfered in this transaction.
    pub file_size: FileSizeSensitive,
    /// List of any filestore requests to take after transaction is complete
    pub filestore_requests: Vec<FileStoreRequest>,
    /// Any Messages to user received either from the metadataPDU or as input
    pub message_to_user: Vec<MessageToUser>,
    /// Flag to track whether Transaciton Closure will be requested.
    pub closure_requested: bool,
    /// Flag to track what kind of [Checksum](crate::filestore::ChecksumType) will be used in this transaction.
    pub checksum_type: ChecksumType,
}

#[cfg_attr(test, derive(Clone))]
pub struct TransactionConfig {
    /// Flag to determine if this transaction is a Send or Receive. See [Action]
    pub action_type: Action,
    /// Identification number of the source (this) entity. See [EntityID]
    pub source_entity_id: EntityID,
    /// Identification number of the destination (remote) entity. See [EntityID]
    pub destination_entity_id: EntityID,
    /// CFDP [TransmissionMode]
    pub transmission_mode: TransmissionMode,
    /// The sequence number in Big Endian Bytes.
    pub sequence_number: TransactionSeqNum,
    /// Flag to indicate whether or not the file size fits inside a [u32]
    pub file_size_flag: FileSizeFlag,
    /// A Mapping of actions to take when each condition is reached.
    /// See also [Condition] and [FaultHandlerAction].
    pub fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// The maximum length a file segment sent to Destination can be.
    /// u16 will be larger than any possible CCSDS packet size.
    pub file_size_segment: u16,
    /// Flag indicating whether or not CRCs will be present on the PDUs
    pub crc_flag: CRCFlag,
    /// Flag indicating whether file metadata is included with FileData
    pub segment_metadata_flag: SegmentedData,
    /// Maximum count of timeouts on a [Timer] before a fault is generated.
    pub max_count: u32,
    /// Maximum amount timeof without activity before the inactivity [Timer] increments its count.
    pub inactivity_timeout: i64,
    /// Maximum amount timeof without activity before the NAK [Timer] increments its count.
    pub nak_timeout: i64,
    /// Maximum amount timeof without activity before the ACK [Timer] increments its count.
    pub ack_timeout: i64,
    // used when a proxy put request is received to originate a transaction
    pub send_proxy_response: bool,
}

pub struct Transaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<T>,
    /// Channel used to send outgoing PDUs through the Transport layer.
    transport_tx: Sender<(VariableID, PDU)>,
    /// Channel for message to users to propagate back up
    message_tx: Sender<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// A mapping of offsets and segments lengths to monitor progress.
    /// For a Receiver, these are the received segments.
    /// For a Sender, these are the sent segments.
    saved_segments: BTreeMap<FileSizeSensitive, FileSizeSensitive>,
    /// The list of all missing information
    naks: VecDeque<SegmentRequestForm>,
    /// Flag to check if metadata on the file has been received
    pub(crate) metadata: Option<Metadata>,
    /// Measurement of how large of a file has been received so far
    received_file_size: FileSizeSensitive,
    /// a cache of the header used for interactions in this transmission
    header: Option<PDUHeader>,
    /// The current condition of the transaction
    condition: Condition,
    // Track whether the transaction is complete or not
    delivery_code: DeliveryCode,
    // Status of the current File
    file_status: FileStatusCode,
    // the responses of any filestore actions
    filestore_response: Vec<FileStoreResponse>,
    // Timer used to track if the Nak limit has been reached
    // inactivity has occured
    // or the ACK limit is reached
    timer: Timer,
    // checksum cache to reduce I/0
    // doubles as stored checksum in received mode
    checksum: Option<u32>,
    // tracker to know what to do when a timeout occurs
    waiting_on: WaitingOn,
    // The current state of the transaction.
    // Used to determine when the thread should be killed
    state: TransactionState,
    // a tracker for sending the first EoF
    do_once: bool,
}
impl<T: FileStore> Transaction<T> {
    /// Start a new Transaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore)
    pub fn new(
        // Configuation of this Transaction.
        config: TransactionConfig,
        // Connection to the local FileStore implementation.
        filestore: Arc<T>,
        // Sender channel connected to the Transport thread to send PDUs.
        transport_tx: Sender<(VariableID, PDU)>,
        // Sender channel used to propagate Message To User back up to the Daemon Thread.
        message_tx: Sender<(TransactionID, TransmissionMode, Vec<MessageToUser>)>,
    ) -> Self {
        let received_file_size = match &config.file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(0),
            FileSizeFlag::Large => FileSizeSensitive::Large(0),
        };
        let timer = Timer::new(
            config.inactivity_timeout,
            config.max_count,
            config.ack_timeout,
            config.max_count,
            config.nak_timeout,
            config.max_count,
        );

        let mut transaction = Self {
            status: TransactionStatus::Undefined,
            config,
            filestore,
            transport_tx,
            message_tx,
            file_handle: None,
            saved_segments: BTreeMap::new(),
            naks: VecDeque::new(),
            metadata: None,
            received_file_size,
            header: None,
            condition: Condition::NoError,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
            filestore_response: Vec::new(),
            timer,
            checksum: None,
            waiting_on: WaitingOn::None,
            state: TransactionState::Active,
            do_once: true,
        };
        transaction.timer.restart_inactivity();
        transaction
    }

    pub fn get_status(&self) -> &TransactionStatus {
        &self.status
    }

    pub(crate) fn get_state(&self) -> &TransactionState {
        &self.state
    }

    pub fn get_mode(&self) -> TransmissionMode {
        self.config.transmission_mode.clone()
    }
    pub fn generate_report(&self) -> Report {
        Report {
            id: self.id(),
            state: self.get_state().clone(),
            status: self.get_status().clone(),
            condition: self.condition.clone(),
        }
    }
    fn get_header(
        &mut self,
        direction: Direction,
        pdu_type: PDUType,
        pdu_data_field_length: u16,
        segmentation_control: SegmentationControl,
    ) -> PDUHeader {
        if let Some(header) = &self.header {
            // update the necessary fields but copy the rest
            // from the cache
            PDUHeader {
                pdu_type,
                pdu_data_field_length,
                segmentation_control,
                ..header.clone()
            }
        } else {
            let header = PDUHeader {
                version: U3::One,
                pdu_type,
                direction,
                transmission_mode: self.config.transmission_mode.clone(),
                crc_flag: self.config.crc_flag.clone(),
                large_file_flag: self.config.file_size_flag,
                pdu_data_field_length,
                segmentation_control,
                segment_metadata_flag: self.config.segment_metadata_flag.clone(),
                source_entity_id: self.config.source_entity_id.clone(),
                transaction_sequence_number: self.config.sequence_number.clone(),
                destination_entity_id: self.config.destination_entity_id.clone(),
            };
            self.header = Some(header.clone());
            header
        }
    }

    pub fn id(&self) -> (VariableID, TransactionSeqNum) {
        (
            self.config.source_entity_id.clone(),
            self.config.sequence_number.clone(),
        )
    }

    fn get_checksum(&mut self) -> TransactionResult<u32> {
        match self.checksum {
            Some(val) => Ok(val),
            None => {
                let checksum_type = self
                    .metadata
                    .as_ref()
                    .map(|meta| meta.checksum_type.clone())
                    .ok_or_else(|| {
                        let id = self.id();
                        TransactionError::MissingMetadata(id)
                    })?;
                let checksum = self.get_handle()?.checksum(checksum_type)?;
                self.checksum = Some(checksum);
                Ok(checksum)
            }
        }
    }

    fn initialize_tempfile(&mut self) -> TransactionResult<()> {
        self.file_handle = Some(self.filestore.open_tempfile()?);
        Ok(())
    }

    fn open_source_file(&mut self) -> TransactionResult<()> {
        self.file_handle = {
            let id = self.id();

            let fname = &self
                .metadata
                .as_ref()
                .ok_or(TransactionError::MissingMetadata(id))?
                .source_filename;

            Some(self.filestore.open(fname, OpenOptions::new().read(true))?)
        };
        Ok(())
    }

    fn get_handle(&mut self) -> TransactionResult<&mut File> {
        let id = self.id();
        if self.file_handle.is_none() {
            match self.config.action_type {
                Action::Receive => self.initialize_tempfile()?,
                Action::Send => self.open_source_file()?,
            }
        };
        self.file_handle
            .as_mut()
            .ok_or(TransactionError::NoFile(id))
    }

    fn is_file_transfer(&self) -> bool {
        self.metadata
            .as_ref()
            .map(|meta| !meta.source_filename.as_os_str().is_empty())
            .unwrap_or(false)
    }

    fn store_file_data(
        &mut self,
        pdu: FileDataPDU,
    ) -> TransactionResult<(FileSizeSensitive, usize)> {
        let (offset, file_data) = match pdu {
            FileDataPDU::Segmented(data) => (data.offset, data.file_data),
            FileDataPDU::Unsegmented(data) => (data.offset, data.file_data),
        };
        let length = file_data.len();
        {
            let handle = self.get_handle()?;
            handle
                .seek(SeekFrom::Start(offset.as_u64()))
                .map_err(FileStoreError::IO)?;
            handle
                .write_all(file_data.as_slice())
                .map_err(FileStoreError::IO)?;
            match &self.config.file_size_flag {
                FileSizeFlag::Small => self.saved_segments.insert(
                    offset.clone(),
                    FileSizeSensitive::Small(file_data.len() as u32),
                ),
                FileSizeFlag::Large => self.saved_segments.insert(
                    offset.clone(),
                    FileSizeSensitive::Large(file_data.len() as u64),
                ),
            };
        }

        Ok((offset, length))
    }

    /// Read a segment of size `length` from position `offset` in the file.
    /// When offset is [None], reads from the current cursor location.
    /// when length is [None] uses the maximum length for the receiving Engine.
    /// This size is set by the `file_size_segment` field in the [TransactionConfig].
    fn get_file_segment(
        &mut self,
        offset: Option<u64>,
        length: Option<u16>,
    ) -> TransactionResult<(u64, Vec<u8>)> {
        // use the maximum size for the receiever if no length is given
        let length = length.unwrap_or(self.config.file_size_segment);
        let handle = self.get_handle()?;
        // if no offset given read from current cursor position
        let offset = offset.unwrap_or(handle.stream_position().map_err(FileStoreError::IO)?);

        // If the offset is not provided start at the current position

        handle
            .seek(SeekFrom::Start(offset))
            .map_err(FileStoreError::IO)?;

        // use take to limit the final segment from trying to read past the EoF.
        let data = {
            let mut buff = Vec::<u8>::new();
            handle
                .take(length as u64)
                .read_to_end(&mut buff)
                .map_err(FileStoreError::IO)?;
            buff
        };

        Ok((offset, data))
    }

    /// Send a segment of size `length` from position `offset` in the file.
    /// When offset is [None], reads from the current cursor location.
    /// when length is [None] uses the maximum length for the receiving Engine.
    /// This size is set by the `file_size_segment` field in the [TransactionConfig].
    pub fn send_file_segment(
        &mut self,
        offset: Option<u64>,
        length: Option<u16>,
    ) -> TransactionResult<()> {
        self.timer.restart_inactivity();
        let (offset, file_data) = self.get_file_segment(offset, length)?;

        let offset = match &self.config.file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(offset.try_into()?),
            FileSizeFlag::Large => FileSizeSensitive::Large(offset),
        };
        let (data, segmentation_control) = match self.config.segment_metadata_flag {
            SegmentedData::NotPresent => (
                FileDataPDU::Unsegmented(UnsegmentedFileData { offset, file_data }),
                SegmentationControl::NotPreserved,
            ),
            // TODO need definition of segment metadata. Not currently supported.
            SegmentedData::Present => unimplemented!(),
        };
        let destination = self.config.destination_entity_id.clone();
        let payload = PDUPayload::FileData(data);
        let payload_len: u16 = payload.clone().encode().len().try_into()?;
        let pdu = PDU {
            header: self.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                segmentation_control,
            ),
            payload,
        };

        self.transport_tx.send((destination, pdu))?;

        Ok(())
    }

    pub fn all_data_sent(&mut self) -> TransactionResult<bool> {
        match self.is_file_transfer() {
            true => match self.condition == Condition::NoError {
                true => {
                    let handle = self.get_handle()?;
                    let eof = handle.stream_position().map_err(FileStoreError::IO)?
                        == handle.metadata().map_err(FileStoreError::IO)?.len();
                    if eof && self.do_once {
                        self.send_eof(None)?;
                        self.do_once = false;
                    }
                    Ok(eof)
                }
                // if we have been canceled or another error happened just say
                // end of file reached
                false => Ok(true),
            },
            false => Ok(true),
        }
    }

    pub fn send_missing_data(&mut self) -> TransactionResult<()> {
        match self.naks.pop_front() {
            Some(request) => {
                // only restart inactivity if we have something to do.
                self.timer.restart_inactivity();
                let (offset, length) = match (request.start_offset, request.end_offset) {
                    (FileSizeSensitive::Small(s), FileSizeSensitive::Small(e)) => {
                        (s.into(), (e - s).try_into()?)
                    }
                    (FileSizeSensitive::Large(s), FileSizeSensitive::Large(e)) => {
                        (s, (e - s).try_into()?)
                    }
                    _ => unreachable!(),
                };
                match offset == 0 && length == 0 {
                    true => self.send_metadata(),
                    false => {
                        let current_pos = {
                            let handle = self.get_handle()?;
                            handle.stream_position().map_err(FileStoreError::IO)?
                        };

                        self.send_file_segment(Some(offset), Some(length))?;

                        // restore to original location in the file
                        let handle = self.get_handle()?;
                        handle
                            .seek(SeekFrom::Start(current_pos))
                            .map_err(FileStoreError::IO)?;
                        Ok(())
                    }
                }
            }
            None => Ok(()),
        }
    }

    fn update_naks(&mut self, file_size: Option<FileSizeSensitive>) {
        let mut naks: VecDeque<SegmentRequestForm> = VecDeque::new();
        let mut pointer: FileSizeSensitive = match &self.config.file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(0_u32),
            FileSizeFlag::Large => FileSizeSensitive::Large(0_u64),
        };

        if self.metadata.is_none() {
            naks.push_back(match &self.config.file_size_flag {
                FileSizeFlag::Small => SegmentRequestForm::from((0_u32, 0_u32)),
                FileSizeFlag::Large => SegmentRequestForm::from((0_u64, 0_u64)),
            });
        }
        self.saved_segments.iter().for_each(|(offset, length)| {
            if offset > &pointer {
                naks.push_back(SegmentRequestForm {
                    start_offset: pointer.clone(),
                    end_offset: offset.clone(),
                });
            }
            pointer = match (offset, length) {
                (FileSizeSensitive::Small(left), FileSizeSensitive::Small(right)) => {
                    FileSizeSensitive::Small(left + right)
                }
                (FileSizeSensitive::Large(left), FileSizeSensitive::Large(right)) => {
                    FileSizeSensitive::Large(left + right)
                }
                // We have forced everything to be the same type, we just needed to get at their inners.
                _ => unreachable!(),
            };
        });

        if let Some(size) = file_size {
            if pointer < size {
                naks.push_back(SegmentRequestForm {
                    start_offset: pointer,
                    end_offset: size,
                });
            }
        }

        self.naks = naks
    }

    fn finalize_file(&mut self) -> TransactionResult<FileStatusCode> {
        let id = self.id();
        {
            let mut outfile = self.filestore.open(
                self.metadata
                    .as_ref()
                    .map(|meta| meta.destination_filename.clone())
                    .ok_or(TransactionError::NoFile(id))?,
                File::options().create(true).write(true).truncate(true),
            )?;
            let handle = self.get_handle()?;
            // rewind to the beginning of the file.
            // this might not be necessary with the io call that follows
            handle.rewind().map_err(FileStoreError::IO)?;
            io::copy(handle, &mut outfile).map_err(FileStoreError::IO)?;
            outfile.sync_all().map_err(FileStoreError::IO)?;
        }
        // Drop the temporary file
        self.file_handle = None;

        Ok(FileStatusCode::Retained)
    }

    pub fn send_eof(&mut self, fault_location: Option<VariableID>) -> TransactionResult<()> {
        self.timer.restart_ack();
        self.waiting_on = WaitingOn::AckEof;
        //  if is lazily evaluated so the fault is only handled if the limit is reached
        if self.timer.ack.limit_reached()
            && !self.proceed_despite_fault(Condition::PositiveLimitReached)?
        {
            return Ok(());
        }
        let eof = EndOfFile {
            condition: self.condition.clone(),
            checksum: self.get_checksum()?,
            file_size: self
                .metadata
                .as_ref()
                .ok_or_else(|| {
                    let id = self.id();
                    TransactionError::MissingMetadata(id)
                })?
                .file_size
                .clone(),
            fault_location,
        };

        let payload = PDUPayload::Directive(Operations::EoF(eof));

        let payload_len = payload.clone().encode().len() as u16;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.destination_entity_id.clone();
        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;
        Ok(())
    }

    pub fn abandon(&mut self) {
        self.status = TransactionStatus::Terminated;
        self.shutdown();
    }

    pub fn shutdown(&mut self) {
        self.state = TransactionState::Terminated;
        self.timer.ack.pause();
        self.timer.nak.pause();
        self.timer.inactivity.pause();
    }

    pub fn cancel(&mut self) -> TransactionResult<()> {
        self.condition = Condition::CancelReceived;
        self._cancel()
    }

    fn _cancel(&mut self) -> TransactionResult<()> {
        match (&self.config.action_type, &self.config.transmission_mode) {
            (Action::Send, TransmissionMode::Acknowledged) => {
                self.send_eof(Some(self.config.source_entity_id.clone()))
            }
            (Action::Send, TransmissionMode::Unacknowledged) => {
                self.send_eof(Some(self.config.source_entity_id.clone()))?;
                self.shutdown();
                Ok(())
            }
            (Action::Receive, TransmissionMode::Acknowledged) => self.send_finished(None),
            (Action::Receive, TransmissionMode::Unacknowledged) => {
                if self
                    .metadata
                    .as_ref()
                    .map(|meta| meta.closure_requested)
                    .unwrap_or(false)
                {
                    // need to check on fault_location
                    self.send_finished(None)?;
                }
                self.shutdown();
                Ok(())
            }
        }
        // make some kind of log/indication
    }

    pub fn suspend(&mut self) {
        self.timer.ack.pause();
        self.timer.nak.pause();
        self.timer.inactivity.pause();
        self.state = TransactionState::Suspended;
    }

    pub fn resume(&mut self) {
        self.timer.restart_inactivity();
        match self.waiting_on {
            WaitingOn::AckEof | WaitingOn::AckFin => self.timer.restart_ack(),
            WaitingOn::Nak => self.timer.restart_nak(),
            _ => {}
        }
        self.state = TransactionState::Active;

        // what to do about the other timers?
        // We need to track if one is needed?
    }

    /// Take action according to the defined handler mapping.
    /// Returns a boolean indicating if the calling function should continue (true) or not (false.)
    fn proceed_despite_fault(&mut self, condition: Condition) -> TransactionResult<bool> {
        self.condition = condition;
        match self
            .config
            .fault_handler_override
            .get(&self.condition)
            .unwrap_or(&FaultHandlerAction::Cancel)
        {
            FaultHandlerAction::Ignore => {
                // Log ignoring error
                Ok(true)
            }
            FaultHandlerAction::Cancel => {
                self._cancel()?;
                Ok(false)
            }

            FaultHandlerAction::Suspend => {
                self.suspend();
                Ok(false)
            }
            FaultHandlerAction::Abandon => {
                self.abandon();
                Ok(false)
            }
        }
    }

    fn send_ack_finished(&mut self) -> TransactionResult<()> {
        let ack = PositiveAcknowledgePDU {
            directive: PDUDirective::Finished,
            directive_subtype_code: ACKSubDirective::Finished,
            condition: self.condition.clone(),
            transaction_status: self.status.clone(),
        };
        let payload = PDUPayload::Directive(Operations::Ack(ack));
        let payload_len = payload.clone().encode().len() as u16;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.destination_entity_id.clone();
        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;

        Ok(())
    }

    fn send_ack_eof(&mut self) -> TransactionResult<()> {
        let ack = PositiveAcknowledgePDU {
            directive: PDUDirective::EoF,
            directive_subtype_code: ACKSubDirective::Other,
            condition: self.condition.clone(),
            transaction_status: self.status.clone(),
        };
        let payload = PDUPayload::Directive(Operations::Ack(ack));
        let payload_len = payload.clone().encode().len();

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            // TODO add semgentation Control ability
            SegmentationControl::NotPreserved,
        );

        let destination = header.source_entity_id.clone();
        let pdu = PDU { header, payload };
        self.transport_tx.send((destination, pdu))?;

        Ok(())
    }

    fn check_file_size(&mut self, file_size: FileSizeSensitive) -> TransactionResult<()> {
        if self.received_file_size > file_size {
            // we will always exit here anyway
            self.proceed_despite_fault(Condition::FilesizeError)?;
        }
        Ok(())
    }

    fn get_progress(&self) -> FileSizeSensitive {
        // the Enum types are guaranteed to match the FileSize flag, we just need to unwrap them.
        match &self.config.file_size_flag {
            FileSizeFlag::Small => self.saved_segments.iter().fold(
                FileSizeSensitive::Small(0),
                |size, (_offset, length)| match (size, length) {
                    (FileSizeSensitive::Small(s), FileSizeSensitive::Small(l)) => {
                        FileSizeSensitive::Small(s + l)
                    }
                    (_, _) => unreachable!(),
                },
            ),
            FileSizeFlag::Large => self.saved_segments.iter().fold(
                FileSizeSensitive::Large(0),
                |size, (_offset, length)| match (size, length) {
                    (FileSizeSensitive::Large(s), FileSizeSensitive::Large(l)) => {
                        FileSizeSensitive::Large(s + l)
                    }
                    (_, _) => unreachable!(),
                },
            ),
        }
    }

    fn send_finished(&mut self, fault_location: Option<VariableID>) -> TransactionResult<()> {
        self.timer.restart_ack();
        self.waiting_on = WaitingOn::AckFin;
        //  if is lazily evaluated so the fault is only handled if the limit is reached
        if self.timer.ack.limit_reached()
            && !self.proceed_despite_fault(Condition::PositiveLimitReached)?
        {
            return Ok(());
        }
        let finished = Finished {
            condition: self.condition.clone(),
            delivery_code: self.delivery_code.clone(),
            file_status: self.file_status.clone(),
            filestore_response: self.filestore_response.clone(),
            fault_location,
        };

        let payload = PDUPayload::Directive(Operations::Finished(finished));

        let payload_len = payload.clone().encode().len() as u16;

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.source_entity_id.clone();
        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;
        Ok(())
    }

    pub fn send_naks(&mut self) -> TransactionResult<()> {
        self.timer.restart_nak();
        self.waiting_on = WaitingOn::Nak;
        //  if is lazily evaluated so the fault is only handled if the limit is reached
        if self.timer.nak.limit_reached()
            && !self.proceed_despite_fault(Condition::NakLimitReached)?
        {
            return Ok(());
        }

        let nak = NegativeAcknowldegmentPDU {
            start_of_scope: match &self.config.file_size_flag {
                FileSizeFlag::Small => FileSizeSensitive::Small(0),
                FileSizeFlag::Large => FileSizeSensitive::Large(0),
            },
            end_of_scope: self.received_file_size.clone(),
            segment_requests: self.naks.clone().into(),
        };
        let payload = PDUPayload::Directive(Operations::Nak(nak));
        let payload_len = payload.clone().encode().len();

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let destination = header.source_entity_id.clone();
        let pdu = PDU { header, payload };
        self.transport_tx.send((destination, pdu))?;

        Ok(())
    }

    fn verify_checksum(&mut self, checksum: u32) -> TransactionResult<bool> {
        let checksum_type = self
            .metadata
            .as_ref()
            .map(|meta| meta.checksum_type.clone())
            .ok_or_else(|| {
                let id = self.id();
                TransactionError::MissingMetadata(id)
            })?;
        let handle = self.get_handle()?;
        Ok(handle.checksum(checksum_type)? == checksum)
    }

    fn finalize_receive(&mut self) -> TransactionResult<()> {
        let checksum = self.checksum.ok_or(TransactionError::NoChecksum)?;
        self.delivery_code = DeliveryCode::Complete;

        self.file_status = if self.is_file_transfer() {
            // proceed_despite_fault returns false if we should exit immediately
            if !self.verify_checksum(checksum)?
                && !self.proceed_despite_fault(Condition::FileChecksumFailure)?
            {
                return Ok(());
            }
            self.finalize_file()
                .unwrap_or(FileStatusCode::FileStoreRejection)
        } else {
            FileStatusCode::Unreported
        };
        // A filestore rejection is a failure mode for the entire transaction.
        if self.file_status == FileStatusCode::FileStoreRejection
            && !self.proceed_despite_fault(Condition::FileStoreRejection)?
        {
            return Ok(());
        }

        // Only perform the Filestore requests if the Copy was successful
        self.filestore_response = {
            let mut fail_rest = false;
            let mut out = vec![];
            if let Some(meta) = self.metadata.as_ref() {
                for request in &meta.filestore_requests {
                    let response = match fail_rest {
                        false => {
                            let rep = self.filestore.process_request(request);
                            fail_rest = rep.action_and_status.is_fail();
                            rep
                        }
                        true => FileStoreResponse::not_performed(request),
                    };

                    out.push(response);
                }
            }
            out
        };
        Ok(())
    }

    pub fn get_proxy_response(&mut self) -> ProxyPutResponse {
        ProxyPutResponse {
            condition: self.condition.clone(),
            delivery_code: self.delivery_code.clone(),
            file_status: self.file_status.clone(),
        }
    }

    pub fn monitor_timeout(&mut self) -> TransactionResult<()> {
        if self.timer.inactivity.timeout_occured()
            && self.timer.inactivity.limit_reached()
            && !self.proceed_despite_fault(Condition::InactivityDetected)?
        {
            return Ok(());
        }
        if self.timer.nak.timeout_occured() && self.waiting_on == WaitingOn::Nak {
            return self.send_naks();
        }
        if self.timer.ack.timeout_occured() {
            match self.waiting_on {
                WaitingOn::AckEof => return self.send_eof(None),
                WaitingOn::AckFin => return self.send_finished(None),
                _ => {}
            }
        }
        Ok(())
    }

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        self.timer.restart_inactivity();
        let PDU {
            header: _header,
            payload,
        } = pdu;
        match (&self.config.action_type, &self.config.transmission_mode) {
            (Action::Send, TransmissionMode::Acknowledged) => {
                match payload {
                    PDUPayload::FileData(_data) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "File Data".to_owned(),
                    ))),
                    PDUPayload::Directive(operation) => match operation {
                        Operations::EoF(_eof) => Err(TransactionError::UnexpectedPDU((
                            self.config.sequence_number.clone(),
                            self.config.action_type,
                            self.config.transmission_mode.clone(),
                            "End of File".to_owned(),
                        ))),
                        Operations::Metadata(_metadata) => Err(TransactionError::UnexpectedPDU((
                            self.config.sequence_number.clone(),
                            self.config.action_type,
                            self.config.transmission_mode.clone(),
                            "Metadata PDU".to_owned(),
                        ))),
                        Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU((
                            self.config.sequence_number.clone(),
                            self.config.action_type,
                            self.config.transmission_mode.clone(),
                            "Prompt PDU".to_owned(),
                        ))),
                        Operations::Finished(finished) => {
                            self.delivery_code = finished.delivery_code;
                            self.send_ack_finished()?;
                            self.condition = finished.condition;
                            match self.condition != Condition::NoError {
                                true => {
                                    info!(
                                        "Transaction {:?}. Ended due to condition {:?}",
                                        self.id(),
                                        self.condition
                                    );
                                }
                                false => {
                                    if self.config.send_proxy_response {
                                        // if this originiated from a ProxyPutRequest
                                        // originate a Put request to send the results back
                                        if let Some(origin) = self.metadata.as_ref().and_then(|meta| {
                                    meta.message_to_user.iter().find_map(|msg| {
                                        match UserOperation::decode(&mut msg.message_text.as_slice())
                                            .ok()?
                                        {
                                            UserOperation::OriginatingTransactionIDMessage(id) => {
                                                Some(id)
                                            }
                                            _ => None,
                                        }
                                    })
                                }) {
                                    let mut message_to_user = vec![
                                        MessageToUser::from(UserOperation::Response(
                                            UserResponse::ProxyPut(self.get_proxy_response()),
                                        )),
                                        MessageToUser::from(
                                            UserOperation::OriginatingTransactionIDMessage(
                                                origin.clone(),
                                            ),
                                        ),
                                    ];
                                    finished.filestore_response.iter().for_each(|res| {
                                        message_to_user.push(MessageToUser::from(
                                            UserOperation::Response(UserResponse::ProxyFileStore(
                                                res.clone(),
                                            )),
                                        ))
                                    });
                                    // });

                                    let req = PutRequest {
                                        source_filename: "".into(),
                                        destination_filename: "".into(),
                                        destination_entity_id: origin.source_entity_id,
                                        transmission_mode: TransmissionMode::Unacknowledged,
                                        filestore_requests: vec![],
                                        message_to_user,
                                    };
                                    // we should be able to connect to the socket we are running
                                    // just fine. but we can ignore errors per
                                    // CCSDS 727.0-B-5  § 6.2.5.1.2
                                    if let Ok(mut conn) = LocalSocketStream::connect(SOCKET_ADDR) {
                                        let _ = conn.write_all(req.encode().as_slice());
                                    }
                                }
                                    }
                                }
                            }

                            self.shutdown();
                            Ok(())
                        }
                        Operations::Nak(nak) => {
                            // check and filter for naks where the length of the request is greater than
                            // the maximum allowed file size
                            // as the Receiver we don't care if the length is too big
                            // But the Sender needs segments to fit in the expected size.
                            let formatted_segments =
                                nak.segment_requests
                                    .into_iter()
                                    .flat_map(|form| match form {
                                        SegmentRequestForm {
                                            start_offset: FileSizeSensitive::Small(s),
                                            end_offset: FileSizeSensitive::Small(e),
                                        } => (s..e)
                                            .step_by(self.config.file_size_segment.into())
                                            .map(|num| {
                                                if num
                                                    < e.saturating_sub(
                                                        self.config.file_size_segment.into(),
                                                    )
                                                {
                                                    SegmentRequestForm {
                                                        start_offset: FileSizeSensitive::Small(num),
                                                        end_offset: FileSizeSensitive::Small(
                                                            num + self.config.file_size_segment
                                                                as u32,
                                                        ),
                                                    }
                                                } else {
                                                    SegmentRequestForm {
                                                        start_offset: FileSizeSensitive::Small(num),
                                                        end_offset: FileSizeSensitive::Small(e),
                                                    }
                                                }
                                            })
                                            .collect::<Vec<SegmentRequestForm>>(),
                                        SegmentRequestForm {
                                            start_offset: FileSizeSensitive::Large(s),
                                            end_offset: FileSizeSensitive::Large(e),
                                        } => (s..e)
                                            .step_by(self.config.file_size_segment.into())
                                            .map(|num| {
                                                if num
                                                    < e.saturating_sub(
                                                        self.config.file_size_segment.into(),
                                                    )
                                                {
                                                    SegmentRequestForm {
                                                        start_offset: FileSizeSensitive::Large(num),
                                                        end_offset: FileSizeSensitive::Large(
                                                            num + self.config.file_size_segment
                                                                as u64,
                                                        ),
                                                    }
                                                } else {
                                                    SegmentRequestForm {
                                                        start_offset: FileSizeSensitive::Large(num),
                                                        end_offset: FileSizeSensitive::Large(e),
                                                    }
                                                }
                                            })
                                            .collect::<Vec<SegmentRequestForm>>(),
                                        _ => unreachable!(),
                                    });
                            self.naks.extend(formatted_segments);
                            // filter out any duplicated NAKS
                            let mut uniques = HashSet::new();
                            self.naks.retain(|element| uniques.insert(element.clone()));
                            Ok(())
                        }
                        Operations::Ack(ack) => {
                            if ack.directive == PDUDirective::EoF {
                                self.timer.ack.pause();
                                self.waiting_on = WaitingOn::None;
                                // all good
                                Ok(())
                            } else {
                                Err(TransactionError::UnexpectedPDU((
                                    self.config.sequence_number.clone(),
                                    self.config.action_type,
                                    self.config.transmission_mode.clone(),
                                    format!("ACK {:?}", ack.directive),
                                )))
                            }
                        }
                        Operations::KeepAlive(keepalive) => {
                            self.received_file_size = keepalive.progress;
                            Ok(())
                        }
                    },
                }
            }
            (Action::Send, TransmissionMode::Unacknowledged) => match payload {
                PDUPayload::FileData(_data) => Err(TransactionError::UnexpectedPDU((
                    self.config.sequence_number.clone(),
                    self.config.action_type,
                    self.config.transmission_mode.clone(),
                    "File Data".to_owned(),
                ))),
                PDUPayload::Directive(operation) => match operation {
                    Operations::EoF(_eof) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "End of File".to_owned(),
                    ))),
                    Operations::Metadata(_metadata) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "Metadata PDU".to_owned(),
                    ))),
                    Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "Prompt PDU".to_owned(),
                    ))),
                    Operations::Ack(_ack) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "ACK PDU".to_owned(),
                    ))),
                    Operations::KeepAlive(_keepalive) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "Keep Alive PDU".to_owned(),
                    ))),
                    Operations::Finished(finished) => {
                        match self
                            .metadata
                            .as_ref()
                            .map(|meta| meta.closure_requested)
                            .unwrap_or(false)
                        {
                            true => {
                                if finished.condition != Condition::NoError {
                                    info!(
                                        "Transaction {:?}. Ended due to condition {:?}",
                                        self.id(),
                                        finished.condition
                                    );
                                }
                                self.condition = finished.condition;
                                self.delivery_code = finished.delivery_code;
                                self.shutdown();
                                Ok(())
                            }
                            false => Err(TransactionError::UnexpectedPDU((
                                self.config.sequence_number.clone(),
                                self.config.action_type,
                                self.config.transmission_mode.clone(),
                                "Prompt PDU".to_owned(),
                            ))),
                        }
                    }
                    Operations::Nak(_) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "NAK PDU".to_owned(),
                    ))),
                },
            },
            (Action::Receive, TransmissionMode::Acknowledged) => {
                match payload {
                    PDUPayload::FileData(filedata) => {
                        // if we're getting data but have sent a nak
                        // restart the timer.
                        if self.waiting_on == WaitingOn::Nak {
                            self.timer.restart_nak();
                        }
                        // Issue notice of recieved? Log it.
                        let (offset, length) = self.store_file_data(filedata)?;
                        // update the total received size if appropriate.
                        let size = offset + length;
                        if self.received_file_size < size {
                            self.received_file_size = size;
                        }
                        self.update_naks(self.metadata.as_ref().map(|meta| meta.file_size.clone()));

                        // need a block here for if we have sent naks
                        // in deferred mode. a second EoF is not sent
                        // so we should check if we have the whole thing?
                        if self.waiting_on == WaitingOn::Nak {
                            match self.naks.is_empty() {
                                false => self.send_naks()?,
                                true => {
                                    self.finalize_receive()?;
                                    self.timer.nak.pause();
                                    self.waiting_on = WaitingOn::None;
                                    self.send_finished(if self.condition == Condition::NoError {
                                        None
                                    } else {
                                        Some(self.config.destination_entity_id.clone())
                                    })?
                                }
                            };
                        }

                        Ok(())
                    }
                    PDUPayload::Directive(operation) => {
                        match operation {
                            Operations::EoF(eof) => {
                                self.condition = eof.condition;
                                self.send_ack_eof()?;
                                self.checksum = Some(eof.checksum);
                                self.waiting_on = WaitingOn::MissingData;

                                if self.condition == Condition::NoError {
                                    self.update_naks(Some(eof.file_size.clone()));

                                    self.check_file_size(eof.file_size)?;
                                    match self.naks.is_empty() {
                                        true => {
                                            self.finalize_receive()?;
                                            self.timer.nak.pause();
                                            self.waiting_on = WaitingOn::None;
                                            self.send_finished(
                                                if self.condition == Condition::NoError {
                                                    None
                                                } else {
                                                    Some(self.config.destination_entity_id.clone())
                                                },
                                            )
                                        }
                                        false => self.send_naks(),
                                    }
                                } else {
                                    // Any other condition is essentially a
                                    // CANCEL operation
                                    self._cancel()
                                    // issue finished log
                                    // shutdown?
                                }
                            }
                            Operations::Finished(_finished) => {
                                Err(TransactionError::UnexpectedPDU((
                                    self.config.sequence_number.clone(),
                                    self.config.action_type,
                                    self.config.transmission_mode.clone(),
                                    "Finished".to_owned(),
                                )))
                            }
                            Operations::Ack(ack) => {
                                if ack.directive == PDUDirective::Finished
                                    && ack.directive_subtype_code == ACKSubDirective::Finished
                                    && ack.condition == Condition::NoError
                                {
                                    self.timer.ack.pause();
                                    self.waiting_on = WaitingOn::None;
                                    self.shutdown();
                                    Ok(())
                                } else {
                                    // No other ACKs are expected for a receiver
                                    Err(TransactionError::UnexpectedPDU((
                                        self.config.sequence_number.clone(),
                                        self.config.action_type,
                                        self.config.transmission_mode.clone(),
                                        format!(
                                            "ACK PDU: ({:?}, {:?})",
                                            ack.directive, ack.directive_subtype_code
                                        ),
                                    )))
                                }
                            }
                            Operations::Metadata(metadata) => {
                                if self.metadata.is_none() {
                                    let message_to_user =
                                        metadata.options.iter().filter_map(|op| match op {
                                            MetadataTLV::MessageToUser(req) => Some(req.clone()),
                                            _ => None,
                                        });
                                    // push each request up to the Daemon
                                    self.message_tx.send((
                                        self.id(),
                                        self.config.transmission_mode.clone(),
                                        message_to_user.clone().collect(),
                                    ))?;

                                    self.metadata = Some(Metadata {
                                        source_filename: std::str::from_utf8(
                                            metadata.source_filename.as_slice(),
                                        )
                                        .map_err(FileStoreError::UTF8)?
                                        .into(),
                                        destination_filename: std::str::from_utf8(
                                            metadata.destination_filename.as_slice(),
                                        )
                                        .map_err(FileStoreError::UTF8)?
                                        .into(),
                                        file_size: metadata.file_size,
                                        checksum_type: metadata.checksum_type,
                                        closure_requested: metadata.closure_requested,
                                        filestore_requests: metadata
                                            .options
                                            .iter()
                                            .filter_map(|op| match op {
                                                MetadataTLV::FileStoreRequest(req) => {
                                                    Some(req.clone())
                                                }
                                                _ => None,
                                            })
                                            .collect(),
                                        message_to_user: message_to_user.collect(),
                                    });
                                }
                                Ok(())
                            }
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU((
                                self.config.sequence_number.clone(),
                                self.config.action_type,
                                self.config.transmission_mode.clone(),
                                "NAK PDU".to_owned(),
                            ))),
                            Operations::Prompt(prompt) => match prompt.nak_or_keep_alive {
                                NakOrKeepAlive::Nak => {
                                    self.update_naks(
                                        self.metadata.as_ref().map(|meta| meta.file_size.clone()),
                                    );
                                    self.send_naks()
                                }
                                NakOrKeepAlive::KeepAlive => {
                                    let progress = self.get_progress();
                                    let data = KeepAlivePDU { progress };

                                    let payload =
                                        PDUPayload::Directive(Operations::KeepAlive(data));
                                    let payload_len = payload.clone().encode().len() as u16;

                                    let header = self.get_header(
                                        Direction::ToSender,
                                        PDUType::FileDirective,
                                        payload_len,
                                        SegmentationControl::NotPreserved,
                                    );

                                    let destination = header.source_entity_id.clone();

                                    let pdu = PDU { header, payload };
                                    self.transport_tx.send((destination, pdu))?;
                                    Ok(())
                                }
                            },
                            Operations::KeepAlive(_keepalive) => {
                                Err(TransactionError::UnexpectedPDU((
                                    self.config.sequence_number.clone(),
                                    self.config.action_type,
                                    self.config.transmission_mode.clone(),
                                    "KeepAlive PDU".to_owned(),
                                )))
                            }
                        }
                    }
                }
            }
            (Action::Receive, TransmissionMode::Unacknowledged) => match payload {
                PDUPayload::FileData(filedata) => {
                    // Issue notice of recieved? Log it.
                    let (offset, length) = self.store_file_data(filedata)?;
                    // update the total received size if appropriate.
                    let size = offset + length;
                    if self.received_file_size < size {
                        self.received_file_size = size;
                    }
                    Ok(())
                }
                PDUPayload::Directive(operation) => match operation {
                    Operations::Ack(ack) => {
                        if ack.directive == PDUDirective::Finished
                            && ack.directive_subtype_code == ACKSubDirective::Finished
                            && ack.condition == Condition::NoError
                            && self
                                .metadata
                                .as_ref()
                                .map(|meta| meta.closure_requested)
                                .unwrap_or(false)
                        {
                            Ok(())
                        } else {
                            // No other ACKs are expected for a receiver
                            Err(TransactionError::UnexpectedPDU((
                                self.config.sequence_number.clone(),
                                self.config.action_type,
                                self.config.transmission_mode.clone(),
                                format!(
                                    "ACK PDU: ({:?}, {:?})",
                                    ack.directive, ack.directive_subtype_code
                                ),
                            )))
                        }
                    }
                    Operations::EoF(eof) => {
                        self.condition = eof.condition;
                        self.checksum = Some(eof.checksum);
                        if self.condition == Condition::NoError {
                            self.check_file_size(eof.file_size)?;
                            self.finalize_receive()?;
                            // if closure was requested send a finished PDU
                            if self
                                .metadata
                                .as_ref()
                                .map(|meta| meta.closure_requested)
                                .unwrap_or(false)
                            {
                                self.send_finished(if self.condition == Condition::NoError {
                                    None
                                } else {
                                    Some(self.config.destination_entity_id.clone())
                                })?;
                            }

                            Ok(())
                        } else {
                            // Any other condition is essentially a
                            // CANCEL operation
                            self._cancel()
                            // issue finished log
                            // shutdown?
                        }
                    }
                    Operations::Metadata(metadata) => {
                        if self.metadata.is_none() {
                            let message_to_user =
                                metadata.options.iter().filter_map(|op| match op {
                                    MetadataTLV::MessageToUser(req) => Some(req.clone()),
                                    _ => None,
                                });
                            // push each request up to the Daemon
                            self.message_tx.send((
                                self.id(),
                                self.config.transmission_mode.clone(),
                                message_to_user.clone().collect(),
                            ))?;

                            self.metadata = Some(Metadata {
                                source_filename: std::str::from_utf8(
                                    metadata.source_filename.as_slice(),
                                )
                                .map_err(FileStoreError::UTF8)?
                                .into(),
                                destination_filename: std::str::from_utf8(
                                    metadata.destination_filename.as_slice(),
                                )
                                .map_err(FileStoreError::UTF8)?
                                .into(),
                                file_size: metadata.file_size,
                                checksum_type: metadata.checksum_type,
                                closure_requested: metadata.closure_requested,
                                filestore_requests: metadata
                                    .options
                                    .iter()
                                    .filter_map(|op| match op {
                                        MetadataTLV::FileStoreRequest(req) => Some(req.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                                message_to_user: message_to_user.collect(),
                            });
                        }
                        Ok(())
                    }
                    // should never receive this as a Receiver type
                    Operations::Finished(_finished) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "Finished".to_owned(),
                    ))),
                    Operations::KeepAlive(_keepalive) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "Keep Alive".to_owned(),
                    ))),
                    Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "Prompt".to_owned(),
                    ))),
                    Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU((
                        self.config.sequence_number.clone(),
                        self.config.action_type,
                        self.config.transmission_mode.clone(),
                        "NAK PDU".to_owned(),
                    ))),
                },
            },
        }
    }

    fn send_metadata(&mut self) -> TransactionResult<()> {
        self.timer.restart_inactivity();
        let id = self.id();
        let destination = self.config.destination_entity_id.clone();
        let metadata = MetadataPDU {
            closure_requested: self
                .metadata
                .as_ref()
                .map(|meta| meta.closure_requested)
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            checksum_type: self
                .metadata
                .as_ref()
                .map(|meta| meta.checksum_type.clone())
                .ok_or_else(|| {
                    let id = self.id();
                    TransactionError::MissingMetadata(id)
                })?,
            file_size: self
                .metadata
                .as_ref()
                .map(|meta| meta.file_size.clone())
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            source_filename: self
                .metadata
                .as_ref()
                .map(|meta| meta.source_filename.as_str().as_bytes().to_vec())
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            destination_filename: self
                .metadata
                .as_ref()
                .map(|meta| meta.destination_filename.as_str().as_bytes().to_vec())
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            options: self
                .metadata
                .as_ref()
                .map(|meta| {
                    meta.filestore_requests
                        .iter()
                        .map(|fs| MetadataTLV::FileStoreRequest(fs.clone()))
                        .chain(
                            meta.message_to_user
                                .iter()
                                .map(|msg| MetadataTLV::MessageToUser(msg.clone())),
                        )
                        .collect()
                })
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
        };
        let payload = PDUPayload::Directive(Operations::Metadata(metadata));
        let payload_len: u16 = payload.clone().encode().len().try_into()?;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            // TODO add semgentation Control ability
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;
        Ok(())
    }

    pub(crate) fn put(&mut self, metadata: Metadata) -> TransactionResult<()> {
        self.metadata = Some(metadata);
        self.send_metadata()
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use super::*;
    use crate::{
        assert_err,
        filestore::NativeFileStore,
        pdu::{FileStoreAction, FileStoreStatus, PromptPDU, RenameStatus},
    };

    use camino::Utf8Path;
    use crossbeam_channel::unbounded;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    #[once]
    fn tempdir_fixture() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    #[once]
    fn default_config() -> TransactionConfig {
        TransactionConfig {
            action_type: Action::Send,
            source_entity_id: VariableID::from(12_u16),
            destination_entity_id: VariableID::from(15_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            sequence_number: VariableID::from(3_u16),
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: HashMap::new(),
            file_size_segment: 3_u16,
            crc_flag: CRCFlag::NotPresent,
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: 5_u32,
            inactivity_timeout: 300_i64,
            ack_timeout: 300_i64,
            nak_timeout: 300_i64,
            send_proxy_response: false,
        }
    }

    #[rstest]
    fn test_transaction(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        assert_eq!(
            TransactionStatus::Undefined,
            transaction.get_status().clone()
        );
        let mut path = Utf8PathBuf::new();
        path.push("a");

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        assert!(transaction.is_file_transfer())
    }

    #[rstest]
    fn header(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let payload_len = 12;
        let expected = PDUHeader {
            version: U3::One,
            pdu_type: PDUType::FileDirective,
            direction: Direction::ToSender,
            transmission_mode: TransmissionMode::Acknowledged,
            crc_flag: CRCFlag::NotPresent,
            large_file_flag: FileSizeFlag::Small,
            pdu_data_field_length: payload_len,
            segmentation_control: SegmentationControl::NotPreserved,
            segment_metadata_flag: SegmentedData::NotPresent,
            source_entity_id: default_config.source_entity_id.clone(),
            destination_entity_id: default_config.destination_entity_id.clone(),
            transaction_sequence_number: default_config.sequence_number.clone(),
        };
        assert_eq!(
            expected,
            transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved
            )
        )
    }

    #[rstest]
    fn store_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let input = vec![0, 5, 255, 99];
        let data = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: FileSizeSensitive::Small(6),
            file_data: input.clone(),
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(FileSizeSensitive::Small(6), offset);
        assert_eq!(4, length);

        let handle = transaction.get_handle().unwrap();
        handle.seek(SeekFrom::Start(6)).unwrap();
        let mut buff = vec![0; 4];
        handle.read_exact(&mut buff).unwrap();

        assert_eq!(input, buff)
    }

    #[rstest]
    fn finalize_file(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let input = "This is test data\n!";
        let output_file = {
            let mut temp_buff = Utf8PathBuf::new();
            temp_buff.push("finalize_test.txt");
            temp_buff
        };

        let mut transaction = Transaction::new(config, filestore.clone(), transport_tx, message_tx);
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.len() as u32),
            source_filename: output_file.clone(),
            destination_filename: output_file.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let data = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: FileSizeSensitive::Small(0),
            file_data: input.as_bytes().to_vec(),
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(FileSizeSensitive::Small(0), offset);
        assert_eq!(input.len(), length);

        let result = transaction
            .finalize_file()
            .expect("Error writing to finalize file.");
        assert_eq!(FileStatusCode::Retained, result);

        let mut out_string = String::new();
        let contents = {
            filestore
                .open(output_file, OpenOptions::new().read(true))
                .expect("Cannot open finalized file.")
                .read_to_string(&mut out_string)
                .expect("Cannot read finalized file.")
        };
        assert_eq!(input.len(), contents);
        assert_eq!(input, out_string)
    }

    #[rstest]
    fn send_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore.clone(), transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("testfile");
            buf
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(10),
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let input = vec![0, 5, 255, 99];

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: FileSizeSensitive::Small(6),
            file_data: input.clone(),
        }));
        let payload_len = payload.clone().encode().len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            let fname = transaction
                .metadata
                .as_ref()
                .unwrap()
                .source_filename
                .clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(fname, OpenOptions::new().create_new(true).write(true))
                    .unwrap();
                handle
                    .seek(SeekFrom::Start(6))
                    .expect("Cannot seek file cursor in thread.");
                handle
                    .write_all(input.as_slice())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }

            let offset = FileSizeSensitive::Small(6);
            let length = 4;

            transaction
                .send_file_segment(Some(offset.as_u64()), Some(length as u16))
                .unwrap();
        });
        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id.clone();
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    #[case(SegmentRequestForm { start_offset: FileSizeSensitive::Small(6), end_offset: FileSizeSensitive::Small(10) })]
    #[case(SegmentRequestForm { start_offset: FileSizeSensitive::Small(0), end_offset: FileSizeSensitive::Small(0) })]
    fn send_missing(
        #[case] nak: SegmentRequestForm,
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let mut path = Utf8PathBuf::new();
        path.push("testfile_missing");

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(10),
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let input = vec![0, 5, 255, 99];
        let pdu = match &nak {
            SegmentRequestForm {
                start_offset: FileSizeSensitive::Small(6),
                end_offset: FileSizeSensitive::Small(10),
            } => {
                let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
                    offset: FileSizeSensitive::Small(6),
                    file_data: input.clone(),
                }));
                let payload_len = payload.clone().encode().len();

                let header = transaction.get_header(
                    Direction::ToReceiver,
                    PDUType::FileData,
                    payload_len as u16,
                    SegmentationControl::NotPreserved,
                );
                PDU { header, payload }
            }
            _ => {
                let payload = PDUPayload::Directive(Operations::Metadata(MetadataPDU {
                    closure_requested: false,
                    file_size: FileSizeSensitive::Small(10),
                    checksum_type: ChecksumType::Modular,
                    source_filename: path.clone().as_str().as_bytes().to_vec(),
                    destination_filename: path.clone().as_str().as_bytes().to_vec(),
                    options: vec![],
                }));
                let payload_len = payload.clone().encode().len();

                let header = transaction.get_header(
                    Direction::ToReceiver,
                    PDUType::FileDirective,
                    payload_len as u16,
                    SegmentationControl::NotPreserved,
                );
                PDU { header, payload }
            }
        };

        thread::spawn(move || {
            let fname = transaction
                .metadata
                .as_ref()
                .unwrap()
                .source_filename
                .clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(
                        fname,
                        OpenOptions::new().create(true).truncate(true).write(true),
                    )
                    .unwrap();
                handle
                    .seek(SeekFrom::Start(6))
                    .expect("Cannot seek file cursor in thread.");
                handle
                    .write_all(input.as_slice())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }

            transaction.naks.push_back(nak);
            transaction.send_missing_data().unwrap();

            transaction
                .filestore
                .delete_file(path.clone())
                .expect("cannot remove file");
        });
        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id.clone();
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn update_naks(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.file_size_flag = file_size_flag;
        let file_size = match &file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(20),
            FileSizeFlag::Large => FileSizeSensitive::Large(20),
        };

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let input = vec![0, 5, 255, 99];
        let data = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: match file_size_flag {
                FileSizeFlag::Small => FileSizeSensitive::Small(6),
                FileSizeFlag::Large => FileSizeSensitive::Large(6),
            },
            file_data: input,
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        match file_size_flag {
            FileSizeFlag::Small => {
                assert_eq!(FileSizeSensitive::Small(6), offset);
            }
            FileSizeFlag::Large => {
                assert_eq!(FileSizeSensitive::Large(6), offset);
            }
        }
        assert_eq!(4, length);
        transaction.update_naks(Some(file_size.clone()));
        transaction.received_file_size = file_size.clone();

        let expected: VecDeque<SegmentRequestForm> = match file_size_flag {
            FileSizeFlag::Small => vec![
                SegmentRequestForm::from((0_u32, 0_u32)),
                SegmentRequestForm::from((0_u32, 6_u32)),
                SegmentRequestForm::from((10_u32, 20_u32)),
            ]
            .into(),
            FileSizeFlag::Large => vec![
                SegmentRequestForm::from((0_u64, 0_u64)),
                SegmentRequestForm::from((0_u64, 6_u64)),
                SegmentRequestForm::from((10_u64, 20_u64)),
            ]
            .into(),
        };

        assert_eq!(expected, transaction.naks);

        let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowldegmentPDU {
            start_of_scope: match file_size_flag {
                FileSizeFlag::Small => FileSizeSensitive::Small(0),
                FileSizeFlag::Large => FileSizeSensitive::Large(0),
            },
            end_of_scope: file_size,
            segment_requests: expected.into(),
        }));

        let payload_len = payload.clone().encode().len();
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        // now that naks are created correctly
        // test the right PDU is sent
        thread::spawn(move || {
            transaction.send_naks().unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.source_entity_id.clone();

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn send_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore.clone(), transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_eof.dat");
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let (checksum, _overflow) =
            input
                .as_bytes()
                .chunks(4)
                .fold((0_u32, false), |accum: (u32, bool), chunk| {
                    let mut vec = vec![0_u8; 4];
                    vec[..chunk.len()].copy_from_slice(chunk);
                    accum
                        .0
                        .overflowing_add(u32::from_be_bytes(vec.try_into().unwrap()))
                });

        let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
            condition: Condition::NoError,
            checksum,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            fault_location: None,
        }));
        let payload_len = payload.clone().encode().len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            let fname = transaction
                .metadata
                .as_ref()
                .unwrap()
                .source_filename
                .clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(fname, OpenOptions::new().create_new(true).write(true))
                    .unwrap();
                handle
                    .write_all(input.as_bytes())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }
            transaction.send_eof(None).unwrap()
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id.clone();

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);

        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    fn cancel_send(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            Transaction::new(config.clone(), filestore.clone(), transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push(format!("test_eof_{:}.dat", config.transmission_mode as u8));
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let (checksum, _overflow) =
            input
                .as_bytes()
                .chunks(4)
                .fold((0_u32, false), |accum: (u32, bool), chunk| {
                    let mut vec = vec![0_u8; 4];
                    vec[..chunk.len()].copy_from_slice(chunk);
                    accum
                        .0
                        .overflowing_add(u32::from_be_bytes(vec.try_into().unwrap()))
                });

        let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
            condition: Condition::CancelReceived,
            checksum,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            fault_location: Some(config.source_entity_id),
        }));
        let payload_len = payload.clone().encode().len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            let fname = transaction
                .metadata
                .as_ref()
                .unwrap()
                .source_filename
                .clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(
                        fname,
                        OpenOptions::new().create(true).truncate(true).write(true),
                    )
                    .unwrap();
                handle
                    .write_all(input.as_bytes())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }
            transaction.cancel().unwrap();

            if transaction.config.transmission_mode == TransmissionMode::Unacknowledged {
                assert_eq!(TransactionStatus::Terminated, transaction.status);
            }
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id.clone();

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);

        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    fn cancel_receive(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;
        config.action_type = Action::Receive;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config.clone(), filestore, transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push(format!("test_eof_{:}.dat", config.transmission_mode as u8));
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Finished(Finished {
            condition: Condition::CancelReceived,
            delivery_code: transaction.delivery_code.clone(),
            file_status: transaction.file_status.clone(),
            filestore_response: vec![],
            fault_location: None,
        }));
        let payload_len = payload.clone().encode().len();

        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            transaction.cancel().unwrap();

            if transaction.config.transmission_mode == TransmissionMode::Unacknowledged {
                assert_eq!(TransactionStatus::Terminated, transaction.status);
            }
        });

        if config.transmission_mode == TransmissionMode::Acknowledged {
            let (destination_id, received_pdu) = transport_rx.recv().unwrap();
            let expected_id = default_config.source_entity_id.clone();

            assert_eq!(expected_id, destination_id);
            assert_eq!(pdu, received_pdu);
        }
    }

    #[rstest]
    fn suspend(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        transaction.timer.restart_ack();
        transaction.timer.restart_inactivity();
        transaction.timer.restart_nak();

        {
            let timers = [
                &transaction.timer.nak,
                &transaction.timer.inactivity,
                &transaction.timer.nak,
            ];
            timers.iter().for_each(|timer| {
                assert!(timer.is_ticking());
            });
        }

        transaction.suspend();

        let timers = [
            &transaction.timer.ack,
            &transaction.timer.inactivity,
            &transaction.timer.nak,
        ];
        timers.iter().for_each(|timer| assert!(!timer.is_ticking()));
    }

    #[rstest]
    fn send_ack_fin(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config.clone(), filestore, transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push(format!("test_eof_{:}.dat", config.transmission_mode as u8));
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
            directive: PDUDirective::Finished,
            directive_subtype_code: ACKSubDirective::Finished,
            condition: Condition::NoError,
            transaction_status: transaction.status.clone(),
        }));

        let payload_len = payload.clone().encode().len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            transaction.send_ack_finished().unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id.clone();

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn send_ack_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config.clone(), filestore, transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push(format!("test_eof_{:}.dat", config.transmission_mode as u8));
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
            directive: PDUDirective::EoF,
            directive_subtype_code: ACKSubDirective::Other,
            condition: Condition::NoError,
            transaction_status: transaction.status.clone(),
        }));

        let payload_len = payload.clone().encode().len();

        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            transaction.send_ack_eof().unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.source_entity_id.clone();

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn finalize_receive(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore.clone(), transport_tx, message_tx);

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_finalize_receive.dat");
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(input.as_bytes().len() as u32),
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            checksum_type: ChecksumType::Modular,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::DeleteFile,
                first_filename: path.as_str().as_bytes().to_vec(),
                second_filename: vec![],
            }],
        });

        let (checksum, _overflow) =
            input
                .as_bytes()
                .chunks(4)
                .fold((0_u32, false), |accum: (u32, bool), chunk| {
                    let mut vec = [0_u8; 4];
                    vec[..chunk.len()].copy_from_slice(chunk);
                    accum.0.overflowing_add(u32::from_be_bytes(vec))
                });

        let pdu = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: FileSizeSensitive::Small(0),
            file_data: input.as_bytes().to_vec(),
        });
        transaction.checksum = Some(checksum);

        transaction.store_file_data(pdu).unwrap();
        transaction.finalize_receive().unwrap();

        assert!(!filestore.get_native_path(path).exists());
    }

    #[rstest]
    fn pdu_error_unack_recv(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::Ack(PositiveAcknowledgePDU {
                directive: PDUDirective::Finished,
                directive_subtype_code: ACKSubDirective::Finished,
                condition: Condition::NoError,
                transaction_status: TransactionStatus::Active,
            }),
            Operations::Ack(PositiveAcknowledgePDU {
                directive: PDUDirective::EoF,
                directive_subtype_code: ACKSubDirective::Other,
                condition: Condition::NoError,
                transaction_status: TransactionStatus::Active,
            }),
            Operations::Finished(Finished{
                condition: Condition::CheckLimitReached,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![FileStoreResponse {
                    action_and_status: FileStoreStatus::RenameFile(
                        RenameStatus::NewFilenameAlreadyExists,
                    ),
                    first_filename: "/path/to/a/file".as_bytes().to_vec(),
                    second_filename: "/path/to/a/new/file".as_bytes().to_vec(),
                    filestore_message: vec![1_u8, 3, 58],
                }],
                fault_location: None,

            }),
            Operations::KeepAlive(KeepAlivePDU{
                progress: FileSizeSensitive::Small(12_u32)
            }),
            Operations::Prompt(PromptPDU{
                nak_or_keep_alive: NakOrKeepAlive::KeepAlive,
            }),
            Operations::Nak(NegativeAcknowldegmentPDU{
                start_of_scope: FileSizeSensitive::Small(0_u32),
                end_of_scope: FileSizeSensitive::Small(1022_u32),
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            })
        )]
        operation: Operations,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU((
                _,
                Action::Receive,
                TransmissionMode::Unacknowledged,
                _
            )))
        )
    }

    #[rstest]
    fn pdu_error_ack_recv(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::Ack(PositiveAcknowledgePDU {
                directive: PDUDirective::EoF,
                directive_subtype_code: ACKSubDirective::Other,
                condition: Condition::NoError,
                transaction_status: TransactionStatus::Active,
            }),
            Operations::Finished(Finished{
                condition: Condition::CheckLimitReached,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![FileStoreResponse {
                    action_and_status: FileStoreStatus::RenameFile(
                        RenameStatus::NewFilenameAlreadyExists,
                    ),
                    first_filename: "/path/to/a/file".as_bytes().to_vec(),
                    second_filename: "/path/to/a/new/file".as_bytes().to_vec(),
                    filestore_message: vec![1_u8, 3, 58],
                }],
                fault_location: None,

            }),
            Operations::KeepAlive(KeepAlivePDU{
                progress: FileSizeSensitive::Small(12_u32)
            }),
            Operations::Nak(NegativeAcknowldegmentPDU{
                start_of_scope: FileSizeSensitive::Small(0_u32),
                end_of_scope: FileSizeSensitive::Small(1022_u32),
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            })
        )]
        operation: Operations,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU((
                _,
                Action::Receive,
                TransmissionMode::Acknowledged,
                _
            )))
        )
    }

    #[rstest]
    fn pdu_error_unack_send(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::EoF(EndOfFile{
                condition: Condition::CancelReceived,
                checksum: 12_u32,
                file_size: FileSizeSensitive::Small(1022_u32),
                fault_location: None,
            }),
            Operations::Metadata(MetadataPDU{
                closure_requested: false,
                checksum_type: ChecksumType::Modular,
                file_size: FileSizeSensitive::Small(1022_u32),
                source_filename: "test_filename".as_bytes().to_vec(),
                destination_filename: "test_filename".as_bytes().to_vec(),
                options: vec![],
            }),
            Operations::Prompt(PromptPDU{
                nak_or_keep_alive: NakOrKeepAlive::KeepAlive,
            }),
            Operations::Ack(PositiveAcknowledgePDU {
                directive: PDUDirective::EoF,
                directive_subtype_code: ACKSubDirective::Other,
                condition: Condition::NoError,
                transaction_status: TransactionStatus::Active,
            }),
            Operations::KeepAlive(KeepAlivePDU{
                progress: FileSizeSensitive::Small(12_u32)
            }),
            Operations::Finished(Finished{
                condition: Condition::CheckLimitReached,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![FileStoreResponse {
                    action_and_status: FileStoreStatus::RenameFile(
                        RenameStatus::NewFilenameAlreadyExists,
                    ),
                    first_filename: "/path/to/a/file".as_bytes().to_vec(),
                    second_filename: "/path/to/a/new/file".as_bytes().to_vec(),
                    filestore_message: vec![1_u8, 3, 58],
                }],
                fault_location: None,

            }),
            Operations::Nak(NegativeAcknowldegmentPDU{
                start_of_scope: FileSizeSensitive::Small(0_u32),
                end_of_scope: FileSizeSensitive::Small(1022_u32),
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            }),
        )]
        operation: Operations,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU((
                _,
                Action::Send,
                TransmissionMode::Unacknowledged,
                _
            )))
        )
    }

    #[rstest]
    fn pdu_error_ack_send(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::EoF(EndOfFile{
                condition: Condition::CancelReceived,
                checksum: 12_u32,
                file_size: FileSizeSensitive::Small(1022_u32),
                fault_location: None,
            }),
            Operations::Metadata(MetadataPDU{
                closure_requested: false,
                checksum_type: ChecksumType::Modular,
                file_size: FileSizeSensitive::Small(1022_u32),
                source_filename: "test_filename".as_bytes().to_vec(),
                destination_filename: "test_filename".as_bytes().to_vec(),
                options: vec![],
            }),
            Operations::Prompt(PromptPDU{
                nak_or_keep_alive: NakOrKeepAlive::KeepAlive,
            }),
            Operations::Ack(PositiveAcknowledgePDU {
                directive: PDUDirective::Finished,
                directive_subtype_code: ACKSubDirective::Finished,
                condition: Condition::NoError,
                transaction_status: TransactionStatus::Active,
            }),
        )]
        operation: Operations,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU((
                _,
                Action::Send,
                TransmissionMode::Acknowledged,
                _
            )))
        )
    }

    #[rstest]
    fn pdu_error_send_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: FileSizeSensitive::Small(12_u32),
            file_data: (0..12_u8).collect::<Vec<u8>>(),
        }));
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileData,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU((_, Action::Send, _, _)))
        )
    }

    #[rstest]
    fn recv_store_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: FileSizeSensitive::Small(12_u32),
            file_data: (0..12_u8).collect::<Vec<u8>>(),
        }));
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileData,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        transaction.process_pdu(pdu).unwrap();
        let contents: Vec<u8> = {
            let mut buf = Vec::new();
            let handle = transaction.get_handle().unwrap();
            handle.rewind().unwrap();
            handle.read_to_end(&mut buf).unwrap();
            buf
        };
        let expected = {
            let mut buf = vec![0_u8; 12];
            buf.extend(0..12_u8);
            buf
        };
        assert_eq!(expected, contents);
    }

    #[rstest]
    fn recv_store_metadata(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, message_rx) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        let expected_msg = MessageToUser {
            message_text: vec![1_u8, 3, 55],
        };
        let fs_req = FileStoreRequest {
            action_code: FileStoreAction::CreateFile,
            first_filename: "some_name".as_bytes().to_vec(),
            second_filename: vec![],
        };

        let expected = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![expected_msg.clone()],
            filestore_requests: vec![fs_req.clone()],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Metadata(MetadataPDU {
            closure_requested: false,
            checksum_type: ChecksumType::Modular,
            file_size: FileSizeSensitive::Small(600_u32),
            source_filename: "Test_file.txt".as_bytes().to_vec(),
            destination_filename: "Test_file.txt".as_bytes().to_vec(),
            options: vec![
                MetadataTLV::MessageToUser(expected_msg.clone()),
                MetadataTLV::FileStoreRequest(fs_req),
            ],
        }));
        let payload_len = payload.clone().encode().len() as u16;
        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            transaction.process_pdu(pdu).unwrap();
            assert_eq!(expected, transaction.metadata);
        });

        let (_, _, user_msg) = message_rx.recv().unwrap();
        assert_eq!(1, user_msg.len());
        assert_eq!(expected_msg, user_msg[0])
    }

    #[rstest]
    fn recv_eof_all_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.transmission_mode = transmission_mode.clone();

        let expected_id = config.source_entity_id.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: true,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let input_data = "Some_test words!\nHello\nWorld!";

        let (checksum, _overflow) =
            input_data
                .as_bytes()
                .chunks(4)
                .fold((0_u32, false), |accum: (u32, bool), chunk| {
                    let mut vec = vec![0_u8; 4];
                    vec[..chunk.len()].copy_from_slice(chunk);
                    accum
                        .0
                        .overflowing_add(u32::from_be_bytes(vec.try_into().unwrap()))
                });

        let file_pdu = {
            let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
                offset: FileSizeSensitive::Small(0),
                file_data: input_data.as_bytes().to_vec(),
            }));
            let payload_len = payload.clone().encode().len() as u16;
            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let input_pdu = {
            let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
                condition: Condition::NoError,
                checksum,
                file_size: FileSizeSensitive::Small(input_data.len() as u32),
                fault_location: None,
            }));
            let payload_len = payload.clone().encode().len() as u16;
            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let expected_pdu = {
            let payload = PDUPayload::Directive(Operations::Finished(Finished {
                condition: Condition::NoError,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![],
                fault_location: None,
            }));

            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        thread::spawn(move || {
            // this is the whole contents of the file
            transaction.process_pdu(file_pdu).unwrap();

            transaction.process_pdu(input_pdu).unwrap();

            if transaction.config.transmission_mode == TransmissionMode::Acknowledged {
                assert!(transaction.timer.ack.is_ticking());

                let ack_fin = {
                    let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                        directive: PDUDirective::Finished,
                        directive_subtype_code: ACKSubDirective::Finished,
                        condition: Condition::NoError,
                        transaction_status: TransactionStatus::Undefined,
                    }));

                    let payload_len = payload.clone().encode().len() as u16;
                    let header = transaction.get_header(
                        Direction::ToReceiver,
                        PDUType::FileDirective,
                        payload_len,
                        SegmentationControl::NotPreserved,
                    );

                    PDU { header, payload }
                };
                transaction.process_pdu(ack_fin).unwrap();

                assert!(!transaction.timer.ack.is_ticking());
            }
        });

        // need to get the EoF from Acknowledged mode too.
        if transmission_mode == TransmissionMode::Acknowledged {
            let eof_pdu = {
                let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                    directive: PDUDirective::EoF,
                    directive_subtype_code: ACKSubDirective::Other,
                    condition: Condition::NoError,
                    transaction_status: TransactionStatus::Undefined,
                }));

                let payload_len = payload.clone().encode().len() as u16;

                let header = {
                    let mut head = expected_pdu.header.clone();
                    head.pdu_data_field_length = payload_len;
                    head
                };

                PDU { header, payload }
            };
            let (destination_id, end_of_file) = transport_rx.recv().unwrap();

            assert_eq!(expected_id, destination_id);
            assert_eq!(eof_pdu, end_of_file)
        }

        let (destination_id, finished_pdu) = transport_rx.recv().unwrap();

        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, finished_pdu)
    }

    #[rstest]
    fn recv_prompt(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(NakOrKeepAlive::Nak, NakOrKeepAlive::KeepAlive)] nak_or_keep_alive: NakOrKeepAlive,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Receive;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.source_entity_id.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_file");
            buf
        };
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let prompt_pdu = {
            let payload = PDUPayload::Directive(Operations::Prompt(PromptPDU {
                nak_or_keep_alive: nak_or_keep_alive.clone(),
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        let expected_pdu = match nak_or_keep_alive {
            NakOrKeepAlive::Nak => {
                let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowldegmentPDU {
                    start_of_scope: FileSizeSensitive::Small(0),
                    end_of_scope: FileSizeSensitive::Small(0),
                    segment_requests: vec![SegmentRequestForm {
                        start_offset: FileSizeSensitive::Small(0),
                        end_offset: FileSizeSensitive::Small(600),
                    }],
                }));
                let payload_len = payload.clone().encode().len() as u16;

                let header = transaction.get_header(
                    Direction::ToSender,
                    PDUType::FileDirective,
                    payload_len,
                    SegmentationControl::NotPreserved,
                );

                PDU { header, payload }
            }
            NakOrKeepAlive::KeepAlive => {
                let payload = PDUPayload::Directive(Operations::KeepAlive(KeepAlivePDU {
                    progress: FileSizeSensitive::Small(0),
                }));
                let payload_len = payload.clone().encode().len() as u16;

                let header = transaction.get_header(
                    Direction::ToSender,
                    PDUType::FileDirective,
                    payload_len,
                    SegmentationControl::NotPreserved,
                );

                PDU { header, payload }
            }
        };
        thread::spawn(move || {
            transaction.process_pdu(prompt_pdu).unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, received_pdu)
    }

    #[rstest]
    fn recv_finished(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.destination_entity_id.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_file");
            buf
        };
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let finished_pdu = {
            let payload = PDUPayload::Directive(Operations::Finished(Finished {
                condition: Condition::NoError,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![],
                fault_location: None,
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        let expected_pdu = {
            let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                condition: Condition::NoError,
                directive: PDUDirective::Finished,
                directive_subtype_code: ACKSubDirective::Finished,
                transaction_status: TransactionStatus::Undefined,
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };
        thread::spawn(move || {
            transaction.process_pdu(finished_pdu).unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, received_pdu)
    }

    #[rstest]
    fn recv_nak(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
        #[values(2_usize, 17_usize)] total_size: usize,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = TransmissionMode::Acknowledged;
        config.file_size_flag = file_size_flag;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_file");
            buf
        };
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: match &file_size_flag {
                FileSizeFlag::Small => FileSizeSensitive::Small(total_size as u32),
                FileSizeFlag::Large => FileSizeSensitive::Large(total_size as u64),
            },
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let nak_pdu = {
            let payload = PDUPayload::Directive(Operations::Nak(match file_size_flag {
                FileSizeFlag::Small => NegativeAcknowldegmentPDU {
                    start_of_scope: FileSizeSensitive::Small(0),
                    end_of_scope: FileSizeSensitive::Small(total_size as u32),
                    segment_requests: vec![SegmentRequestForm {
                        start_offset: FileSizeSensitive::Small(0),
                        end_offset: FileSizeSensitive::Small(total_size as u32),
                    }],
                },
                FileSizeFlag::Large => NegativeAcknowldegmentPDU {
                    start_of_scope: FileSizeSensitive::Large(0),
                    end_of_scope: FileSizeSensitive::Large(total_size as u64),
                    segment_requests: vec![SegmentRequestForm {
                        start_offset: FileSizeSensitive::Large(0),
                        end_offset: FileSizeSensitive::Large(total_size as u64),
                    }],
                },
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        let expected_naks: VecDeque<SegmentRequestForm> = match &file_size_flag {
            FileSizeFlag::Small => (0..total_size as u32)
                .step_by(transaction.config.file_size_segment.into())
                .map(|num| {
                    if num
                        < (total_size as u32)
                            .saturating_sub(transaction.config.file_size_segment.into())
                    {
                        SegmentRequestForm {
                            start_offset: FileSizeSensitive::Small(num),
                            end_offset: FileSizeSensitive::Small(
                                num + transaction.config.file_size_segment as u32,
                            ),
                        }
                    } else {
                        SegmentRequestForm {
                            start_offset: FileSizeSensitive::Small(num),
                            end_offset: FileSizeSensitive::Small(total_size as u32),
                        }
                    }
                })
                .collect(),
            FileSizeFlag::Large => (0..total_size as u64)
                .step_by(transaction.config.file_size_segment.into())
                .map(|num| {
                    if num
                        < (total_size as u64)
                            .saturating_sub(transaction.config.file_size_segment.into())
                    {
                        SegmentRequestForm {
                            start_offset: FileSizeSensitive::Large(num),
                            end_offset: FileSizeSensitive::Large(
                                num + transaction.config.file_size_segment as u64,
                            ),
                        }
                    } else {
                        SegmentRequestForm {
                            start_offset: FileSizeSensitive::Large(num),
                            end_offset: FileSizeSensitive::Large(total_size as u64),
                        }
                    }
                })
                .collect(),
        };
        transaction.process_pdu(nak_pdu).unwrap();
        assert_eq!(expected_naks, transaction.naks);
    }

    #[rstest]
    fn recv_ack_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.destination_entity_id.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_file");
            buf
        };
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(500),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Null,
        });
        transaction.checksum = Some(0);

        let ack_pdu = {
            let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                condition: Condition::NoError,
                directive: PDUDirective::EoF,
                directive_subtype_code: ACKSubDirective::Other,
                transaction_status: TransactionStatus::Undefined,
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        let expected_pdu = {
            let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
                condition: Condition::NoError,
                checksum: 0,
                file_size: FileSizeSensitive::Small(500),
                fault_location: None,
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };
        thread::spawn(move || {
            transaction.send_eof(None).unwrap();
            assert!(transaction.timer.ack.is_ticking());

            transaction.process_pdu(ack_pdu).unwrap();
            assert!(!transaction.timer.ack.is_ticking());
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, received_pdu)
    }

    #[rstest]
    fn recv_keepalive(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.action_type = Action::Send;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_file");
            buf
        };
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(500),
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Null,
        });
        transaction.checksum = Some(0);

        let keep_alive = {
            let payload = PDUPayload::Directive(Operations::KeepAlive(KeepAlivePDU {
                progress: FileSizeSensitive::Small(300),
            }));
            let payload_len = payload.clone().encode().len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        assert_eq!(FileSizeSensitive::Small(0), transaction.received_file_size);
        transaction.process_pdu(keep_alive).unwrap();
        assert_eq!(
            FileSizeSensitive::Small(300),
            transaction.received_file_size
        )
    }
}
