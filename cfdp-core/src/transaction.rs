use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    error::Error,
    fmt::{self, Display},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
};

use crossbeam_channel::{SendError, Sender};

use crate::{
    filestore::{ChecksumType, FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, CRCFlag, Condition, DeliveryCode, Direction, EndOfFile, EntityID,
        FaultHandlerAction, FileDataPDU, FileSizeFlag, FileSizeSensitive, FileStatusCode,
        FileStoreRequest, FileStoreResponse, Finished, KeepAlivePDU, MessageToUser, MetadataPDU,
        MetadataTLV, NakOrKeepAlive, NegativeAcknowldegmentPDU, Operations, PDUDirective,
        PDUHeader, PDUPayload, PDUType, PositiveAcknowledgePDU, SegmentRequestForm,
        SegmentationControl, SegmentedData, TransactionStatus, TransmissionMode,
        UnsegmentedFileData, PDU, U3,
    },
    timer::Timer,
};

pub type TransactionResult<T> = Result<T, TransactionError>;
#[derive(Debug)]
pub enum TransactionError {
    FileStore(FileStoreError),
    Transport(SendError<(EntityID, PDU)>),
    UserMessage(SendError<MessageToUser>),
    NoFile((EntityID, Vec<u8>)),
    Daemon(String),
    Poison,
    IntConverstion(TryFromIntError),
    UnexpectedPDU((Vec<u8>, Action, TransmissionMode, String)),
    MissingMetadata((EntityID, Vec<u8>)),
    MissingNak,
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
impl From<SendError<(EntityID, PDU)>> for TransactionError {
    fn from(error: SendError<(EntityID, PDU)>) -> Self {
        Self::Transport(error)
    }
}
impl From<SendError<MessageToUser>> for TransactionError {
    fn from(error: SendError<MessageToUser>) -> Self {
        Self::UserMessage(error)
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
        }
    }
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

struct Metadata {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: PathBuf,
    /// The size of the file being transfered in this transaction.
    pub file_size: FileSizeSensitive,
    /// List of any filestore requests to take after transaction is complete
    filestore_requests: Vec<FileStoreRequest>,
    /// Any Messages to user received either from the metadataPDU or as input
    message_to_user: Vec<MessageToUser>,
    /// Flag to track whether Transaciton Closure will be requested.
    pub closure_requested: bool,
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
    pub sequence_number: Vec<u8>,
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
    /// Flag to track what kind of [Checksum](crate::filestore::ChecksumType) will be used in this transaction.
    pub checksum_type: ChecksumType,
    /// Maximum count of timeouts on a [Timer] before a fault is generated.
    pub max_count: u32,
    /// Maximum amount timeof without activity before a [Timer] increments its count.
    pub inactivity_timeout: i64,
}

pub struct Transaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<Mutex<T>>,
    /// Channel used to send outgoing PDUs through the Transport layer.
    transport_tx: Sender<(EntityID, PDU)>,
    /// Channel for message to users to propagate back up
    message_tx: Sender<MessageToUser>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// A mapping of offsets and segments lengths to monitor progress.
    /// For a Receiver, these are the received segments.
    /// For a Sender, these are the sent segments.
    saved_segments: BTreeMap<FileSizeSensitive, FileSizeSensitive>,
    /// The list of all missing information
    naks: VecDeque<SegmentRequestForm>,
    /// Flag to check if metadata on the file has been received
    metadata: Option<Metadata>,
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
    nak_timer: Timer,
    // Timer used to track if the Inactivity limit
    inactivity_timer: Timer,
    // Timer used to track ACK receipt
    ack_timer: Timer,
    // checksum cache to reduce I/0
    checksum: Option<u32>,
}
impl<T: FileStore> Transaction<T> {
    /// Start a new Transaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore)
    pub fn new(
        // Configuation of this Transaction.
        config: TransactionConfig,
        // Connection to the local FileStore implementation.
        filestore: Arc<Mutex<T>>,
        // Sender channel connected to the Transport thread to send PDUs.
        transport_tx: Sender<(EntityID, PDU)>,
        // Sender channel used to propagate Message To User back up to the Daemon Thread.
        message_tx: Sender<MessageToUser>,
    ) -> Self {
        let received_file_size = match &config.file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(0),
            FileSizeFlag::Large => FileSizeSensitive::Large(0),
        };
        let nak_timer = Timer::new(config.inactivity_timeout, config.max_count);
        let inactivity_timer = Timer::new(config.inactivity_timeout, config.max_count);
        let ack_timer = Timer::new(config.inactivity_timeout, config.max_count);

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
            nak_timer,
            inactivity_timer,
            ack_timer,
            checksum: None,
        };
        transaction.inactivity_timer.restart();
        transaction
    }

    pub fn get_status(&self) -> &TransactionStatus {
        &self.status
    }

    fn get_header(
        &mut self,
        direction: Direction,
        pdu_type: PDUType,
        pdu_data_field_length: u16,
        segmentation_control: SegmentationControl,
    ) -> PDUHeader {
        if let Some(header) = &self.header {
            header.clone()
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

    pub fn id(&self) -> (EntityID, Vec<u8>) {
        (
            self.config.source_entity_id.clone(),
            self.config.sequence_number.clone(),
        )
    }

    fn get_checksum(&mut self) -> TransactionResult<u32> {
        match self.checksum {
            Some(val) => Ok(val),
            None => {
                let checksum_type = self.config.checksum_type.clone();
                let checksum = self.get_handle()?.checksum(checksum_type)?;
                self.checksum = Some(checksum);
                Ok(checksum)
            }
        }
    }

    fn initialize_tempfile(&mut self) -> TransactionResult<()> {
        self.file_handle = Some(self.filestore.lock()?.open_tempfile()?);
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

            Some(
                self.filestore
                    .lock()?
                    .open(fname, OpenOptions::new().read(true))?,
            )
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
    fn send_file_segment(
        &mut self,
        offset: Option<u64>,
        length: Option<u16>,
    ) -> TransactionResult<()> {
        let (offset, file_data) = self.get_file_segment(offset, length)?;
        println!("{:?}, {:?}", offset, file_data);
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

    fn send_missing_data(&mut self) -> TransactionResult<()> {
        let request = self.naks.pop_front().ok_or(TransactionError::MissingNak)?;

        let (offset, length) = match (request.start_offset, request.end_offset) {
            (FileSizeSensitive::Small(s), FileSizeSensitive::Small(e)) => {
                (s.into(), (e - s).try_into()?)
            }
            (FileSizeSensitive::Large(s), FileSizeSensitive::Large(e)) => (s, (e - s).try_into()?),
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
            let mut outfile = self.filestore.lock()?.open(
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

    fn send_eof(&mut self, fault_location: Option<EntityID>) -> TransactionResult<()> {
        self.ack_timer.restart();
        match self.ack_timer.limit_reached() {
            true => self.handle_fault(Condition::PositiveLimitReached),
            false => {
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
        }
    }

    fn abandon(&mut self) {
        self.status = TransactionStatus::Terminated;
    }
    fn cancel(&mut self) -> TransactionResult<()> {
        match (&self.config.action_type, &self.config.transmission_mode) {
            (Action::Send, TransmissionMode::Acknowledged) => {
                self.condition = Condition::CancelReceived;
                self.send_eof(Some(self.config.source_entity_id.clone()))
            }
            (Action::Send, TransmissionMode::Unacknowledged) => {
                self.condition = Condition::CancelReceived;
                self.send_eof(Some(self.config.source_entity_id.clone()))?;
                self.abandon();
                Ok(())
            }
            (Action::Receive, TransmissionMode::Acknowledged) => self.send_finished(None),
            (Action::Receive, TransmissionMode::Unacknowledged) => {
                self.abandon();
                Ok(())
            }
        }
        // make some kind of log/indication
    }

    fn suspend(&mut self) {
        self.ack_timer.pause();
        self.nak_timer.pause();
        self.inactivity_timer.pause();
    }
    fn handle_fault(&mut self, condition: Condition) -> TransactionResult<()> {
        self.condition = condition;
        match self
            .config
            .fault_handler_override
            .get(&self.condition)
            .unwrap_or(&FaultHandlerAction::Cancel)
        {
            FaultHandlerAction::Ignore => {
                // Log ignoring error
                Ok(())
            }
            FaultHandlerAction::Cancel => self.cancel(),

            FaultHandlerAction::Suspend => {
                self.suspend();
                Ok(())
            }
            FaultHandlerAction::Abandon => {
                self.abandon();
                Ok(())
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
        match self.received_file_size >= file_size {
            false => Ok(()),
            true => self.handle_fault(Condition::FilesizeError),
        }
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

    fn send_finished(&mut self, fault_location: Option<EntityID>) -> TransactionResult<()> {
        self.ack_timer.restart();
        match self.ack_timer.limit_reached() {
            true => self.handle_fault(Condition::PositiveLimitReached),
            false => {
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
        }
    }

    fn send_naks(&mut self) -> TransactionResult<()> {
        self.nak_timer.restart();
        match self.nak_timer.limit_reached() {
            true => self.handle_fault(Condition::NakLimitReached),
            false => {
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
        }
    }

    fn verify_checksum(&mut self, checksum: u32) -> TransactionResult<bool> {
        let checksum_type = self.config.checksum_type.clone();
        let handle = self.get_handle()?;
        Ok(handle.checksum(checksum_type)? == checksum)
    }

    fn finalize_receive(&mut self, checksum: u32) -> TransactionResult<()> {
        self.delivery_code = DeliveryCode::Complete;

        self.file_status = if self.is_file_transfer() {
            match self.verify_checksum(checksum)? {
                true => self
                    .finalize_file()
                    .unwrap_or(FileStatusCode::FileStoreRejection),
                false => return self.handle_fault(Condition::FileChecksumFailure),
            }
        } else {
            FileStatusCode::Unreported
        };
        // A filestore rejection is a failure mode for the entire transaction.
        if self.file_status == FileStatusCode::FileStoreRejection {
            return self.handle_fault(Condition::FileStoreRejection);
        }

        // Only perform the Filestore requests if the Copy was successful
        self.filestore_response = {
            let mut fail_rest = false;
            let mut out = vec![];
            if let Some(meta) = self.metadata.as_ref() {
                for request in &meta.filestore_requests {
                    let response = match fail_rest {
                        false => {
                            let rep = self.filestore.lock()?.process_request(request);
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

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        self.inactivity_timer.restart();
        let PDU {
            header: _header,
            payload,
        } = pdu;
        match (&self.config.action_type, &self.config.transmission_mode) {
            (Action::Send, TransmissionMode::Acknowledged) => match payload {
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
                        if finished.condition == Condition::NoError {
                            self.send_ack_finished()
                            // shutdown
                        } else {
                            // essentially cancel
                            // but nothing else to do
                            self.condition = finished.condition;
                            self.send_ack_finished()
                            // shutdown
                        }
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
                                        start_offset: FileSizeSensitive::Small(mut s),
                                        end_offset: FileSizeSensitive::Small(e),
                                    } => {
                                        match e - s > (self.config.file_size_segment as u32) {
                                            // no op this is fine
                                            false => vec![form].into_iter(),
                                            true => {
                                                let mut out = vec![];
                                                while s < (e - self.config.file_size_segment as u32)
                                                {
                                                    out.push(SegmentRequestForm {
                                                        start_offset: FileSizeSensitive::Small(s),
                                                        end_offset: FileSizeSensitive::Small(
                                                            s + self.config.file_size_segment
                                                                as u32,
                                                        ),
                                                    });
                                                    s += self.config.file_size_segment as u32;
                                                }
                                                out.push(SegmentRequestForm {
                                                    start_offset: FileSizeSensitive::Small(s),
                                                    end_offset: FileSizeSensitive::Small(e),
                                                });
                                                out.into_iter()
                                            }
                                        }
                                    }
                                    SegmentRequestForm {
                                        start_offset: FileSizeSensitive::Large(mut s),
                                        end_offset: FileSizeSensitive::Large(e),
                                    } => {
                                        match e - s > (self.config.file_size_segment as u64) {
                                            // no op this is fine
                                            false => vec![form].into_iter(),
                                            true => {
                                                let mut out = vec![];
                                                while s < (e - self.config.file_size_segment as u64)
                                                {
                                                    out.push(SegmentRequestForm {
                                                        start_offset: FileSizeSensitive::Large(s),
                                                        end_offset: FileSizeSensitive::Large(
                                                            s + self.config.file_size_segment
                                                                as u64,
                                                        ),
                                                    });
                                                    s += self.config.file_size_segment as u64;
                                                }
                                                out.push(SegmentRequestForm {
                                                    start_offset: FileSizeSensitive::Large(s),
                                                    end_offset: FileSizeSensitive::Large(e),
                                                });
                                                out.into_iter()
                                            }
                                        }
                                    }
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
                            self.ack_timer.pause();
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
            },
            (Action::Send, TransmissionMode::Unacknowledged) => {
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
                        Operations::Ack(_ack) => Err(TransactionError::UnexpectedPDU((
                            self.config.sequence_number.clone(),
                            self.config.action_type,
                            self.config.transmission_mode.clone(),
                            "ACK PDU".to_owned(),
                        ))),
                        Operations::KeepAlive(_keepalive) => {
                            Err(TransactionError::UnexpectedPDU((
                                self.config.sequence_number.clone(),
                                self.config.action_type,
                                self.config.transmission_mode.clone(),
                                "Keep Alive PDU".to_owned(),
                            )))
                        }
                        Operations::Finished(finished) => {
                            self.delivery_code = finished.delivery_code;
                            match self
                                .metadata
                                .as_ref()
                                .map(|meta| meta.closure_requested)
                                .unwrap_or(false)
                            {
                                true => {
                                    if finished.condition == Condition::NoError {
                                        // self.send_ack_finished()
                                        // shutdown
                                    } else {
                                        // essentially cancel
                                        // but nothing else to do
                                        self.condition = finished.condition;
                                        // self.send_ack_finished()
                                        // shutdown
                                    }
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
                }
            }
            (Action::Receive, TransmissionMode::Acknowledged) => {
                match payload {
                    PDUPayload::FileData(filedata) => {
                        // Issue notice of recieved? Log it.
                        let (offset, length) = self.store_file_data(filedata)?;
                        // update the total received size if appropriate.
                        let size = offset + length;
                        if self.received_file_size < size {
                            self.received_file_size = size;
                        }
                        self.update_naks(self.metadata.as_ref().map(|meta| meta.file_size.clone()));
                        Ok(())
                    }
                    PDUPayload::Directive(operation) => {
                        match operation {
                            Operations::EoF(eof) => {
                                if eof.condition == Condition::NoError {
                                    self.condition = eof.condition;
                                    self.update_naks(Some(eof.file_size.clone()));
                                    self.send_ack_eof()?;

                                    self.check_file_size(eof.file_size)?;
                                    match self.naks.is_empty() {
                                        true => {
                                            self.finalize_receive(eof.checksum)?;
                                            self.nak_timer.pause();
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
                                    self.condition = eof.condition;
                                    self.send_ack_eof()?;
                                    self.cancel()
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
                                    self.ack_timer.pause();
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
                                    message_to_user
                                        .clone()
                                        .try_for_each(|msg| self.message_tx.send(msg))?;

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
                        if eof.condition == Condition::NoError {
                            self.condition = eof.condition;

                            self.check_file_size(eof.file_size)?;
                            self.finalize_receive(eof.checksum)?;
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
                            self.condition = eof.condition;
                            // self.send_ack_eof()
                            self.cancel()
                            // issue finished log
                            // shutdown?
                        }
                    }
                    Operations::Metadata(metadata) => {
                        if self.metadata.is_none() {
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
                                closure_requested: metadata.closure_requested,
                                filestore_requests: metadata
                                    .options
                                    .iter()
                                    .filter_map(|op| match op {
                                        MetadataTLV::FileStoreRequest(req) => Some(req.clone()),
                                        _ => None,
                                    })
                                    .collect(),
                                message_to_user: metadata
                                    .options
                                    .iter()
                                    .filter_map(|op| match op {
                                        MetadataTLV::MessageToUser(req) => Some(req.clone()),
                                        _ => None,
                                    })
                                    .collect(),
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
        let id = self.id();
        let destination = self.config.destination_entity_id.clone();
        let metadata = MetadataPDU {
            closure_requested: self
                .metadata
                .as_ref()
                .map(|meta| meta.closure_requested)
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            checksum_type: self.config.checksum_type.clone(),
            file_size: self
                .metadata
                .as_ref()
                .map(|meta| meta.file_size.clone())
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            source_filename: self
                .metadata
                .as_ref()
                .and_then(|meta| {
                    meta.source_filename
                        .to_str()
                        .map(|str| str.as_bytes().to_vec())
                })
                .ok_or_else(|| TransactionError::MissingMetadata(id.clone()))?,
            destination_filename: self
                .metadata
                .as_ref()
                .and_then(|meta| {
                    meta.destination_filename
                        .to_str()
                        .map(|str| str.as_bytes().to_vec())
                })
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

    pub fn put(&mut self) -> TransactionResult<()> {
        self.send_metadata()
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use crate::filestore::NativeFileStore;

    use super::*;

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
            source_entity_id: EntityID::from(12_u16),
            destination_entity_id: EntityID::from(15_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            sequence_number: 3_u16.to_be_bytes().to_vec(),
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: HashMap::new(),
            file_size_segment: 3_u16,
            crc_flag: CRCFlag::NotPresent,
            segment_metadata_flag: SegmentedData::NotPresent,
            checksum_type: ChecksumType::Modular,
            max_count: 5_u32,
            inactivity_timeout: 300_i64,
        }
    }

    #[rstest]
    fn test_transaction(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(Mutex::new(NativeFileStore::new(tempdir_fixture.path())));

        let mut transaction = Transaction::new(config, filestore, transport_tx, message_tx);
        assert_eq!(
            TransactionStatus::Undefined,
            transaction.get_status().clone()
        );
        let mut pathbuf = PathBuf::new();
        pathbuf.push("a");

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(600),
            source_filename: pathbuf.clone(),
            destination_filename: pathbuf.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
        });

        assert!(transaction.is_file_transfer())
    }

    #[rstest]
    fn header(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(Mutex::new(NativeFileStore::new(tempdir_fixture.path())));

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
        let filestore = Arc::new(Mutex::new(NativeFileStore::new(tempdir_fixture.path())));

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
    fn send_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(Mutex::new(NativeFileStore::new(tempdir_fixture.path())));
        let mut transaction = Transaction::new(config, filestore.clone(), transport_tx, message_tx);

        let mut pathbuf = PathBuf::new();
        pathbuf.push("testfile");

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(10),
            source_filename: pathbuf.clone(),
            destination_filename: pathbuf.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
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
                    .lock()
                    .unwrap()
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
        filestore
            .lock()
            .unwrap()
            .delete_file(pathbuf.clone())
            .expect("cannot remove file");
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
        let filestore = Arc::new(Mutex::new(NativeFileStore::new(tempdir_fixture.path())));
        let mut transaction = Transaction::new(config, filestore.clone(), transport_tx, message_tx);

        let mut pathbuf = PathBuf::new();
        pathbuf.push("testfile_missing");

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: FileSizeSensitive::Small(10),
            source_filename: pathbuf.clone(),
            destination_filename: pathbuf.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
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
                    source_filename: pathbuf.clone().to_str().unwrap().as_bytes().to_vec(),
                    destination_filename: pathbuf.clone().to_str().unwrap().as_bytes().to_vec(),
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
                    .lock()
                    .unwrap()
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
        });
        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id.clone();
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
        filestore
            .lock()
            .unwrap()
            .delete_file(pathbuf.clone())
            .expect("cannot remove file");
    }
}
