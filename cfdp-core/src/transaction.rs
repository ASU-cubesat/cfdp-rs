use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt::{self, Display},
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
};

use crossbeam_channel::{SendError, Sender};

use crate::{
    filestore::{ChecksumType, FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, CRCFlag, Condition, DeliveryCode, Direction, EntityID, FaultHandlerAction,
        FileDataPDU, FileSizeFlag, FileSizeSensitive, FileStatusCode, FileStoreRequest,
        FileStoreResponse, Finished, KeepAlivePDU, MessageToUser, MetadataPDU, MetadataTLV,
        NakOrKeepAlive, NegativeAcknowldegmentPDU, Operations, PDUDirective, PDUHeader, PDUPayload,
        PDUType, PositiveAcknowledgePDU, SegmentRequestForm, SegmentationControl, SegmentedData,
        TransactionStatus, TransmissionMode, UnsegmentedFileData, PDU, U3,
    },
    timer::Timer,
};

pub type TransactionResult<T> = Result<T, TransactionError>;
#[derive(Debug)]
pub enum TransactionError {
    FileStore(FileStoreError),
    Transport(SendError<(EntityID, PDU)>),
    NoFile((EntityID, Vec<u8>)),
    Daemon(String),
    Poison,
    IntConverstion(TryFromIntError),
    UnexpectedPDU((Vec<u8>, Action, TransmissionMode, String)),
    MissingMetadata((EntityID, Vec<u8>)),
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
        }
    }
}
impl Error for TransactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FileStore(source) => Some(source),
            Self::Transport(source) => Some(source),
            Self::NoFile(_) => None,
            Self::Daemon(_) => None,
            Self::Poison => None,
            Self::IntConverstion(source) => Some(source),
            Self::UnexpectedPDU(_) => None,
            Self::MissingMetadata(_) => None,
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
    /// Flag to track whether Transaciton Closure will be requested.
    pub closure_requested: bool,
}

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
    /// Flag to indicate whether or not hte file size fits inside a [u32]
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
    /// Maximum count of a Timer to reach before issuing a Timeout
    pub max_count: u32,
    /// Maximum amount of without activity before a [Timer] increments its count.
    pub inactivity_timeout: i64,
}

pub struct Transaction<T: FileStore> {
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<Mutex<T>>,
    /// Channel used to send outgoing PDUs through the Transport layer.
    transport_tx: Sender<(EntityID, PDU)>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// A mapping of offsets and segments lengths to monitor progress.
    /// For a Receiver, these are the received segments.
    /// For a Sender, these are the sent segments.
    saved_segments: BTreeMap<FileSizeSensitive, FileSizeSensitive>,
    /// The list of all missing information
    naks: Vec<SegmentRequestForm>,
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
}
impl<T: FileStore> Transaction<T> {
    /// Start a new Transaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore)
    pub fn new(
        config: TransactionConfig,
        filestore: Arc<Mutex<T>>,
        transport_tx: Sender<(EntityID, PDU)>,
    ) -> Self {
        let received_file_size = match &config.file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(0),
            FileSizeFlag::Large => FileSizeSensitive::Large(0),
        };
        let nak_timer = Timer::new(config.inactivity_timeout, config.max_count);
        let inactivity_timer = Timer::new(config.inactivity_timeout, config.max_count);

        let mut transaction = Self {
            config,
            filestore,
            transport_tx,
            file_handle: None,
            saved_segments: BTreeMap::new(),
            naks: Vec::new(),
            metadata: None,
            received_file_size,
            header: None,
            condition: Condition::NoError,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
            filestore_response: Vec::new(),
            nak_timer,
            inactivity_timer,
        };
        transaction.inactivity_timer.restart();
        transaction
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

    fn get_handle(&mut self) -> TransactionResult<&mut File> {
        if self.file_handle.is_none() {
            self.initialize_tempfile()?;
        }
        let id = self.id();
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

    fn initialize_tempfile(&mut self) -> TransactionResult<()> {
        self.file_handle = Some(self.filestore.lock()?.open_tempfile()?);
        Ok(())
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

    fn send_file_segment(&mut self) -> TransactionResult<()> {
        let (offset, file_data) = self.get_file_segment(None, None)?;
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

    fn update_naks(&mut self, file_size: Option<FileSizeSensitive>) {
        let mut naks: Vec<SegmentRequestForm> = vec![];
        let mut pointer: FileSizeSensitive = match &self.config.file_size_flag {
            FileSizeFlag::Small => FileSizeSensitive::Small(0_u32),
            FileSizeFlag::Large => FileSizeSensitive::Large(0_u64),
        };

        if self.metadata.is_none() {
            naks.push(match &self.config.file_size_flag {
                FileSizeFlag::Small => SegmentRequestForm::from((0_u32, 0_u32)),
                FileSizeFlag::Large => SegmentRequestForm::from((0_u64, 0_u64)),
            });
        }
        self.saved_segments.iter().for_each(|(offset, length)| {
            if offset > &pointer {
                naks.push(SegmentRequestForm {
                    start_offset: pointer.clone(),
                    end_offset: offset.clone(),
                });
            }
            pointer = match (offset, length) {
                (FileSizeSensitive::Small(left), FileSizeSensitive::Small(right)) => {
                    FileSizeSensitive::Small(*left + right)
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
                naks.push(SegmentRequestForm {
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

    fn cancel(&self) -> TransactionResult<()> {
        // make some kind of log/indication
        Ok(())
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
            }
            FaultHandlerAction::Cancel => {
                // send finished
                // restart ack timer
                todo!()
            }

            FaultHandlerAction::Suspend => {
                // change state to suspend
                // suspend any timers
                todo!()
            }
            FaultHandlerAction::Abandon => {
                // shutdown timers
                // end transaction
                todo!()
            }
        }
        Ok(())
    }

    fn send_ack_eof(&mut self) -> TransactionResult<()> {
        let ack = PositiveAcknowledgePDU {
            directive: PDUDirective::EoF,
            directive_subtype_code: ACKSubDirective::Other,
            condition: self.condition.clone(),
            transaction_status: TransactionStatus::Undefined,
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
        match self.received_file_size > file_size {
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
                    segment_requests: self.naks.clone(),
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

        self.filestore_response = self
            .metadata
            .as_ref()
            .map(|meta| {
                meta.filestore_requests
                    .iter()
                    .map(|req| self.filestore.lock().map(|fs| fs.process_request(req)))
                    .filter_map(Result::ok)
                    .collect()
            })
            .unwrap_or_else(Vec::new);
        Ok(())
    }

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        self.inactivity_timer.restart();
        let PDU {
            header: _header,
            payload,
        } = pdu;
        match (&self.config.action_type, &self.config.transmission_mode) {
            (Action::Send, TransmissionMode::Acknowledged) => {
                todo!()
            }
            (Action::Send, TransmissionMode::Unacknowledged) => {
                todo!()
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

    pub fn put(&mut self, options: Vec<MessageToUser>) -> TransactionResult<()> {
        let id = self.id();
        // TODO this assumes a UNIX system presently. Need to add windows support.
        let destination = self.config.destination_entity_id.clone();
        let metadata = MetadataPDU {
            closure_requested: self.metadata.as_ref().unwrap().closure_requested,
            checksum_type: self.config.checksum_type.clone(),
            file_size: self.metadata.as_ref().unwrap().file_size.clone(),
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
            options: options
                .into_iter()
                .map(MetadataTLV::MessageToUser)
                .collect(),
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
}
