use std::{
    collections::{HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use crossbeam_channel::Sender;
use interprocess::local_socket::LocalSocketStream;
use log::{debug, info};

use super::{
    config::{Metadata, TransactionConfig, TransactionState, WaitingOn},
    error::{TransactionError, TransactionResult},
};
use crate::{
    daemon::{PutRequest, Report, SOCKET_ADDR},
    filestore::{FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, Condition, DeliveryCode, Direction, EndOfFile, FaultHandlerAction,
        FileDataPDU, FileStatusCode, MessageToUser, MetadataPDU, MetadataTLV, Operations,
        PDUDirective, PDUEncode, PDUHeader, PDUPayload, PDUType, PositiveAcknowledgePDU,
        ProxyPutResponse, SegmentRequestForm, SegmentationControl, SegmentedData,
        TransactionSeqNum, TransactionStatus, TransmissionMode, UnsegmentedFileData, UserOperation,
        UserResponse, VariableID, PDU, U3,
    },
    timer::Timer,
};

pub struct SendTransaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<T>,
    /// Channel used to send outgoing PDUs through the Transport layer.
    transport_tx: Sender<(VariableID, PDU)>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// The list of all missing information
    naks: VecDeque<SegmentRequestForm>,
    ///  The metadata of this Transaction
    pub(crate) metadata: Metadata,
    /// Measurement of how large of a file has been received so far
    received_file_size: u64,
    /// a cache of the header used for interactions in this transmission
    header: Option<PDUHeader>,
    /// The current condition of the transaction
    condition: Condition,
    // Track whether the transaction is complete or not
    delivery_code: DeliveryCode,
    // Status of the current File
    file_status: FileStatusCode,
    // Timer used to track if the Nak limit has been reached
    // inactivity has occurred
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
impl<T: FileStore> SendTransaction<T> {
    /// Start a new SendTransaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore)
    pub fn new(
        // Configuation of this Transaction.
        config: TransactionConfig,
        // Metadata of this Transaction
        metadata: Metadata,
        // Connection to the local FileStore implementation.
        filestore: Arc<T>,
        // Sender channel connected to the Transport thread to send PDUs.
        transport_tx: Sender<(VariableID, PDU)>,
    ) -> Self {
        let received_file_size = 0_u64;
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
            file_handle: None,
            naks: VecDeque::new(),
            metadata,
            received_file_size,
            header: None,
            condition: Condition::NoError,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
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
        self.config.transmission_mode
    }
    pub fn generate_report(&self) -> Report {
        Report {
            id: self.id(),
            state: self.get_state().clone(),
            status: self.get_status().clone(),
            condition: self.condition,
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
                transmission_mode: self.config.transmission_mode,
                crc_flag: self.config.crc_flag,
                large_file_flag: self.config.file_size_flag,
                pdu_data_field_length,
                segmentation_control,
                segment_metadata_flag: self.config.segment_metadata_flag.clone(),
                source_entity_id: self.config.source_entity_id,
                transaction_sequence_number: self.config.sequence_number,
                destination_entity_id: self.config.destination_entity_id,
            };
            self.header = Some(header.clone());
            header
        }
    }

    pub fn id(&self) -> (VariableID, TransactionSeqNum) {
        (self.config.source_entity_id, self.config.sequence_number)
    }

    fn get_checksum(&mut self) -> TransactionResult<u32> {
        match self.checksum {
            Some(val) => Ok(val),
            None => {
                let checksum = {
                    match self.is_file_transfer() {
                        true => {
                            let checksum_type = self.metadata.checksum_type.clone();
                            self.get_handle()?.checksum(checksum_type)?
                        }
                        false => 0,
                    }
                };
                self.checksum = Some(checksum);
                Ok(checksum)
            }
        }
    }

    fn open_source_file(&mut self) -> TransactionResult<()> {
        self.file_handle = {
            let fname = &self.metadata.source_filename;

            Some(self.filestore.open(fname, OpenOptions::new().read(true))?)
        };
        Ok(())
    }

    fn get_handle(&mut self) -> TransactionResult<&mut File> {
        let id = self.id();
        if self.file_handle.is_none() {
            self.open_source_file()?
        };
        self.file_handle
            .as_mut()
            .ok_or(TransactionError::NoFile(id))
    }

    pub(crate) fn is_file_transfer(&self) -> bool {
        !self.metadata.source_filename.as_os_str().is_empty()
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

        let (data, segmentation_control) = match self.config.segment_metadata_flag {
            SegmentedData::NotPresent => (
                FileDataPDU::Unsegmented(UnsegmentedFileData { offset, file_data }),
                SegmentationControl::NotPreserved,
            ),
            // TODO need definition of segment metadata. Not currently supported.
            SegmentedData::Present => unimplemented!(),
        };
        let destination = self.config.destination_entity_id;

        let payload = PDUPayload::FileData(data);

        let payload_len: u16 = payload
            .clone()
            .encode(self.config.file_size_flag)
            .len()
            .try_into()?;
        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len,
            segmentation_control,
        );
        let pdu = PDU { header, payload };

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
            false => {
                if self.do_once {
                    self.send_eof(None)?;
                    self.do_once = false;
                }
                Ok(true)
            }
        }
    }

    pub fn send_missing_data(&mut self) -> TransactionResult<()> {
        match self.naks.pop_front() {
            Some(request) => {
                // only restart inactivity if we have something to do.
                self.timer.restart_inactivity();
                let (offset, length) = (
                    request.start_offset,
                    (request.end_offset - request.start_offset).try_into()?,
                );

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
            condition: self.condition,
            checksum: self.get_checksum()?,
            file_size: self.metadata.file_size,
            fault_location,
        };

        let payload = PDUPayload::Directive(Operations::EoF(eof));

        let payload_len = payload.clone().encode(self.config.file_size_flag).len() as u16;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.destination_entity_id;
        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;
        debug!("Transaction {0:?} sending EndOfFile.", self.id());
        Ok(())
    }

    pub fn abandon(&mut self) {
        debug!("Transaction {0:?} abandoning.", self.id());
        self.status = TransactionStatus::Terminated;
        self.shutdown();
    }

    pub fn shutdown(&mut self) {
        debug!("Transaction {0:?} shutting down.", self.id());
        self.state = TransactionState::Terminated;
        self.timer.ack.pause();
        self.timer.nak.pause();
        self.timer.inactivity.pause();
    }

    pub fn cancel(&mut self) -> TransactionResult<()> {
        debug!("Transaction {0:?} canceling.", self.id());
        self.condition = Condition::CancelReceived;
        self._cancel()
    }

    fn _cancel(&mut self) -> TransactionResult<()> {
        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => self.send_eof(Some(self.config.source_entity_id)),
            TransmissionMode::Unacknowledged => {
                self.send_eof(Some(self.config.source_entity_id))?;
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
        if self.waiting_on == WaitingOn::AckEof {
            self.timer.restart_ack();
        }
        self.state = TransactionState::Active;
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
            condition: self.condition,
            transaction_status: self.status.clone(),
        };
        let payload = PDUPayload::Directive(Operations::Ack(ack));
        let payload_len = payload.clone().encode(self.config.file_size_flag).len() as u16;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.destination_entity_id;
        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;

        Ok(())
    }

    pub fn get_proxy_response(&mut self) -> ProxyPutResponse {
        ProxyPutResponse {
            condition: self.condition,
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
        if self.timer.ack.timeout_occured() && self.waiting_on == WaitingOn::AckEof {
            return self.send_eof(None);
        }
        Ok(())
    }

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        self.timer.restart_inactivity();
        let PDU {
            header: _header,
            payload,
        } = pdu;
        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => {
                match payload {
                    PDUPayload::FileData(_data) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "File Data".to_owned(),
                    )),
                    PDUPayload::Directive(operation) => match operation {
                        Operations::EoF(_eof) => Err(TransactionError::UnexpectedPDU(
                            self.config.sequence_number,
                            self.config.transmission_mode,
                            "End of File".to_owned(),
                        )),
                        Operations::Metadata(_metadata) => Err(TransactionError::UnexpectedPDU(
                            self.config.sequence_number,
                            self.config.transmission_mode,
                            "Metadata PDU".to_owned(),
                        )),
                        Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU(
                            self.config.sequence_number,
                            self.config.transmission_mode,
                            "Prompt PDU".to_owned(),
                        )),
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
                                        if let Some(origin) = self.metadata.message_to_user.iter().find_map(|msg| {
                                                match UserOperation::decode(&mut msg.message_text.as_slice()).ok()? {
                                                    UserOperation::OriginatingTransactionIDMessage(id) => Some(id),
                                                    _ => None,
                                                }
                                        }) {
                                            let mut message_to_user = vec![
                                                MessageToUser::from(UserOperation::Response(UserResponse::ProxyPut(
                                                    self.get_proxy_response(),
                                                ))),
                                                MessageToUser::from(UserOperation::OriginatingTransactionIDMessage(
                                                    origin.clone(),
                                                )),
                                            ];
                                            finished.filestore_response.iter().for_each(|res| {
                                                message_to_user.push(MessageToUser::from(UserOperation::Response(
                                                    UserResponse::ProxyFileStore(res.clone()),
                                                )))
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
                                        val @ SegmentRequestForm {
                                            start_offset: start,
                                            end_offset: end,
                                        } if start == end => vec![val],
                                        SegmentRequestForm {
                                            start_offset: start,
                                            end_offset: end,
                                        } => (start..end)
                                            .step_by(self.config.file_size_segment.into())
                                            .map(|num| {
                                                if num
                                                    < end.saturating_sub(
                                                        self.config.file_size_segment.into(),
                                                    )
                                                {
                                                    SegmentRequestForm {
                                                        start_offset: num,
                                                        end_offset: num
                                                            + self.config.file_size_segment as u64,
                                                    }
                                                } else {
                                                    SegmentRequestForm {
                                                        start_offset: num,
                                                        end_offset: end,
                                                    }
                                                }
                                            })
                                            .collect::<Vec<SegmentRequestForm>>(),
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
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode,
                                    format!("ACK {:?}", ack.directive),
                                ))
                            }
                        }
                        Operations::KeepAlive(keepalive) => {
                            self.received_file_size = keepalive.progress;
                            Ok(())
                        }
                    },
                }
            }
            TransmissionMode::Unacknowledged => match payload {
                PDUPayload::FileData(_data) => Err(TransactionError::UnexpectedPDU(
                    self.config.sequence_number,
                    self.config.transmission_mode,
                    "File Data".to_owned(),
                )),
                PDUPayload::Directive(operation) => match operation {
                    Operations::EoF(_eof) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "End of File".to_owned(),
                    )),
                    Operations::Metadata(_metadata) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "Metadata PDU".to_owned(),
                    )),
                    Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "Prompt PDU".to_owned(),
                    )),
                    Operations::Ack(_ack) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "ACK PDU".to_owned(),
                    )),
                    Operations::KeepAlive(_keepalive) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "Keep Alive PDU".to_owned(),
                    )),
                    Operations::Finished(finished) => match self.metadata.closure_requested {
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
                        false => Err(TransactionError::UnexpectedPDU(
                            self.config.sequence_number,
                            self.config.transmission_mode,
                            "Prompt PDU".to_owned(),
                        )),
                    },
                    Operations::Nak(_) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "NAK PDU".to_owned(),
                    )),
                },
            },
        }
    }

    fn send_metadata(&mut self) -> TransactionResult<()> {
        self.timer.restart_inactivity();

        let destination = self.config.destination_entity_id;
        let metadata = MetadataPDU {
            closure_requested: self.metadata.closure_requested,
            checksum_type: self.metadata.checksum_type.clone(),
            file_size: self.metadata.file_size,
            source_filename: self.metadata.source_filename.as_str().as_bytes().to_vec(),
            destination_filename: self
                .metadata
                .destination_filename
                .as_str()
                .as_bytes()
                .to_vec(),
            options: self
                .metadata
                .filestore_requests
                .iter()
                .map(|fs| MetadataTLV::FileStoreRequest(fs.clone()))
                .chain(
                    self.metadata
                        .message_to_user
                        .iter()
                        .map(|msg| MetadataTLV::MessageToUser(msg.clone())),
                )
                .collect(),
        };
        let payload = PDUPayload::Directive(Operations::Metadata(metadata));
        let payload_len: u16 = payload
            .clone()
            .encode(self.config.file_size_flag)
            .len()
            .try_into()?;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            // TODO add semgentation Control ability
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        self.transport_tx.send((destination, pdu))?;
        debug!("Transaction {0:?} sending Metadata.", self.id());
        Ok(())
    }

    pub(crate) fn start(&mut self) -> TransactionResult<()> {
        self.send_metadata()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        assert_err,
        filestore::{ChecksumType, NativeFileStore},
        pdu::{
            CRCFlag, FileSizeFlag, FileStoreResponse, FileStoreStatus, Finished, KeepAlivePDU,
            NakOrKeepAlive, NegativeAcknowledgmentPDU, PromptPDU, RenameStatus, SegmentedData,
            UnsegmentedFileData,
        },
    };

    use super::super::config::test::default_config;
    use super::*;

    use camino::{Utf8Path, Utf8PathBuf};
    use crossbeam_channel::unbounded;
    use rstest::{fixture, rstest};
    use std::thread;
    use tempfile::TempDir;
    #[fixture]
    #[once]
    fn tempdir_fixture() -> TempDir {
        TempDir::new().unwrap()
    }

    #[rstest]
    fn header(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let metadata = test_metadata(10, Utf8PathBuf::from(""));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);
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
            source_entity_id: default_config.source_entity_id,
            destination_entity_id: default_config.destination_entity_id,
            transaction_sequence_number: default_config.sequence_number,
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
    fn test_if_file_transfer(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let metadata = test_metadata(600_u64, Utf8PathBuf::from("a"));
        let transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        assert_eq!(
            TransactionStatus::Undefined,
            transaction.get_status().clone()
        );

        assert!(transaction.is_file_transfer())
    }

    #[rstest]
    fn send_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let path = Utf8PathBuf::from("testfile");

        let metadata = test_metadata(10, path.clone());
        let mut transaction =
            SendTransaction::new(config, metadata, filestore.clone(), transport_tx);

        let input = vec![0, 5, 255, 99];

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 6,
            file_data: input.clone(),
        }));
        let payload_len = payload
            .clone()
            .encode(transaction.config.file_size_flag)
            .len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            let fname = transaction.metadata.source_filename.clone();

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

            let offset = 6;
            let length = 4;

            transaction
                .send_file_segment(Some(offset), Some(length as u16))
                .unwrap();
        });
        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        let expected_id = default_config.destination_entity_id;
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    #[case(SegmentRequestForm { start_offset: 6, end_offset: 10 })]
    #[case(SegmentRequestForm { start_offset: 0, end_offset: 0 })]
    fn send_missing(
        #[case] nak: SegmentRequestForm,
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let path = Utf8PathBuf::from("testfile_missing");
        let metadata = test_metadata(10, path.clone());

        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let input = vec![0, 5, 255, 99];
        let pdu = match &nak {
            SegmentRequestForm {
                start_offset: 6,
                end_offset: 10,
            } => {
                let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
                    offset: 6,
                    file_data: input.clone(),
                }));
                let payload_len = payload
                    .clone()
                    .encode(transaction.config.file_size_flag)
                    .len();

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
                    file_size: 10,
                    checksum_type: ChecksumType::Modular,
                    source_filename: path.clone().as_str().as_bytes().to_vec(),
                    destination_filename: path.clone().as_str().as_bytes().to_vec(),
                    options: vec![],
                }));
                let payload_len = payload
                    .clone()
                    .encode(transaction.config.file_size_flag)
                    .len();

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
            let fname = transaction.metadata.source_filename.clone();

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
        let expected_id = default_config.destination_entity_id;
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn checksum_cache(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let metadata = test_metadata(10, Utf8PathBuf::from(""));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let result = transaction
            .get_checksum()
            .expect("Cannot get file checksum");
        assert_eq!(0, result);
        assert!(transaction.checksum.is_some());
        let result = transaction
            .get_checksum()
            .expect("Cannot get file checksum");
        assert_eq!(0, result);
    }

    #[rstest]
    fn send_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let input = "Here is some test data to write!$*#*.\n";

        let path = Utf8PathBuf::from("test_eof.dat");
        let metadata = test_metadata(input.as_bytes().len() as u64, path.clone());
        let mut transaction =
            SendTransaction::new(config, metadata, filestore.clone(), transport_tx);

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
            file_size: input.as_bytes().len() as u64,
            fault_location: None,
        }));
        let payload_len = payload
            .clone()
            .encode(transaction.config.file_size_flag)
            .len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            let fname = transaction.metadata.source_filename.clone();

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
        let expected_id = default_config.destination_entity_id;

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
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let input = "Here is some test data to write!$*#*.\n";

        let path = Utf8PathBuf::from(format!("test_eof_{:}.dat", config.transmission_mode as u8));
        let metadata = test_metadata(input.as_bytes().len() as u64, path.clone());
        let mut transaction =
            SendTransaction::new(config.clone(), metadata, filestore.clone(), transport_tx);

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
            file_size: input.as_bytes().len() as u64,
            fault_location: Some(config.source_entity_id),
        }));
        let payload_len = payload.clone().encode(config.file_size_flag).len();

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            let fname = transaction.metadata.source_filename.clone();

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
        let expected_id = default_config.destination_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);

        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    fn suspend(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let metadata = test_metadata(0, Utf8PathBuf::from(""));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

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
        assert_eq!(TransactionState::Suspended, transaction.state)
    }

    #[rstest]
    fn resume(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let metadata = test_metadata(0, Utf8PathBuf::from(""));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

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

        transaction.resume();
        assert_eq!(TransactionState::Active, transaction.state);

        assert!(transaction.timer.inactivity.is_ticking());
        assert!(!transaction.timer.ack.is_ticking());
        assert!(!transaction.timer.nak.is_ticking());

        transaction.waiting_on = WaitingOn::AckEof;
        transaction.resume();
        assert!(transaction.timer.ack.is_ticking());
    }

    #[rstest]
    fn send_ack_fin(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let path = Utf8PathBuf::from(format!("test_eof_{:}.dat", config.transmission_mode as u8));
        let input = "Here is some test data to write!$*#*.\n";
        let metadata = test_metadata(input.as_bytes().len() as u64, path);
        let mut transaction =
            SendTransaction::new(config.clone(), metadata, filestore, transport_tx);

        let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
            directive: PDUDirective::Finished,
            directive_subtype_code: ACKSubDirective::Finished,
            condition: Condition::NoError,
            transaction_status: transaction.status.clone(),
        }));

        let payload_len = payload.clone().encode(config.file_size_flag).len();

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
        let expected_id = default_config.destination_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn pdu_error_unack_send(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::EoF(EndOfFile{
                condition: Condition::CancelReceived,
                checksum: 12_u32,
                file_size: 1022_u64,
                fault_location: None,
            }),
            Operations::Metadata(MetadataPDU{
                closure_requested: false,
                checksum_type: ChecksumType::Modular,
                file_size: 1022_u64,
                source_filename: "test_filename".into(),
                destination_filename: "test_filename".into(),
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
                progress: 12_u64
            }),
            Operations::Finished(Finished{
                condition: Condition::CheckLimitReached,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![FileStoreResponse {
                    action_and_status: FileStoreStatus::RenameFile(
                        RenameStatus::NewFilenameAlreadyExists,
                    ),
                    first_filename: "/path/to/a/file".into(),
                    second_filename: "/path/to/a/new/file".into(),
                    filestore_message: vec![1_u8, 3, 58],
                }],
                fault_location: None,

            }),
            Operations::Nak(NegativeAcknowledgmentPDU{
                start_of_scope: 0_u64,
                end_of_scope: 1022_u64,
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            }),
        )]
        operation: Operations,
    ) {
        let (transport_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload
            .clone()
            .encode(transaction.config.file_size_flag)
            .len() as u16;
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
            Err(TransactionError::UnexpectedPDU(
                _,
                TransmissionMode::Unacknowledged,
                _
            ))
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
                file_size: 1022_u64,
                fault_location: None,
            }),
            Operations::Metadata(MetadataPDU{
                closure_requested: false,
                checksum_type: ChecksumType::Modular,
                file_size: 1022_u64,
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
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload
            .clone()
            .encode(transaction.config.file_size_flag)
            .len() as u16;
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
            Err(TransactionError::UnexpectedPDU(
                _,
                TransmissionMode::Acknowledged,
                _
            ))
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
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 12_u64,
            file_data: (0..12_u8).collect::<Vec<u8>>(),
        }));
        let payload_len = payload
            .clone()
            .encode(transaction.config.file_size_flag)
            .len() as u16;
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileData,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(result, Err(TransactionError::UnexpectedPDU(_, _, _)))
    }
    #[rstest]
    fn recv_finished(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.destination_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let path = Utf8PathBuf::from("test_file");
        let metadata = test_metadata(600, path);
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let finished_pdu = {
            let payload = PDUPayload::Directive(Operations::Finished(Finished {
                condition: Condition::NoError,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![],
                fault_location: None,
            }));
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;

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
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;

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
        #[values(2_u64, u32::MAX as u64 + 100_u64)] total_size: u64,
    ) {
        let file_size_flag = match total_size > u32::MAX.into() {
            true => FileSizeFlag::Large,
            false => FileSizeFlag::Small,
        };

        let (transport_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;
        config.file_size_flag = file_size_flag;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let metadata = test_metadata(total_size, Utf8PathBuf::from("test_file"));
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        let nak_pdu = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: total_size,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 0,
                    end_offset: total_size,
                }],
            }));
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        let expected_naks: VecDeque<SegmentRequestForm> = (0..total_size)
            .step_by(transaction.config.file_size_segment.into())
            .map(|num| {
                if num < total_size.saturating_sub(transaction.config.file_size_segment.into()) {
                    SegmentRequestForm {
                        start_offset: num,
                        end_offset: num + transaction.config.file_size_segment as u64,
                    }
                } else {
                    SegmentRequestForm {
                        start_offset: num,
                        end_offset: total_size,
                    }
                }
            })
            .collect();

        transaction.process_pdu(nak_pdu).unwrap();
        assert_eq!(expected_naks, transaction.naks);
    }

    #[rstest]
    fn recv_ack_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.destination_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let path = Utf8PathBuf::from("test_file");

        let metadata = Metadata {
            closure_requested: false,
            file_size: 500,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Null,
        };
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);

        transaction.checksum = Some(0);

        let ack_pdu = {
            let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                condition: Condition::NoError,
                directive: PDUDirective::EoF,
                directive_subtype_code: ACKSubDirective::Other,
                transaction_status: TransactionStatus::Undefined,
            }));
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;

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
                file_size: 500,
                fault_location: None,
            }));
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;

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
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let path = Utf8PathBuf::from("test_file");
        let metadata = Metadata {
            closure_requested: false,
            file_size: 500,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Null,
        };
        let mut transaction = SendTransaction::new(config, metadata, filestore, transport_tx);
        transaction.checksum = Some(0);

        let keep_alive = {
            let payload =
                PDUPayload::Directive(Operations::KeepAlive(KeepAlivePDU { progress: 300 }));
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        assert_eq!(0, transaction.received_file_size);
        transaction.process_pdu(keep_alive).unwrap();
        assert_eq!(300, transaction.received_file_size)
    }

    fn test_metadata(file_size: u64, path: Utf8PathBuf) -> Metadata {
        Metadata {
            closure_requested: false,
            file_size,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        }
    }
}
