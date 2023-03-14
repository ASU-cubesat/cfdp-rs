use std::{
    collections::{HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    sync::Arc,
    time::Duration,
};

use crossbeam_channel::Sender;
use log::{debug, info};

use super::{
    config::{Metadata, TransactionConfig, TransactionState},
    error::{TransactionError, TransactionResult},
    TransactionID,
};
use crate::{
    daemon::{FinishedIndication, Indication, Report, ResumeIndication, SuspendIndication},
    filestore::{FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, Condition, DeliveryCode, Direction, EndOfFile, FaultHandlerAction,
        FileDataPDU, FileStatusCode, MetadataPDU, MetadataTLV, Operations, PDUDirective, PDUHeader,
        PDUPayload, PDUType, PositiveAcknowledgePDU, SegmentRequestForm, SegmentationControl,
        SegmentedData, TransactionStatus, TransmissionMode, UnsegmentedFileData, VariableID, PDU,
        U3,
    },
    timer::Timer,
};

#[derive(PartialEq, Debug)]
enum SendState {
    // initial state
    // send the metadata and then go to SendData (if file transfer) or directly to SendEof
    SendMetadata,
    // send data until finished then go to SendEof
    SendData,
    // send EOF and wait for ack, then go to Finished
    SendEof,
    // send EOF and wait for ack, then go to Finished
    Cancelled,
    // wait for Finished and send ack
    Finished,
}

pub struct SendTransaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<T>,
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
    /// Timer used to track if the Nak limit has been reached
    /// inactivity has occurred
    /// or the ACK limit is reached
    timer: Timer,
    //. checksum cache to reduce I/0
    //. doubles as stored checksum in received mode
    checksum: Option<u32>,
    /// The current general state of the transaction.
    /// Used to determine when the thread should be killed
    state: TransactionState,
    /// The detailed sub-state valid when state = TransactionState::Active
    /// Used to control the state machine
    send_state: SendState,
    /// EoF prepared to be sent at the next opportunity if the bool is true
    eof: Option<(EndOfFile, bool)>,
    ack: Option<PositiveAcknowledgePDU>,
    /// Channel for Indications to propagate back up
    indication_tx: Sender<Indication>,
    /// flag to track if the initial EoFSent Indication has been sent.
    /// This indication only needs to be delivered for the initial EoF transmission
    send_eof_indication: bool,
}
impl<T: FileStore> SendTransaction<T> {
    /// Start a new SendTransaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore)
    pub fn new(
        // Configuration of this Transaction.
        config: TransactionConfig,
        // Metadata of this Transaction
        metadata: Metadata,
        // Connection to the local FileStore implementation.
        filestore: Arc<T>,
        // Sender channel used to propagate Message To User back up to the Daemon Thread.
        indication_tx: Sender<Indication>,
    ) -> TransactionResult<Self> {
        let received_file_size = 0_u64;
        let timer = Timer::new(
            config.inactivity_timeout,
            config.max_count,
            config.ack_timeout,
            config.max_count,
            config.nak_timeout,
            config.max_count,
        );

        let me = Self {
            status: TransactionStatus::Undefined,
            config,
            filestore,
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
            send_state: SendState::SendMetadata,
            state: TransactionState::Active,
            eof: None,
            ack: None,
            indication_tx,
            send_eof_indication: true,
        };
        me.indication_tx.send(Indication::Transaction(me.id()))?;
        Ok(me)
    }

    pub(crate) fn has_pdu_to_send(&self) -> bool {
        match self.send_state {
            SendState::SendMetadata | SendState::SendData => true,
            SendState::SendEof => !self.naks.is_empty() || self.eof.as_ref().map_or(false, |x| x.1),
            SendState::Cancelled => self.eof.as_ref().map_or(false, |x| x.1),
            SendState::Finished => self.ack.is_some(),
        }
    }

    // returns the time until the first timeout (inactivity or ack)
    pub(crate) fn until_timeout(&self) -> Duration {
        match self.send_state {
            SendState::SendEof | SendState::Cancelled => self.timer.until_timeout(),
            _ => Duration::MAX,
        }
    }

    pub(crate) fn send_pdu(
        &mut self,
        transport_tx: &Sender<(VariableID, PDU)>,
    ) -> TransactionResult<()> {
        match self.send_state {
            SendState::SendMetadata => {
                self.send_metadata(transport_tx)?;
                if self.is_file_transfer() {
                    self.send_state = SendState::SendData;
                } else {
                    self.prepare_eof(None)?;
                    self.send_state = SendState::SendEof;
                }
            }
            SendState::SendData => {
                while !transport_tx.is_full() {
                    if !self.naks.is_empty() {
                        // if we have received a NAK send the missing data
                        self.send_missing_data(transport_tx)?;
                    } else {
                        self.send_file_segment(None, None, transport_tx)?
                    }

                    let handle = self.get_handle()?;
                    if handle.stream_position().map_err(FileStoreError::IO)?
                        == handle.metadata().map_err(FileStoreError::IO)?.len()
                    {
                        self.prepare_eof(None)?;
                        self.send_state = SendState::SendEof;
                        break;
                    }
                }
            }
            SendState::SendEof => {
                if !self.naks.is_empty() {
                    // if we have received a NAK send the missing data
                    self.send_missing_data(transport_tx)?;
                } else {
                    self.send_eof(transport_tx)?;

                    // EoFSent indication only needs to be sent for the initial EoF transmission
                    if self.send_eof_indication {
                        self.indication_tx.send(Indication::EoFSent(self.id()))?;
                        self.send_eof_indication = false;
                    }

                    if self.get_mode() == TransmissionMode::Unacknowledged {
                        // if closure is not requested, this transaction is finished.
                        // indicate as much to the User
                        if !self.metadata.closure_requested {
                            self.indication_tx
                                .send(Indication::Finished(FinishedIndication {
                                    id: self.id(),
                                    report: self.generate_report(),
                                    filestore_responses: vec![],
                                    file_status: self.file_status,
                                    delivery_code: self.delivery_code,
                                }))?;
                        }
                        self.shutdown();
                    }
                }
            }
            SendState::Cancelled => {
                self.send_eof(transport_tx)?;
            }
            SendState::Finished => {
                self.send_ack(transport_tx)?;
            }
        }
        Ok(())
    }

    pub fn handle_timeout(&mut self) -> TransactionResult<()> {
        match self.send_state {
            SendState::SendEof => {
                if self.timer.inactivity.limit_reached() {
                    self.handle_fault(Condition::InactivityDetected)?
                }
                if self.timer.ack.timeout_occured() {
                    if self.timer.ack.limit_reached() {
                        self.handle_fault(Condition::PositiveLimitReached)?
                    } else {
                        self.set_eof_flag(true);
                    }
                }
            }
            SendState::Cancelled => {
                if self.timer.inactivity.limit_reached() {
                    self.abandon();
                }
                if self.timer.ack.timeout_occured() {
                    if self.timer.ack.limit_reached() {
                        self.abandon();
                    } else {
                        self.set_eof_flag(true);
                    }
                }
            }
            _ => {}
        };

        Ok(())
    }

    pub fn get_status(&self) -> TransactionStatus {
        self.status
    }

    pub(crate) fn get_state(&self) -> TransactionState {
        self.state
    }

    pub fn get_mode(&self) -> TransmissionMode {
        self.config.transmission_mode
    }
    fn generate_report(&self) -> Report {
        Report {
            id: self.id(),
            state: self.get_state(),
            status: self.get_status(),
            condition: self.condition,
        }
    }

    pub fn send_report(&self, sender: Option<Sender<Report>>) -> TransactionResult<()> {
        let report = self.generate_report();
        if let Some(channel) = sender {
            channel.send(report.clone())?;
        }
        self.indication_tx.send(Indication::Report(report))?;

        Ok(())
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
                segment_metadata_flag: self.config.segment_metadata_flag,
                source_entity_id: self.config.source_entity_id,
                transaction_sequence_number: self.config.sequence_number,
                destination_entity_id: self.config.destination_entity_id,
            };
            self.header = Some(header.clone());
            header
        }
    }

    pub fn id(&self) -> TransactionID {
        TransactionID(self.config.source_entity_id, self.config.sequence_number)
    }

    fn get_checksum(&mut self) -> TransactionResult<u32> {
        match self.checksum {
            Some(val) => Ok(val),
            None => {
                let checksum = {
                    match self.is_file_transfer() {
                        true => {
                            let checksum_type = self.metadata.checksum_type;
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
            let mut buff = Vec::<u8>::with_capacity(length as usize);
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
        transport_tx: &Sender<(VariableID, PDU)>,
    ) -> TransactionResult<()> {
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

        let payload_len: u16 = payload.encoded_len(self.config.file_size_flag);

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len,
            segmentation_control,
        );
        let pdu = PDU { header, payload };

        transport_tx.send((destination, pdu))?;

        Ok(())
    }

    pub fn send_missing_data(
        &mut self,
        transport_tx: &Sender<(VariableID, PDU)>,
    ) -> TransactionResult<()> {
        match self.naks.pop_front() {
            Some(request) => {
                // only restart inactivity if we have something to do.
                self.timer.restart_inactivity();
                let (offset, length) = (
                    request.start_offset,
                    (request.end_offset - request.start_offset).try_into()?,
                );

                match offset == 0 && length == 0 {
                    true => self.send_metadata(transport_tx),
                    false => {
                        let current_pos = {
                            let handle = self.get_handle()?;
                            handle.stream_position().map_err(FileStoreError::IO)?
                        };

                        self.send_file_segment(Some(offset), Some(length), transport_tx)?;

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

    fn prepare_eof(&mut self, fault_location: Option<VariableID>) -> TransactionResult<()> {
        self.eof = Some((
            EndOfFile {
                condition: self.condition,
                checksum: self.get_checksum()?,
                file_size: self.metadata.file_size,
                fault_location,
            },
            true,
        ));

        Ok(())
    }

    pub fn send_eof(&mut self, transport_tx: &Sender<(VariableID, PDU)>) -> TransactionResult<()> {
        if let Some((eof, true)) = &self.eof {
            self.timer.restart_ack();

            let payload = PDUPayload::Directive(Operations::EoF(eof.clone()));
            let payload_len = payload.encoded_len(self.config.file_size_flag);
            let header = self.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            let destination = header.destination_entity_id;
            let pdu = PDU { header, payload };

            transport_tx.send((destination, pdu))?;
            debug!("Transaction {0} sent EndOfFile.", self.id());
            self.set_eof_flag(false);
        }
        Ok(())
    }

    // set the true flag on the eof such that it is sent at the next opportunity
    fn set_eof_flag(&mut self, flag: bool) {
        if let Some(x) = self.eof.as_mut() {
            x.1 = flag;
        }
    }

    pub fn abandon(&mut self) {
        debug!("Transaction {0} abandoning.", self.id());
        self.status = TransactionStatus::Terminated;
        self.shutdown();
    }

    pub fn shutdown(&mut self) {
        debug!("Transaction {0} shutting down.", self.id());
        self.state = TransactionState::Terminated;
        self.timer.ack.pause();
        self.timer.inactivity.pause();
    }

    pub fn cancel(&mut self) -> TransactionResult<()> {
        self._cancel(Condition::CancelReceived)
    }

    fn _cancel(&mut self, condition: Condition) -> TransactionResult<()> {
        self.timer.inactivity.pause();
        self.condition = condition;
        self.send_state = SendState::Cancelled;
        self.prepare_eof(Some(self.config.source_entity_id))
    }

    pub fn suspend(&mut self) -> TransactionResult<()> {
        self.timer.ack.pause();
        self.timer.inactivity.pause();
        self.state = TransactionState::Suspended;

        self.indication_tx
            .send(Indication::Suspended(SuspendIndication {
                id: self.id(),
                condition: self.condition,
            }))?;
        Ok(())
    }

    pub fn resume(&mut self) -> TransactionResult<()> {
        match self.send_state {
            SendState::SendEof | SendState::Cancelled => {
                self.timer.restart_ack();
                self.timer.restart_inactivity();
            }
            _ => {}
        }

        self.state = TransactionState::Active;

        // use the current location in the file as the progress
        let progress = match self.is_file_transfer() {
            true => {
                let handle = self.get_handle()?;
                handle.stream_position().map_err(FileStoreError::IO)?
            }
            false => 0,
        };

        self.indication_tx
            .send(Indication::Resumed(ResumeIndication {
                id: self.id(),
                progress,
            }))?;
        Ok(())
    }

    /// Take action according to the defined handler mapping.
    /// Returns a boolean indicating if the calling function should continue (true) or not (false.)
    fn handle_fault(&mut self, condition: Condition) -> TransactionResult<()> {
        self.condition = condition;
        let action = self
            .config
            .fault_handler_override
            .get(&self.condition)
            .unwrap_or(&FaultHandlerAction::Cancel);
        info!(
            "Transaction {:?} handling fault {:?}, action: {:?} ",
            self.id(),
            condition,
            action
        );

        match action {
            FaultHandlerAction::Ignore => {}
            FaultHandlerAction::Cancel => {
                self._cancel(condition)?;
            }

            FaultHandlerAction::Suspend => {
                self.suspend()?;
            }
            FaultHandlerAction::Abandon => {
                self.abandon();
            }
        }
        Ok(())
    }

    fn prepare_ack(&mut self) {
        self.ack.replace(PositiveAcknowledgePDU {
            directive: PDUDirective::Finished,
            directive_subtype_code: ACKSubDirective::Finished,
            condition: self.condition,
            transaction_status: self.status,
        });
    }

    fn send_ack(&mut self, transport_tx: &Sender<(VariableID, PDU)>) -> TransactionResult<()> {
        if let Some(ack) = self.ack.take() {
            let payload = PDUPayload::Directive(Operations::Ack(ack));
            let payload_len = payload.encoded_len(self.config.file_size_flag);

            let header = self.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            let destination = header.destination_entity_id;
            let pdu = PDU { header, payload };

            transport_tx.send((destination, pdu))?;
            debug!("Transaction {0} sent Ack(Finished).", self.id());
            self.shutdown();
        }
        Ok(())
    }

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        if self.send_state == SendState::SendEof {
            self.timer.restart_inactivity();
        }
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
                            self.file_status = finished.file_status;
                            self.prepare_ack();
                            self.send_state = SendState::Finished;
                            self.condition = finished.condition;
                            debug!(
                                "Transaction {} received Finished ({:?})",
                                self.id(),
                                self.condition
                            );

                            self.indication_tx
                                .send(Indication::Finished(FinishedIndication {
                                    id: self.id(),
                                    report: self.generate_report(),
                                    filestore_responses: finished.filestore_response,
                                    file_status: self.file_status,
                                    delivery_code: self.delivery_code,
                                }))?;

                            match self.condition != Condition::NoError {
                                true => {
                                    info!(
                                        "Transaction {:?}. Ended due to condition {:?}",
                                        self.id(),
                                        self.condition
                                    );
                                }
                                false => {}
                            }
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

                            self.indication_tx
                                .send(Indication::Finished(FinishedIndication {
                                    id: self.id(),
                                    report: self.generate_report(),
                                    filestore_responses: finished.filestore_response,
                                    file_status: self.file_status,
                                    delivery_code: self.delivery_code,
                                }))?;

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

    fn send_metadata(&mut self, transport_tx: &Sender<(VariableID, PDU)>) -> TransactionResult<()> {
        let destination = self.config.destination_entity_id;
        let metadata = MetadataPDU {
            closure_requested: self.metadata.closure_requested,
            checksum_type: self.metadata.checksum_type,
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
        let payload_len: u16 = payload.encoded_len(self.config.file_size_flag);

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            // TODO add semgentation Control ability
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        transport_tx.send((destination, pdu))?;
        debug!("Transaction {0} sent Metadata.", self.id());
        Ok(())
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

    use std::io::Write;

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
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(10, Utf8PathBuf::from(""));
        let mut transaction = SendTransaction::new(config, metadata, filestore, indication_tx)
            .expect("unable to start transaction.");

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
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(600_u64, Utf8PathBuf::from("a"));
        let transaction = SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

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

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(10, path.clone());
        let mut transaction =
            SendTransaction::new(config, metadata, filestore.clone(), indication_tx).unwrap();

        let input = vec![0, 5, 255, 99];

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 6,
            file_data: input.clone(),
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len,
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
                .send_file_segment(Some(offset), Some(length as u16), &transport_tx)
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

        let (indication_tx, _indication_rx) = unbounded();
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

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
                let payload_len = payload.encoded_len(transaction.config.file_size_flag);

                let header = transaction.get_header(
                    Direction::ToReceiver,
                    PDUType::FileData,
                    payload_len,
                    SegmentationControl::NotPreserved,
                );
                PDU { header, payload }
            }
            _ => {
                let payload = PDUPayload::Directive(Operations::Metadata(MetadataPDU {
                    closure_requested: false,
                    file_size: 10,
                    checksum_type: ChecksumType::Modular,
                    source_filename: path.as_str().as_bytes().to_vec(),
                    destination_filename: path.as_str().as_bytes().to_vec(),
                    options: vec![],
                }));
                let payload_len = payload.encoded_len(transaction.config.file_size_flag);

                let header = transaction.get_header(
                    Direction::ToReceiver,
                    PDUType::FileDirective,
                    payload_len,
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
            transaction.send_missing_data(&transport_tx).unwrap();

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
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(10, Utf8PathBuf::from(""));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

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

        let (indication_tx, _indication_rx) = unbounded();
        let path = Utf8PathBuf::from("test_eof.dat");
        let metadata = test_metadata(input.as_bytes().len() as u64, path.clone());
        let mut transaction =
            SendTransaction::new(config, metadata, filestore.clone(), indication_tx).unwrap();

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
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
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
            transaction.prepare_eof(None).unwrap();
            transaction.send_eof(&transport_tx).unwrap()
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

        let (indication_tx, _indication_rx) = unbounded();
        let path = Utf8PathBuf::from(format!("test_eof_{:}.dat", config.transmission_mode as u8));
        let metadata = test_metadata(input.as_bytes().len() as u64, path.clone());
        let mut transaction =
            SendTransaction::new(config.clone(), metadata, filestore.clone(), indication_tx)
                .unwrap();

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
        let payload_len = payload.encoded_len(config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
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
            assert!(transaction.has_pdu_to_send());
            transaction.send_pdu(&transport_tx).unwrap();

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
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(0, Utf8PathBuf::from(""));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        transaction.timer.restart_ack();
        transaction.timer.restart_inactivity();
        transaction.timer.restart_nak();

        {
            let timers = [&transaction.timer.ack, &transaction.timer.inactivity];
            timers.iter().for_each(|timer| {
                assert!(timer.is_ticking());
            });
        }

        transaction.suspend().unwrap();

        let timers = [&transaction.timer.ack, &transaction.timer.inactivity];
        timers.iter().for_each(|timer| assert!(!timer.is_ticking()));
        assert_eq!(TransactionState::Suspended, transaction.state)
    }

    #[rstest]
    fn resume(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(0, Utf8PathBuf::from(""));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        transaction.timer.restart_ack();
        transaction.timer.restart_inactivity();

        {
            let timers = [&transaction.timer.ack, &transaction.timer.inactivity];
            timers.iter().for_each(|timer| {
                assert!(timer.is_ticking());
            });
        }

        transaction.suspend().unwrap();

        let timers = [&transaction.timer.ack, &transaction.timer.inactivity];
        timers.iter().for_each(|timer| assert!(!timer.is_ticking()));

        transaction.resume().unwrap();
        assert_eq!(TransactionState::Active, transaction.state);

        assert!(!transaction.timer.ack.is_ticking());

        transaction.send_state = SendState::SendEof;
        transaction.resume().unwrap();
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

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(input.as_bytes().len() as u64, path);
        let mut transaction =
            SendTransaction::new(config.clone(), metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
            directive: PDUDirective::Finished,
            directive_subtype_code: ACKSubDirective::Finished,
            condition: Condition::NoError,
            transaction_status: transaction.status,
        }));

        let payload_len = payload.encoded_len(config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        thread::spawn(move || {
            transaction.prepare_ack();
            transaction.send_ack(&transport_tx).unwrap();
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
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
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
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
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
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 12_u64,
            file_data: (0..12_u8).collect::<Vec<u8>>(),
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
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

        let (indication_tx, _indication_rx) = unbounded();
        let path = Utf8PathBuf::from("test_file");
        let metadata = test_metadata(600, path);
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let finished_pdu = {
            let payload = PDUPayload::Directive(Operations::Finished(Finished {
                condition: Condition::NoError,
                delivery_code: DeliveryCode::Complete,
                file_status: FileStatusCode::Retained,
                filestore_response: vec![],
                fault_location: None,
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
            assert!(transaction.has_pdu_to_send());
            transaction.send_pdu(&transport_tx).unwrap();
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

        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;
        config.file_size_flag = file_size_flag;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = unbounded();
        let metadata = test_metadata(total_size, Utf8PathBuf::from("test_file"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let nak_pdu = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: total_size,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 0,
                    end_offset: total_size,
                }],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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

        let (indication_tx, _indication_rx) = unbounded();
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        transaction.checksum = Some(0);

        let ack_pdu = {
            let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                condition: Condition::NoError,
                directive: PDUDirective::EoF,
                directive_subtype_code: ACKSubDirective::Other,
                transaction_status: TransactionStatus::Undefined,
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };
        thread::spawn(move || {
            transaction.prepare_eof(None).unwrap();
            transaction.send_eof(&transport_tx).unwrap();
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

        let (indication_tx, _indication_rx) = unbounded();
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();
        transaction.checksum = Some(0);

        let keep_alive = {
            let payload =
                PDUPayload::Directive(Operations::KeepAlive(KeepAlivePDU { progress: 300 }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
