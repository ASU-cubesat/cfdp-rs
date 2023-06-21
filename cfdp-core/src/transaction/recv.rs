use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    sync::Arc,
    time::Duration,
};

use camino::Utf8PathBuf;
use log::{debug, warn};
use tokio::sync::{
    mpsc::{Permit, Sender},
    oneshot,
};

use super::{
    config::{Metadata, TransactionConfig, TransactionState},
    error::{TransactionError, TransactionResult},
    TransactionID,
};
use crate::{
    daemon::{
        FileSegmentIndication, FinishedIndication, Indication, MetadataRecvIndication,
        NakProcedure, Report, ResumeIndication, SuspendIndication,
    },
    filestore::{FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, Condition, DeliveryCode, Direction, FaultHandlerAction, FileDataPDU,
        FileStatusCode, FileStoreResponse, Finished, KeepAlivePDU, MetadataTLV, NakOrKeepAlive,
        NegativeAcknowledgmentPDU, Operations, PDUDirective, PDUHeader, PDUPayload, PDUType,
        PositiveAcknowledgePDU, PromptPDU, SegmentRequestForm, SegmentationControl,
        TransactionStatus, TransmissionMode, VariableID, PDU, U3,
    },
    segments,
    timer::Timer,
};

#[derive(PartialEq, Debug)]
enum RecvState {
    // initial state
    // received data, missing data and EOF
    ReceiveData,
    // send Finished and wait for ack
    Finished,
    // send Finished and wait for ack
    Cancelled,
}

pub struct RecvTransaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<T>,
    /// Channel for Indications to propagate back up
    indication_tx: Sender<Indication>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// A sorted list of contiguous (start offset, end offset) non overlapping received segments to monitor progress and detect NAKs.
    saved_segments: Vec<(u64, u64)>,
    /// when to send NAKs - immediately after detection or after EOF
    nak_procedure: NakProcedure,
    /// Flag to check if metadata on the file has been received
    pub(crate) metadata: Option<Metadata>,
    /// Measurement of how much of the file has been received so far
    received_file_size: u64,
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
    // The current state of the transaction.
    // Used to determine when the thread should be killed
    state: TransactionState,
    // recv sub-state, applicable when state = Active
    recv_state: RecvState,
    // File size received in the EOF, used also as an indication that EOF has been received
    file_size: Option<u64>,
    // EOF ack prepared to be sent at the next opportunity
    ack: Option<PositiveAcknowledgePDU>,
    // Finished PDU prepared to be sent at the next opportunity, if the bool is true
    finished: Option<(Finished, bool)>,
    // Prompt PDU to be processed at the next opportunity
    prompt: Option<PromptPDU>,
    // the list of gaps to include in the next NAK
    naks: VecDeque<SegmentRequestForm>,
    // the received_file_size measured when the previous nak has been sent
    nak_received_file_size: u64,
}

impl<T: FileStore> RecvTransaction<T> {
    /// Start a new SendTransaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore).
    ///
    /// The [NakProcedure] is most likely passed from [EntityConfig](crate::daemon::EntityConfig)
    pub fn new(
        // Configuation of this Transaction.
        config: TransactionConfig,
        // Desired NAK procedure. This is most likely passed from an EntityConfig
        nak_procedure: NakProcedure,
        // Connection to the local FileStore implementation.
        filestore: Arc<T>,
        // Sender channel used to propagate Message To User back up to the Daemon Thread.
        indication_tx: Sender<Indication>,
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
            indication_tx,
            file_handle: None,
            saved_segments: Vec::new(),
            nak_procedure,
            metadata: None,
            received_file_size,
            header: None,
            condition: Condition::NoError,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
            filestore_response: Vec::new(),
            timer,
            checksum: None,
            state: TransactionState::Active,
            recv_state: RecvState::ReceiveData,
            ack: None,
            finished: None,
            file_size: None,
            prompt: None,
            naks: VecDeque::new(),
            nak_received_file_size: received_file_size,
        };
        transaction.timer.restart_inactivity();
        transaction
    }

    pub(crate) fn has_pdu_to_send(&self) -> bool {
        match self.recv_state {
            RecvState::ReceiveData => {
                self.ack.is_some() || self.prompt.is_some() || !self.naks.is_empty()
            }
            RecvState::Finished | RecvState::Cancelled => {
                self.finished.as_ref().map_or(false, |x| x.1)
            }
        }
    }

    // returns the time until the first timeout (inactivity or ack)
    pub(crate) fn until_timeout(&self) -> Duration {
        self.timer.until_timeout()
    }

    pub(crate) fn send_pdu(&mut self, permit: Permit<(VariableID, PDU)>) -> TransactionResult<()> {
        if self.prompt.is_some() {
            self.answer_prompt(permit)?;
        } else {
            match self.recv_state {
                RecvState::ReceiveData => {
                    if self.ack.is_some() {
                        self.send_ack_eof(permit)?;
                    } else if !self.naks.is_empty() {
                        self.send_naks(permit)?;
                    }
                }
                RecvState::Finished | RecvState::Cancelled => {
                    if self.ack.is_some() {
                        self.send_ack_eof(permit)?;
                    } else if self.finished.as_ref().map_or(false, |x| x.1) {
                        self.send_finished(permit)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn handle_timeout(&mut self) -> TransactionResult<()> {
        if self.timer.inactivity.limit_reached() {
            if self.recv_state == RecvState::Cancelled {
                warn!("Transaction {} inactivity timeout limit reached in Cancelled state, abandoning", self.id());
                self.abandon();
            } else {
                self.handle_fault(Condition::InactivityDetected)?;
            }
        } else if self.timer.inactivity.timeout_occured() {
            self.timer.restart_inactivity();
        }

        match self.recv_state {
            RecvState::ReceiveData => {
                if self.timer.nak.timeout_occured() {
                    self.naks = self.get_all_naks();
                }
            }
            RecvState::Finished => {
                if self.timer.ack.limit_reached() {
                    self.handle_fault(Condition::PositiveLimitReached)?;
                } else if self.timer.ack.timeout_occured() {
                    self.set_finished_flag(true);
                    self.timer.restart_ack();
                }
            }
            RecvState::Cancelled => {
                if self.timer.ack.limit_reached() {
                    warn!(
                        "Transaction {} ACK timeout limit reached in Cancelled state, abandoning",
                        self.id()
                    );
                    self.abandon();
                } else if self.timer.ack.timeout_occured() {
                    self.set_finished_flag(true);
                    self.timer.restart_ack();
                }
            }
        }

        Ok(())
    }

    fn answer_prompt(&mut self, permit: Permit<(VariableID, PDU)>) -> TransactionResult<()> {
        if let Some(prompt) = self.prompt.take() {
            match prompt.nak_or_keep_alive {
                NakOrKeepAlive::Nak => {
                    self.naks = self.get_all_naks();
                    self.send_naks(permit)?;
                }
                NakOrKeepAlive::KeepAlive => {
                    let progress = self.get_progress();
                    let data = KeepAlivePDU { progress };

                    let payload = PDUPayload::Directive(Operations::KeepAlive(data));
                    let payload_len = payload.encoded_len(self.config.file_size_flag);

                    let header = self.get_header(
                        Direction::ToSender,
                        PDUType::FileDirective,
                        payload_len,
                        SegmentationControl::NotPreserved,
                    );

                    let destination = header.source_entity_id;

                    let pdu = PDU { header, payload };
                    permit.send((destination, pdu));
                }
            }
        }

        Ok(())
    }
    // return true if:
    // - metadata has not been received
    // - there is a gap in the data received
    // - EOF has been received and either no data has been received
    //   or there is a gap between the last segment received and the end of file according to the file_size received in the EOF
    fn has_naks(&self) -> bool {
        self.metadata.is_none() || {
            if let Some(file_size) = self.file_size {
                //eof received
                self.saved_segments.len() != 1 || self.saved_segments[0].1 != file_size
            } else {
                //eof not received
                self.saved_segments.len() > 1
            }
        }
    }

    fn eof_received(&self) -> bool {
        self.file_size.is_some()
    }

    pub fn get_status(&self) -> TransactionStatus {
        self.status
    }

    pub(crate) fn get_state(&self) -> TransactionState {
        self.state
    }

    fn generate_report(&self) -> Report {
        Report {
            id: self.id(),
            state: self.get_state(),
            status: self.get_status(),
            condition: self.condition,
        }
    }

    pub fn send_report(&self, sender: Option<oneshot::Sender<Report>>) -> TransactionResult<()> {
        let report = self.generate_report();
        if let Some(channel) = sender {
            let _ = channel.send(report.clone());
        }
        self.send_indication(Indication::Report(report));

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
    fn initialize_tempfile(&mut self) -> TransactionResult<()> {
        self.file_handle = Some(self.filestore.open_tempfile()?);
        Ok(())
    }

    fn get_handle(&mut self) -> TransactionResult<&mut File> {
        let id = self.id();
        if self.file_handle.is_none() {
            self.initialize_tempfile()?
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

    fn store_file_data(&mut self, pdu: FileDataPDU) -> TransactionResult<(u64, usize)> {
        let (offset, file_data) = match pdu {
            FileDataPDU::Segmented(data) => (data.offset, data.file_data),
            FileDataPDU::Unsegmented(data) => (data.offset, data.file_data),
        };

        let length = file_data.len();

        if length > 0 {
            let handle = self.get_handle()?;
            handle
                .seek(SeekFrom::Start(offset))
                .map_err(FileStoreError::IO)?;
            handle
                .write_all(file_data.as_slice())
                .map_err(FileStoreError::IO)?;
            let new_data_received = segments::update_segments(
                &mut self.saved_segments,
                (offset, offset + file_data.len() as u64),
            );
            self.received_file_size += new_data_received;
        } else {
            warn!(
                "Received FileDataPDU with invalid file_data.length = {}; ignored",
                length
            );
        }

        Ok((offset, length))
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

    pub fn abandon(&mut self) {
        debug!("Transaction {0} abandoning.", self.id());
        self.status = TransactionStatus::Terminated;
        self.shutdown();
    }

    pub fn shutdown(&mut self) {
        debug!("Transaction {0} shutting down.", self.id());

        self.state = TransactionState::Terminated;
        self.timer.ack.pause();
        self.timer.nak.pause();
        self.timer.inactivity.pause();
    }

    pub fn cancel(&mut self) -> TransactionResult<()> {
        debug!("Transaction {0} canceling.", self.id());
        self.condition = Condition::CancelReceived;
        self._cancel();
        Ok(())
    }

    fn _cancel(&mut self) {
        self.recv_state = RecvState::Cancelled;
        self.timer.nak.pause();

        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => self.prepare_finished(None),
            TransmissionMode::Unacknowledged => {
                if self
                    .metadata
                    .as_ref()
                    .map(|meta| meta.closure_requested)
                    .unwrap_or(false)
                {
                    // need to check on fault_location
                    self.prepare_finished(None);
                }
                self.shutdown();
            }
        }
        // make some kind of log/indication
    }

    pub fn suspend(&mut self) -> TransactionResult<()> {
        self.timer.ack.pause();
        self.timer.nak.pause();
        self.timer.inactivity.pause();
        self.state = TransactionState::Suspended;

        self.send_indication(Indication::Suspended(SuspendIndication {
            id: self.id(),
            condition: self.condition,
        }));

        Ok(())
    }

    pub fn resume(&mut self) -> TransactionResult<()> {
        self.timer.reset_inactivity();
        match self.recv_state {
            RecvState::ReceiveData => {
                if self.nak_procedure == NakProcedure::Immediate || self.eof_received() {
                    self.timer.reset_nak();
                    self.naks = self.get_all_naks();
                }
            }
            RecvState::Finished | RecvState::Cancelled => self.timer.reset_ack(),
        }
        self.state = TransactionState::Active;
        self.send_indication(Indication::Resumed(ResumeIndication {
            id: self.id(),
            progress: self.get_progress(),
        }));
        Ok(())
    }

    /// Take action according to the defined handler mapping.
    /// Returns a boolean indicating if the calling function should continue (true) or not (false.)
    fn handle_fault(&mut self, condition: Condition) -> TransactionResult<bool> {
        self.condition = condition;
        warn!("Transaction {} Handling fault {:?}", self.id(), condition);
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
                self._cancel();
                Ok(false)
            }

            FaultHandlerAction::Suspend => {
                self.suspend()?;
                Ok(false)
            }
            FaultHandlerAction::Abandon => {
                self.abandon();
                Ok(false)
            }
        }
    }

    fn prepare_ack_eof(&mut self) {
        self.ack = Some(PositiveAcknowledgePDU {
            directive: PDUDirective::EoF,
            directive_subtype_code: ACKSubDirective::Other,
            condition: self.condition,
            transaction_status: self.status,
        });
    }
    fn send_ack_eof(&mut self, permit: Permit<(VariableID, PDU)>) -> TransactionResult<()> {
        if let Some(ack) = self.ack.take() {
            let payload = PDUPayload::Directive(Operations::Ack(ack));
            let payload_len = payload.encoded_len(self.config.file_size_flag);

            let header = self.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                // TODO add semgentation Control ability
                SegmentationControl::NotPreserved,
            );

            let destination = header.source_entity_id;
            let pdu = PDU { header, payload };
            permit.send((destination, pdu));
        }
        Ok(())
    }

    fn check_file_size(&mut self, file_size: u64) -> TransactionResult<()> {
        if self.saved_segments.last().map(|s| s.1).unwrap_or(0) > file_size {
            warn!(
                "EOF file size {} is smaller than file size received in file data {}",
                file_size, self.received_file_size
            );
            // we will always exit here anyway
            self.handle_fault(Condition::FilesizeError)?;
        }
        Ok(())
    }

    fn get_progress(&self) -> u64 {
        self.received_file_size
    }

    fn prepare_finished(&mut self, fault_location: Option<VariableID>) {
        self.finished = Some((
            Finished {
                condition: self.condition,
                delivery_code: self.delivery_code,
                file_status: self.file_status,
                filestore_response: self.filestore_response.clone(),
                fault_location,
            },
            true,
        ));
    }

    fn send_finished(&mut self, permit: Permit<(VariableID, PDU)>) -> TransactionResult<()> {
        self.timer.restart_ack();
        if let Some((finished, true)) = &self.finished {
            let payload = PDUPayload::Directive(Operations::Finished(finished.clone()));

            let payload_len = payload.encoded_len(self.config.file_size_flag);

            let header = self.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            let destination = header.source_entity_id;
            let pdu = PDU { header, payload };

            permit.send((destination, pdu));
            debug!("Transaction {0} sent Finished", self.id());
            self.set_finished_flag(false);
        }
        Ok(())
    }

    // set the true flag on the fin such that it is sent at the next opportunity
    fn set_finished_flag(&mut self, flag: bool) {
        if let Some(x) = self.finished.as_mut() {
            x.1 = flag;
        }
    }

    fn get_all_naks(&self) -> VecDeque<SegmentRequestForm> {
        let mut naks: VecDeque<SegmentRequestForm> = VecDeque::new();
        let mut pointer = 0_u64;

        if self.metadata.is_none() {
            naks.push_back((0_u64, 0_u64).into());
        }
        self.saved_segments.iter().for_each(|(start, end)| {
            if start > &pointer {
                naks.push_back(SegmentRequestForm {
                    start_offset: pointer,
                    end_offset: *start,
                });
            }
            pointer = *end;
        });

        if let Some(file_size) = self.file_size {
            if pointer < file_size {
                naks.push_back(SegmentRequestForm {
                    start_offset: pointer,
                    end_offset: file_size,
                });
            }
        }
        naks
    }

    fn send_naks(&mut self, permit: Permit<(VariableID, PDU)>) -> TransactionResult<()> {
        if self.nak_received_file_size == self.received_file_size {
            if self.timer.nak.limit_reached() {
                self.handle_fault(Condition::NakLimitReached)?;
                return Ok(());
            }
            self.timer.restart_nak();
        } else {
            // new data has been received since last NAK was send; reset the counter to 0
            self.timer.reset_nak();
            self.nak_received_file_size = self.received_file_size;
        }

        let n = usize::min(
            self.naks.len(),
            NegativeAcknowledgmentPDU::max_nak_num(
                self.config.file_size_flag,
                self.config.file_size_segment as u32,
            ) as usize,
        );

        let segment_requests: Vec<SegmentRequestForm> = self.naks.drain(..n).collect();
        let scope_start = segment_requests
            .first()
            .map(|sr| sr.start_offset)
            .unwrap_or(0);
        let scope_end = segment_requests
            .last()
            .map(|sr| sr.end_offset)
            .unwrap_or(self.saved_segments.last().map(|s| s.1).unwrap_or(0));

        let nak = NegativeAcknowledgmentPDU {
            start_of_scope: scope_start,
            end_of_scope: scope_end,
            segment_requests,
        };
        debug!("Transaction {}: sending NAK for {} segments", self.id(), n);

        let payload = PDUPayload::Directive(Operations::Nak(nak));
        let payload_len = payload.encoded_len(self.config.file_size_flag);

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let destination = header.source_entity_id;
        let pdu = PDU { header, payload };

        permit.send((destination, pdu));

        Ok(())
    }

    fn verify_checksum(&mut self, checksum: u32) -> TransactionResult<bool> {
        let checksum_type = self
            .metadata
            .as_ref()
            .map(|meta| meta.checksum_type)
            .ok_or_else(|| {
                let id = self.id();
                TransactionError::MissingMetadata(id)
            })?;
        let handle = self.get_handle()?;
        handle.sync_all().map_err(FileStoreError::IO)?;
        Ok(handle.checksum(checksum_type)? == checksum)
    }

    fn finalize_receive(&mut self) -> TransactionResult<()> {
        let checksum = self.checksum.ok_or(TransactionError::NoChecksum)?;
        self.delivery_code = DeliveryCode::Complete;

        self.file_status = if self.is_file_transfer() {
            if !self.verify_checksum(checksum)?
                && !self.handle_fault(Condition::FileChecksumFailure)?
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
            && !self.handle_fault(Condition::FileStoreRejection)?
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
        // send indication this transaction is finished.
        self.send_indication(Indication::Finished(FinishedIndication {
            id: self.id(),
            report: self.generate_report(),
            file_status: self.file_status,
            delivery_code: self.delivery_code,
            filestore_responses: self.filestore_response.clone(),
        }));
        Ok(())
    }

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        self.timer.reset_inactivity();
        let PDU {
            header: _header,
            payload,
        } = pdu;
        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => {
                match payload {
                    PDUPayload::FileData(filedata) => {
                        //the end of the last segment
                        let prev_end = self.saved_segments.last().map(|(_, end)| *end).unwrap_or(0);

                        let (offset, length) = self.store_file_data(filedata)?;

                        self.send_indication(Indication::FileSegmentRecv(FileSegmentIndication {
                            id: self.id(),
                            offset,
                            length: length as u64,
                        }));

                        if self.nak_procedure == NakProcedure::Immediate && !self.eof_received() {
                            if self.timer.nak.timeout_occured() {
                                //send all gaps at the next opportunity
                                self.naks = self.get_all_naks();
                                self.timer.restart_nak();
                            } else if offset > prev_end {
                                //new gap -> send it at the next opportunity
                                self.naks.push_back(SegmentRequestForm {
                                    start_offset: prev_end,
                                    end_offset: offset,
                                });
                            }
                        }
                        self.check_finished()?;

                        Ok(())
                    }
                    PDUPayload::Directive(operation) => {
                        match operation {
                            Operations::EoF(eof) => {
                                debug!("Transaction {0} received EndOfFile.", self.id());
                                self.condition = eof.condition;
                                self.prepare_ack_eof();
                                self.checksum = Some(eof.checksum);

                                self.send_indication(Indication::EoFRecv(self.id()));

                                if self.condition == Condition::NoError {
                                    self.check_file_size(eof.file_size)?;
                                    self.file_size = Some(eof.file_size);

                                    //send all gaps (if any) at the next opportunity
                                    self.naks = self.get_all_naks();

                                    self.check_finished()?;
                                } else {
                                    // Any other condition is essentially a
                                    // CANCEL operation
                                    self._cancel();
                                }
                                Ok(())
                            }
                            Operations::Finished(_finished) => {
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode,
                                    "Finished".to_owned(),
                                ))
                            }
                            Operations::Ack(ack) => {
                                if (self.recv_state == RecvState::Finished
                                    || self.recv_state == RecvState::Cancelled)
                                    && ack.directive == PDUDirective::Finished
                                    && ack.directive_subtype_code == ACKSubDirective::Finished
                                    && ack.condition == Condition::NoError
                                {
                                    debug!(
                                        "Transaction {0} received ACK Finished({1:?}).",
                                        self.id(),
                                        ack.condition
                                    );
                                    self.timer.ack.pause();
                                    self.shutdown();
                                    Ok(())
                                } else {
                                    // No other ACKs are expected for a receiver
                                    Err(TransactionError::UnexpectedPDU(
                                        self.config.sequence_number,
                                        self.config.transmission_mode,
                                        format!(
                                            "ACK PDU: ({:?}, {:?})",
                                            ack.directive, ack.directive_subtype_code
                                        ),
                                    ))
                                }
                            }
                            Operations::Metadata(metadata) => {
                                if self.metadata.is_none() {
                                    debug!("Transaction {0} received Metadata.", self.id());
                                    let message_to_user =
                                        metadata.options.iter().filter_map(|op| match op {
                                            MetadataTLV::MessageToUser(req) => Some(req.clone()),
                                            _ => None,
                                        });
                                    // push each request up to the Daemon
                                    let source_filename: Utf8PathBuf =
                                        std::str::from_utf8(metadata.source_filename.as_slice())
                                            .map_err(FileStoreError::UTF8)?
                                            .into();

                                    let destination_filename: Utf8PathBuf = std::str::from_utf8(
                                        metadata.destination_filename.as_slice(),
                                    )
                                    .map_err(FileStoreError::UTF8)?
                                    .into();

                                    self.send_indication(Indication::MetadataRecv(
                                        MetadataRecvIndication {
                                            id: self.id(),
                                            source_filename: source_filename.clone(),
                                            destination_filename: destination_filename.clone(),
                                            file_size: metadata.file_size,
                                            transmission_mode: self.config.transmission_mode,
                                            user_messages: message_to_user.clone().collect(),
                                        },
                                    ));

                                    self.metadata = Some(Metadata {
                                        source_filename,
                                        destination_filename,
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
                                    self.check_finished()?;
                                }
                                Ok(())
                            }
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode,
                                "NAK PDU".to_owned(),
                            )),
                            Operations::Prompt(prompt) => {
                                // remember the prompt PDU - it will be answered at the next opportunity
                                self.prompt.replace(prompt);
                                Ok(())
                            }
                            Operations::KeepAlive(_keepalive) => {
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode,
                                    "KeepAlive PDU".to_owned(),
                                ))
                            }
                        }
                    }
                }
            }
            TransmissionMode::Unacknowledged => {
                match payload {
                    PDUPayload::FileData(filedata) => {
                        let (offset, length) = self.store_file_data(filedata)?;
                        self.send_indication(Indication::FileSegmentRecv(FileSegmentIndication {
                            id: self.id(),
                            offset,
                            length: length as u64,
                        }));
                        Ok(())
                    }
                    PDUPayload::Directive(operation) => {
                        match operation {
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
                                    Err(TransactionError::UnexpectedPDU(
                                        self.config.sequence_number,
                                        self.config.transmission_mode,
                                        format!(
                                            "ACK PDU: ({:?}, {:?})",
                                            ack.directive, ack.directive_subtype_code
                                        ),
                                    ))
                                }
                            }
                            Operations::EoF(eof) => {
                                self.condition = eof.condition;
                                self.checksum = Some(eof.checksum);

                                self.send_indication(Indication::EoFRecv(self.id()));

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
                                        self.recv_state = RecvState::Finished;
                                        self.prepare_finished(
                                            if self.condition == Condition::NoError {
                                                None
                                            } else {
                                                Some(self.config.destination_entity_id)
                                            },
                                        );
                                    }
                                } else {
                                    // Any other condition is essentially a
                                    // CANCEL operation
                                    self._cancel()
                                }
                                Ok(())
                            }
                            Operations::Metadata(metadata) => {
                                if self.metadata.is_none() {
                                    let message_to_user =
                                        metadata.options.iter().filter_map(|op| match op {
                                            MetadataTLV::MessageToUser(req) => Some(req.clone()),
                                            _ => None,
                                        });

                                    let source_filename: Utf8PathBuf =
                                        std::str::from_utf8(metadata.source_filename.as_slice())
                                            .map_err(FileStoreError::UTF8)?
                                            .into();

                                    let destination_filename: Utf8PathBuf = std::str::from_utf8(
                                        metadata.destination_filename.as_slice(),
                                    )
                                    .map_err(FileStoreError::UTF8)?
                                    .into();

                                    self.send_indication(Indication::MetadataRecv(
                                        MetadataRecvIndication {
                                            id: self.id(),
                                            source_filename: source_filename.clone(),
                                            destination_filename: destination_filename.clone(),
                                            file_size: metadata.file_size,
                                            transmission_mode: self.config.transmission_mode,
                                            user_messages: message_to_user.clone().collect(),
                                        },
                                    ));

                                    self.metadata = Some(Metadata {
                                        source_filename,
                                        destination_filename,
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
                            // should never receive this as a Receiver type
                            Operations::Finished(_finished) => {
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode,
                                    "Finished".to_owned(),
                                ))
                            }
                            Operations::KeepAlive(_keepalive) => {
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode,
                                    "Keep Alive".to_owned(),
                                ))
                            }
                            Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode,
                                "Prompt".to_owned(),
                            )),
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode,
                                "NAK PDU".to_owned(),
                            )),
                        }
                    }
                }
            }
        }
    }

    // go to the Finished state if the file is complete (used only in acknowledged mode)
    fn check_finished(&mut self) -> TransactionResult<()> {
        if self.metadata.is_some()
            && self.eof_received()
            && !(self.is_file_transfer() && self.has_naks())
        {
            self.finalize_receive()?;
            self.recv_state = RecvState::Finished;
            self.prepare_finished(None);
            self.timer.nak.pause();
        }
        Ok(())
    }

    //send the indication in another task, no need to wait for it
    fn send_indication(&self, indication: Indication) {
        let tx = self.indication_tx.clone();
        tokio::task::spawn(async move { tx.send(indication).await });
    }
}

#[cfg(test)]
mod test {
    use std::{fs::OpenOptions, io::Read};

    use crate::{
        assert_err,
        filestore::{ChecksumType, NativeFileStore},
        pdu::{
            CRCFlag, EndOfFile, FileSizeFlag, FileStoreAction, FileStoreRequest, FileStoreStatus,
            MessageToUser, MetadataPDU, PromptPDU, RenameStatus, SegmentedData,
            UnsegmentedFileData,
        },
    };

    use super::super::config::test::default_config;
    use super::*;

    use camino::{Utf8Path, Utf8PathBuf};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;
    use tokio::sync::mpsc::channel;

    #[fixture]
    #[once]
    fn tempdir_fixture() -> TempDir {
        TempDir::new().unwrap()
    }

    #[rstest]
    fn header(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(1);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);
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
        let (indication_tx, _) = channel(10);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);
        assert_eq!(
            TransactionStatus::Undefined,
            transaction.get_status().clone()
        );
        let mut path = Utf8PathBuf::new();
        path.push("a");

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: 600_u64,
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        assert!(transaction.is_file_transfer())
    }

    #[rstest]
    fn store_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(10);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

        let input = vec![0, 5, 255, 99];
        let data = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 6,
            file_data: input.clone(),
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(6, offset);
        assert_eq!(4, length);

        let handle = transaction.get_handle().unwrap();
        handle.seek(SeekFrom::Start(6)).unwrap();
        let mut buff = vec![0; 4];
        handle.read_exact(&mut buff).unwrap();

        assert_eq!(input, buff)
    }

    #[rstest]
    fn finalize_file(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(10);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let input = "This is test data\n!";
        let output_file = {
            let mut temp_buff = Utf8PathBuf::new();
            temp_buff.push("finalize_test.txt");
            temp_buff
        };

        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred,
            filestore.clone(),
            indication_tx,
        );
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: input.len() as u64,
            source_filename: output_file.clone(),
            destination_filename: output_file.clone(),
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let data = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 0,
            file_data: input.as_bytes().to_vec(),
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(0, offset);
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
    #[tokio::test]
    async fn test_naks(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _) = channel(10);
        let mut config = default_config.clone();
        config.file_size_flag = file_size_flag;
        let file_size = match &file_size_flag {
            FileSizeFlag::Small => 20,
            FileSizeFlag::Large => u32::MAX as u64 + 100_u64,
        };

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

        let input = vec![0, 5, 255, 99];
        let data = FileDataPDU::Unsegmented(UnsegmentedFileData {
            offset: 6,
            file_data: input,
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(6, offset);
        assert_eq!(4, length);

        transaction.file_size = Some(file_size);
        transaction.received_file_size = file_size;

        let expected: VecDeque<SegmentRequestForm> = vec![
            SegmentRequestForm::from((0_u64, 0_u64)),
            SegmentRequestForm::from((0_u64, 6_u64)),
            SegmentRequestForm::from((10_u64, file_size)),
        ]
        .into();

        assert_eq!(expected, transaction.get_all_naks());

        let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
            start_of_scope: 0,
            end_of_scope: file_size,
            segment_requests: expected.into(),
        }));

        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        transaction.naks = transaction.get_all_naks();
        transaction
            .send_naks(transport_tx.reserve().await.unwrap())
            .unwrap();

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        let expected_id = default_config.source_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    #[tokio::test]
    async fn cancel_receive(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, mut transport_rx) = channel(1);
        let (indication_tx, _) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config.clone(),
            NakProcedure::Deferred,
            filestore,
            indication_tx,
        );

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push(format!("test_eof_{:?}.dat", &config.transmission_mode));
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: input.as_bytes().len() as u64,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Finished(Finished {
            condition: Condition::CancelReceived,
            delivery_code: transaction.delivery_code,
            file_status: transaction.file_status,
            filestore_response: vec![],
            fault_location: None,
        }));
        let payload_len = payload.encoded_len(config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        transaction.cancel().unwrap();
        if transaction.config.transmission_mode == TransmissionMode::Unacknowledged {
            assert_eq!(TransactionState::Terminated, transaction.state);
        }

        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();

        if config.transmission_mode == TransmissionMode::Acknowledged {
            let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
            let expected_id = default_config.source_entity_id;

            assert_eq!(expected_id, destination_id);
            assert_eq!(pdu, received_pdu);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn suspend(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _indication_rx) = channel(1);
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

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

        transaction.suspend().unwrap();

        let timers = [
            &transaction.timer.ack,
            &transaction.timer.inactivity,
            &transaction.timer.nak,
        ];
        timers.iter().for_each(|timer| assert!(!timer.is_ticking()));
        assert_eq!(TransactionState::Suspended, transaction.state)
    }

    #[rstest]
    #[tokio::test]
    async fn resume(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _indication_rx) = channel(1);
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

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

        transaction.suspend().unwrap();

        let timers = [
            &transaction.timer.ack,
            &transaction.timer.inactivity,
            &transaction.timer.nak,
        ];
        timers.iter().for_each(|timer| assert!(!timer.is_ticking()));

        transaction.resume().unwrap();
        assert_eq!(TransactionState::Active, transaction.state);

        assert!(transaction.timer.inactivity.is_ticking());
        assert!(!transaction.timer.ack.is_ticking());
        assert!(!transaction.timer.nak.is_ticking());

        transaction.recv_state = RecvState::Finished;
        transaction.resume().unwrap();
        assert!(transaction.timer.ack.is_ticking());

        transaction.recv_state = RecvState::ReceiveData;
        transaction.nak_procedure = NakProcedure::Immediate;
        transaction.resume().unwrap();
        assert!(transaction.timer.nak.is_ticking());
    }

    #[rstest]
    #[tokio::test]
    async fn send_ack_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _) = channel(1);
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config.clone(),
            NakProcedure::Deferred,
            filestore,
            indication_tx,
        );

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push(format!("test_eof_{:}.dat", config.transmission_mode as u8));
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: input.as_bytes().len() as u64,
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
            transaction_status: transaction.status,
        }));

        let payload_len = payload.encoded_len(config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        tokio::task::spawn(async move {
            transaction.prepare_ack_eof();
            transaction
                .send_ack_eof(transport_tx.reserve().await.unwrap())
                .unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        let expected_id = default_config.source_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    #[tokio::test]
    async fn finalize_receive(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _indication_rx) = channel(1);
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred,
            filestore.clone(),
            indication_tx,
        );

        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_finalize_receive.dat");
            buf
        };

        let input = "Here is some test data to write!$*#*.\n";

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: input.as_bytes().len() as u64,
            source_filename: path.clone(),
            destination_filename: path.clone(),
            message_to_user: vec![],
            checksum_type: ChecksumType::Modular,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::DeleteFile,
                first_filename: path.clone(),
                second_filename: "".into(),
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
            offset: 0,
            file_data: input.as_bytes().to_vec(),
        });
        transaction.checksum = Some(checksum);

        transaction.store_file_data(pdu).unwrap();
        transaction.finalize_receive().unwrap();

        assert!(!filestore.get_native_path(path).exists());
    }

    #[rstest]
    fn pdu_error_unack(
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
                    first_filename: "/path/to/a/file".into(),
                    second_filename: "/path/to/a/new/file".into(),
                    filestore_message: vec![1_u8, 3, 58],
                }],
                fault_location: None,

            }),
            Operations::KeepAlive(KeepAlivePDU{
                progress: 12_u64
            }),
            Operations::Prompt(PromptPDU{
                nak_or_keep_alive: NakOrKeepAlive::KeepAlive,
            }),
            Operations::Nak(NegativeAcknowledgmentPDU{
                start_of_scope: 0_u64,
                end_of_scope: 1022_u64,
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            })
        )]
        operation: Operations,
    ) {
        let (indication_tx, _) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
            Err(TransactionError::UnexpectedPDU(
                _,
                TransmissionMode::Unacknowledged,
                _
            ))
        )
    }

    #[rstest]
    fn pdu_error_ack(
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
                    first_filename: "/path/to/a/file".into(),
                    second_filename: "/path/to/a/new/file".into(),
                    filestore_message: vec![1_u8, 3, 58],
                }],
                fault_location: None,

            }),
            Operations::KeepAlive(KeepAlivePDU{
                progress: 12_u64
            }),
            Operations::Nak(NegativeAcknowledgmentPDU{
                start_of_scope: 0_u64,
                end_of_scope: 1022_u64,
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            })
        )]
        operation: Operations,
    ) {
        let (indication_tx, _) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
            Err(TransactionError::UnexpectedPDU(
                _,
                TransmissionMode::Acknowledged,
                _
            ))
        )
    }

    #[rstest]
    #[tokio::test]
    async fn recv_store_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (indication_tx, _indication_rx) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

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
    #[tokio::test]
    async fn recv_store_metadata(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (indication_tx, mut indication_rx) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

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
            first_filename: "some_name".into(),
            second_filename: "".into(),
        };

        let expected = Some(Metadata {
            closure_requested: false,
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![expected_msg.clone()],
            filestore_requests: vec![fs_req.clone()],
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Metadata(MetadataPDU {
            closure_requested: false,
            checksum_type: ChecksumType::Modular,
            file_size: 600,
            source_filename: "Test_file.txt".as_bytes().to_vec(),
            destination_filename: "Test_file.txt".as_bytes().to_vec(),
            options: vec![
                MetadataTLV::MessageToUser(expected_msg.clone()),
                MetadataTLV::FileStoreRequest(fs_req),
            ],
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        transaction.process_pdu(pdu).unwrap();
        assert_eq!(expected, transaction.metadata);

        let indication = indication_rx.recv().await.unwrap();
        if let Indication::MetadataRecv(MetadataRecvIndication {
            user_messages: user_msg,
            ..
        }) = indication
        {
            assert_eq!(1, user_msg.len());
            assert_eq!(expected_msg, user_msg[0])
        } else {
            panic!()
        }
    }

    #[rstest]
    #[tokio::test]
    async fn recv_eof_all_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _indication_rx) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            closure_requested: true,
            file_size: 600,
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
                offset: 0,
                file_data: input_data.as_bytes().to_vec(),
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
                file_size: input_data.len() as u64,
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

        let expected_pdu = {
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

        {
            // this is the whole contents of the file
            transaction.process_pdu(file_pdu).unwrap();

            transaction.process_pdu(input_pdu).unwrap();
            transaction
                .send_pdu(transport_tx.reserve().await.unwrap())
                .unwrap();

            if transaction.config.transmission_mode == TransmissionMode::Acknowledged {
                transaction
                    .send_pdu(transport_tx.reserve().await.unwrap())
                    .unwrap();
                assert!(transaction.timer.ack.is_ticking());

                let ack_fin = {
                    let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                        directive: PDUDirective::Finished,
                        directive_subtype_code: ACKSubDirective::Finished,
                        condition: Condition::NoError,
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
                transaction.process_pdu(ack_fin).unwrap();

                assert!(!transaction.timer.ack.is_ticking());
            }
        }

        // need to get the EoF from Acknowledged mode too.
        if transmission_mode == TransmissionMode::Acknowledged {
            let eof_pdu = {
                let payload = PDUPayload::Directive(Operations::Ack(PositiveAcknowledgePDU {
                    directive: PDUDirective::EoF,
                    directive_subtype_code: ACKSubDirective::Other,
                    condition: Condition::NoError,
                    transaction_status: TransactionStatus::Undefined,
                }));

                let payload_len = payload.encoded_len(expected_pdu.header.large_file_flag);

                let header = {
                    let mut head = expected_pdu.header.clone();
                    head.pdu_data_field_length = payload_len;
                    head
                };

                PDU { header, payload }
            };
            let (destination_id, end_of_file) = transport_rx.recv().await.unwrap();

            assert_eq!(expected_id, destination_id);
            assert_eq!(eof_pdu, end_of_file)
        }

        let (destination_id, finished_pdu) = transport_rx.recv().await.unwrap();

        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, finished_pdu)
    }

    #[rstest]
    #[tokio::test]
    async fn recv_prompt(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(NakOrKeepAlive::Nak, NakOrKeepAlive::KeepAlive)] nak_or_keep_alive: NakOrKeepAlive,
    ) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _indication_rx) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);
        let path = {
            let mut buf = Utf8PathBuf::new();
            buf.push("test_file");
            buf
        };
        transaction.metadata = Some(Metadata {
            closure_requested: false,
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            message_to_user: vec![],
            filestore_requests: vec![],
            checksum_type: ChecksumType::Modular,
        });

        //this simulates effectively the EOF reception
        //it is required because, according to spec, unless EOF has been received, the NAK sequence
        // will not include the last part of the file (even though the file_size is known from the metadata)
        transaction.file_size = Some(600);

        let prompt_pdu = {
            let payload =
                PDUPayload::Directive(Operations::Prompt(PromptPDU { nak_or_keep_alive }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

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
                let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                    start_of_scope: 0,
                    end_of_scope: 600,
                    segment_requests: vec![SegmentRequestForm {
                        start_offset: 0,
                        end_offset: 600,
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
            }
            NakOrKeepAlive::KeepAlive => {
                let payload =
                    PDUPayload::Directive(Operations::KeepAlive(KeepAlivePDU { progress: 0 }));
                let payload_len = payload.encoded_len(transaction.config.file_size_flag);

                let header = transaction.get_header(
                    Direction::ToSender,
                    PDUType::FileDirective,
                    payload_len,
                    SegmentationControl::NotPreserved,
                );

                PDU { header, payload }
            }
        };

        transaction.process_pdu(prompt_pdu).unwrap();
        assert!(transaction.has_pdu_to_send());
        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, received_pdu)
    }

    #[rstest]
    #[tokio::test]
    async fn nak_split(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _indication_rx) = channel(10);
        let mut config = default_config.clone();
        config.file_size_segment = 16;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, NakProcedure::Deferred, filestore, indication_tx);
        transaction.nak_procedure = NakProcedure::Immediate;

        let file_pdu1 = {
            let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
                offset: 16,
                file_data: vec![0; 16],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let file_pdu2 = {
            let payload = PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData {
                offset: 48,
                file_data: vec![0; 16],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let expected_nak1 = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: 16,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 0,
                    end_offset: 16,
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

        let expected_nak2 = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 32,
                end_of_scope: 48,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 32,
                    end_offset: 48,
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

        transaction.process_pdu(file_pdu1).unwrap();
        transaction.process_pdu(file_pdu2).unwrap();

        assert!(transaction.has_pdu_to_send());
        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();
        assert!(transaction.has_pdu_to_send());
        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_nak1, received_pdu);
        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_nak2, received_pdu)
    }
}
