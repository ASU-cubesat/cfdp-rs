use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    sync::Arc,
};

use crossbeam_channel::Sender;

use super::{
    config::{Metadata, TransactionConfig, TransactionID, TransactionState, WaitingOn},
    error::{TransactionError, TransactionResult},
};
use crate::{
    daemon::Report,
    filestore::{FileChecksum, FileStore, FileStoreError},
    pdu::{
        ACKSubDirective, Condition, DeliveryCode, Direction, FaultHandlerAction, FileDataPDU,
        FileStatusCode, FileStoreResponse, Finished, KeepAlivePDU, MessageToUser, MetadataTLV,
        NakOrKeepAlive, NegativeAcknowledgmentPDU, Operations, PDUDirective, PDUHeader, PDUPayload,
        PDUType, PositiveAcknowledgePDU, SegmentRequestForm, SegmentationControl,
        TransactionSeqNum, TransactionStatus, TransmissionMode, VariableID, PDU, U3,
    },
    segments,
    timer::Timer,
};

pub struct RecvTransaction<T: FileStore> {
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
    /// A sorted list of contiguous (start offset, end offset) to monitor progress.
    /// For a Receiver, these are the received segments.
    /// For a Sender, these are the sent segments.
    saved_segments: Vec<(u64, u64)>,
    /// The list of all missing information
    naks: VecDeque<SegmentRequestForm>,
    /// Flag to check if metadata on the file has been received
    pub(crate) metadata: Option<Metadata>,
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
}
impl<T: FileStore> RecvTransaction<T> {
    /// Start a new SendTransaction with the given [configuration](TransactionConfig)
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
            message_tx,
            file_handle: None,
            saved_segments: Vec::new(),
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
            segments::update_segments(
                &mut self.saved_segments,
                (offset, offset + file_data.len() as u64),
            );
        } else {
            log::warn!(
                "Received FileDataPDU with invalid file_data.length = {}; ignored",
                length
            );
        }

        Ok((offset, length))
    }

    fn update_naks(&mut self, file_size: Option<u64>) {
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
        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => self.send_finished(None),
            TransmissionMode::Unacknowledged => {
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

    fn send_ack_eof(&mut self) -> TransactionResult<()> {
        let ack = PositiveAcknowledgePDU {
            directive: PDUDirective::EoF,
            directive_subtype_code: ACKSubDirective::Other,
            condition: self.condition.clone(),
            transaction_status: self.status.clone(),
        };
        let payload = PDUPayload::Directive(Operations::Ack(ack));
        let payload_len = payload.clone().encode(self.config.file_size_flag).len();

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            // TODO add semgentation Control ability
            SegmentationControl::NotPreserved,
        );

        let destination = header.source_entity_id;
        let pdu = PDU { header, payload };
        self.transport_tx.send((destination, pdu))?;

        Ok(())
    }

    fn check_file_size(&mut self, file_size: u64) -> TransactionResult<()> {
        if self.received_file_size > file_size {
            // we will always exit here anyway
            self.proceed_despite_fault(Condition::FilesizeError)?;
        }
        Ok(())
    }

    fn get_progress(&self) -> u64 {
        // the Enum types are guaranteed to match the FileSize flag, we just need to unwrap them.
        self.saved_segments
            .iter()
            .fold(0_u64, |size, (start, end)| size + (end - start))
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

        let payload_len = payload.clone().encode(self.config.file_size_flag).len() as u16;

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.source_entity_id;
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

        let nak = NegativeAcknowledgmentPDU {
            start_of_scope: 0_u64,
            end_of_scope: self.received_file_size,
            segment_requests: self.naks.clone().into(),
        };

        let payload = PDUPayload::Directive(Operations::Nak(nak));
        let payload_len = payload.clone().encode(self.config.file_size_flag).len();

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len as u16,
            SegmentationControl::NotPreserved,
        );
        let destination = header.source_entity_id;
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
        handle.sync_all().map_err(FileStoreError::IO)?;
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
        if self.timer.ack.timeout_occured() && self.waiting_on == WaitingOn::AckFin {
            return self.send_finished(None);
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
                    PDUPayload::FileData(filedata) => {
                        // if we're getting data but have sent a nak
                        // restart the timer.
                        if self.waiting_on == WaitingOn::Nak {
                            self.timer.restart_nak();
                        }
                        // Issue notice of recieved? Log it.
                        let (offset, length) = self.store_file_data(filedata)?;
                        // update the total received size if appropriate.
                        let size = offset + length as u64;
                        if self.received_file_size < size {
                            self.received_file_size = size;
                        }

                        // need a block here for if we have sent naks
                        // in deferred mode. a second EoF is not sent
                        // so we should check if we have the whole thing?
                        if self.waiting_on == WaitingOn::Nak {
                            self.update_naks(self.metadata.as_ref().map(|meta| meta.file_size));
                            match self.naks.is_empty() {
                                false => self.send_naks()?,
                                true => {
                                    self.finalize_receive()?;
                                    self.timer.nak.pause();
                                    self.waiting_on = WaitingOn::None;
                                    self.send_finished(if self.condition == Condition::NoError {
                                        None
                                    } else {
                                        Some(self.config.destination_entity_id)
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
                                    self.update_naks(Some(eof.file_size));

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
                                                    Some(self.config.destination_entity_id)
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
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode.clone(),
                                    "Finished".to_owned(),
                                ))
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
                                    Err(TransactionError::UnexpectedPDU(
                                        self.config.sequence_number,
                                        self.config.transmission_mode.clone(),
                                        format!(
                                            "ACK PDU: ({:?}, {:?})",
                                            ack.directive, ack.directive_subtype_code
                                        ),
                                    ))
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
                                    // need a block here for if we have sent naks
                                    // in deferred mode. a second EoF is not sent
                                    // so we should check if we have the whole thing?
                                    if self.waiting_on == WaitingOn::Nak {
                                        self.update_naks(Some(metadata.file_size));
                                        match self.naks.is_empty() {
                                            false => self.send_naks()?,
                                            true => {
                                                self.finalize_receive()?;
                                                self.timer.nak.pause();
                                                self.waiting_on = WaitingOn::None;
                                                return self.send_finished(
                                                    if self.condition == Condition::NoError {
                                                        None
                                                    } else {
                                                        Some(self.config.destination_entity_id)
                                                    },
                                                );
                                            }
                                        };
                                    }
                                }
                                Ok(())
                            }
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode.clone(),
                                "NAK PDU".to_owned(),
                            )),
                            Operations::Prompt(prompt) => match prompt.nak_or_keep_alive {
                                NakOrKeepAlive::Nak => {
                                    self.update_naks(
                                        self.metadata.as_ref().map(|meta| meta.file_size),
                                    );
                                    self.send_naks()
                                }
                                NakOrKeepAlive::KeepAlive => {
                                    let progress = self.get_progress();
                                    let data = KeepAlivePDU { progress };

                                    let payload =
                                        PDUPayload::Directive(Operations::KeepAlive(data));
                                    let payload_len =
                                        payload.clone().encode(self.config.file_size_flag).len()
                                            as u16;

                                    let header = self.get_header(
                                        Direction::ToSender,
                                        PDUType::FileDirective,
                                        payload_len,
                                        SegmentationControl::NotPreserved,
                                    );

                                    let destination = header.source_entity_id;

                                    let pdu = PDU { header, payload };
                                    self.transport_tx.send((destination, pdu))?;
                                    Ok(())
                                }
                            },
                            Operations::KeepAlive(_keepalive) => {
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode.clone(),
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
                        // Issue notice of recieved? Log it.
                        let (offset, length) = self.store_file_data(filedata)?;
                        // update the total received size if appropriate.
                        let size = offset + length as u64;
                        if self.received_file_size < size {
                            self.received_file_size = size;
                        }
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
                                        self.config.transmission_mode.clone(),
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
                                        self.send_finished(
                                            if self.condition == Condition::NoError {
                                                None
                                            } else {
                                                Some(self.config.destination_entity_id)
                                            },
                                        )?;
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
                                    self.config.transmission_mode.clone(),
                                    "Finished".to_owned(),
                                ))
                            }
                            Operations::KeepAlive(_keepalive) => {
                                Err(TransactionError::UnexpectedPDU(
                                    self.config.sequence_number,
                                    self.config.transmission_mode.clone(),
                                    "Keep Alive".to_owned(),
                                ))
                            }
                            Operations::Prompt(_prompt) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode.clone(),
                                "Prompt".to_owned(),
                            )),
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode.clone(),
                                "NAK PDU".to_owned(),
                            )),
                        }
                    }
                }
            }
        }
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
            MetadataPDU, PromptPDU, RenameStatus, SegmentedData, UnsegmentedFileData,
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
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);
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
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);
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
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
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

        let mut transaction =
            RecvTransaction::new(config, filestore.clone(), transport_tx, message_tx);
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
    fn update_naks(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.file_size_flag = file_size_flag;
        let file_size = match &file_size_flag {
            FileSizeFlag::Small => 20,
            FileSizeFlag::Large => u32::MAX as u64 + 100_u64,
        };

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
        transaction.update_naks(Some(file_size));
        transaction.received_file_size = file_size;

        let expected: VecDeque<SegmentRequestForm> = vec![
            SegmentRequestForm::from((0_u64, 0_u64)),
            SegmentRequestForm::from((0_u64, 6_u64)),
            SegmentRequestForm::from((10_u64, file_size)),
        ]
        .into();

        assert_eq!(expected, transaction.naks);

        let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
            start_of_scope: 0,
            end_of_scope: file_size,
            segment_requests: expected.into(),
        }));

        let payload_len = payload
            .clone()
            .encode(transaction.config.file_size_flag)
            .len();
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
        let expected_id = default_config.source_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
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

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config.clone(), filestore, transport_tx, message_tx);

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
            delivery_code: transaction.delivery_code.clone(),
            file_status: transaction.file_status.clone(),
            filestore_response: vec![],
            fault_location: None,
        }));
        let payload_len = payload.clone().encode(config.file_size_flag).len();

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
            let expected_id = default_config.source_entity_id;

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
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
        let (message_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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

        transaction.waiting_on = WaitingOn::Nak;
        transaction.resume();
        assert!(transaction.timer.nak.is_ticking());
    }

    #[rstest]
    fn send_ack_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, transport_rx) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config.clone(), filestore, transport_tx, message_tx);

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
            transaction_status: transaction.status.clone(),
        }));

        let payload_len = payload.clone().encode(config.file_size_flag).len();

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
        let expected_id = default_config.source_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn finalize_receive(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let config = default_config.clone();

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction =
            RecvTransaction::new(config, filestore.clone(), transport_tx, message_tx);

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
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
    fn recv_store_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, _) = unbounded();
        let (message_tx, _) = unbounded();
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
        config.transmission_mode = transmission_mode.clone();

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);

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
            let payload_len = payload
                .clone()
                .encode(transaction.config.file_size_flag)
                .len() as u16;
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

        let expected_pdu = {
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

                let payload_len = payload
                    .clone()
                    .encode(expected_pdu.header.large_file_flag)
                    .len() as u16;

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
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(config, filestore, transport_tx, message_tx);
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

        let prompt_pdu = {
            let payload = PDUPayload::Directive(Operations::Prompt(PromptPDU {
                nak_or_keep_alive: nak_or_keep_alive.clone(),
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

        let expected_pdu = match nak_or_keep_alive {
            NakOrKeepAlive::Nak => {
                let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                    start_of_scope: 0,
                    end_of_scope: 0,
                    segment_requests: vec![SegmentRequestForm {
                        start_offset: 0,
                        end_offset: 600,
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
            }
            NakOrKeepAlive::KeepAlive => {
                let payload =
                    PDUPayload::Directive(Operations::KeepAlive(KeepAlivePDU { progress: 0 }));
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
            }
        };
        thread::spawn(move || {
            transaction.process_pdu(prompt_pdu).unwrap();
        });

        let (destination_id, received_pdu) = transport_rx.recv().unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_pdu, received_pdu)
    }
}
