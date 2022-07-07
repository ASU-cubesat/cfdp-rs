use std::io::Read;

use num_traits::{FromPrimitive, ToPrimitive};

use super::{
    error::{PDUError, PDUResult},
    fault_handler::FaultHandlerOverride,
    filestore::{FilestoreRequest, FilestoreResponse},
    header::{
        read_length_value_pair, Condition, DeliveryCode, Direction, FileSizeSensitive,
        FileStatusCode, MessageType, PDUDirective, PDUEncode, PDUHeader, SegmentationControl,
        TraceControl, TransactionStatus, TransmissionMode,
    },
};

const USER_OPS_IDENTIFIER: &[u8] = "cfdp".as_bytes();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataPDU {
    header: PDUHeader,
    directive: PDUDirective,
    closure_requested: bool,
    filesize: FileSizeSensitive,
    source_filename: Vec<u8>,
    destination_filename: Vec<u8>,
    metadata_tlvs: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserOperation {
    OriginatingTransactionIDMessage(OriginatingTransactionIDMessage),
    ProxyPutRequest(ProxyPutRequest),
    ProxyPutResponse(ProxyPutResponse),
    ProxyMessageToUser(ProxyMessageToUser),
    ProxyFilestoreRequest(FilestoreRequest),
    ProxyFilestoreResponse(FilestoreResponse),
    ProxyFaultHandlerOverride(FaultHandlerOverride),
    ProxyTransmissionMode(ProxyTransmissionMode),
    ProxyFlowLabel(ProxyFlowLabel),
    ProxySegmentationControl(ProxySegmentationControl),
    ProxyPutCancel,
    DirectoryListingRequest(DirectoryListingRequest),
    DirectoryListingResponse(DirectoryListingResponse),
    RemoteStatusReportRequest(RemoteStatusReportRequest),
    RemoteStatusReportResponse(RemoteStatusReportResponse),
    RemoteSuspendRequest(RemoteSuspendRequest),
    RemoteSuspendResponse(RemoteSuspendResponse),
    RemoteResumeRequest(RemoteResumeRequest),
    RemoteResumeResponse(RemoteResumeResponse),
    SFORequest(SFORequest),
    SFOMessageToUser(SFOMessageToUser),
    SFOFlowLabel(SFOFlowLabel),
    SFOFaultHandlerOverride(FaultHandlerOverride),
    SFOFilestoreRequest(FilestoreRequest),
    SFOFilestoreResponse(FilestoreResponse),
    SFOReport(SFOReport),
}
impl UserOperation {
    pub fn get_message_type(&self) -> MessageType {
        match self {
            Self::OriginatingTransactionIDMessage(_) => {
                MessageType::OriginatingTransactionIDMessage
            }
            Self::ProxyPutRequest(_) => MessageType::ProxyPutRequest,
            Self::ProxyPutResponse(_) => MessageType::ProxyPutResponse,
            Self::ProxyMessageToUser(_) => MessageType::ProxyMessageToUser,
            Self::ProxyFilestoreRequest(_) => MessageType::ProxyFilestoreRequest,
            Self::ProxyFaultHandlerOverride(_) => MessageType::ProxyFaultHandlerOverride,
            Self::ProxyTransmissionMode(_) => MessageType::ProxyTransmissionMode,
            Self::ProxyFlowLabel(_) => MessageType::ProxyFlowLabel,
            Self::ProxySegmentationControl(_) => MessageType::ProxySegmentationControl,
            Self::ProxyFilestoreResponse(_) => MessageType::ProxyFilestoreResponse,
            Self::ProxyPutCancel => MessageType::ProxyPutCancel,
            Self::DirectoryListingRequest(_) => MessageType::DirectoryListingRequest,
            Self::DirectoryListingResponse(_) => MessageType::DirectoryListingResponse,
            Self::RemoteStatusReportRequest(_) => MessageType::RemoteStatusReportRequest,
            Self::RemoteStatusReportResponse(_) => MessageType::RemoteStatusReportResponse,
            Self::RemoteSuspendRequest(_) => MessageType::RemoteSuspendRequest,
            Self::RemoteSuspendResponse(_) => MessageType::RemoteSuspendResponse,
            Self::RemoteResumeRequest(_) => MessageType::RemoteResumeRequest,
            Self::RemoteResumeResponse(_) => MessageType::RemoteResumeResponse,
            Self::SFORequest(_) => MessageType::SFORequest,
            Self::SFOMessageToUser(_) => MessageType::SFOMessageToUser,
            Self::SFOFlowLabel(_) => MessageType::SFOFlowLabel,
            Self::SFOFaultHandlerOverride(_) => MessageType::SFOFaultHandlerOverride,
            Self::SFOFilestoreRequest(_) => MessageType::SFOFilestoreRequest,
            Self::SFOFilestoreResponse(_) => MessageType::SFOFilestoreResponse,
            Self::SFOReport(_) => MessageType::SFOReport,
        }
    }
}
impl PDUEncode for UserOperation {
    type PDUType = Self;
    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = USER_OPS_IDENTIFIER.to_vec();
        buffer.push(self.get_message_type().to_u8().unwrap());
        let message_buffer = match self {
            Self::OriginatingTransactionIDMessage(msg) => msg.to_bytes(),
            Self::ProxyPutRequest(msg) => msg.to_bytes(),
            Self::ProxyPutResponse(msg) => msg.to_bytes(),
            Self::ProxyMessageToUser(msg) => msg.to_bytes(),
            Self::ProxyFilestoreRequest(msg) => msg.to_bytes(),
            Self::ProxyFaultHandlerOverride(msg) => msg.encode(),
            Self::ProxyTransmissionMode(msg) => msg.to_bytes(),
            Self::ProxyFlowLabel(msg) => msg.to_bytes(),
            Self::ProxySegmentationControl(msg) => msg.to_bytes(),
            Self::ProxyFilestoreResponse(msg) => msg.to_bytes(),
            Self::ProxyPutCancel => vec![],
            Self::DirectoryListingRequest(msg) => msg.to_bytes(),
            Self::DirectoryListingResponse(msg) => msg.to_bytes(),
            Self::RemoteStatusReportRequest(msg) => msg.to_bytes(),
            Self::RemoteStatusReportResponse(msg) => msg.to_bytes(),
            Self::RemoteSuspendRequest(msg) => msg.to_bytes(),
            Self::RemoteSuspendResponse(msg) => msg.to_bytes(),
            Self::RemoteResumeRequest(msg) => msg.to_bytes(),
            Self::RemoteResumeResponse(msg) => msg.to_bytes(),
            Self::SFORequest(msg) => msg.to_bytes(),
            Self::SFOMessageToUser(msg) => msg.to_bytes(),
            Self::SFOFlowLabel(msg) => msg.to_bytes(),
            Self::SFOFaultHandlerOverride(msg) => msg.encode(),
            Self::SFOFilestoreRequest(msg) => msg.to_bytes(),
            Self::SFOFilestoreResponse(msg) => msg.to_bytes(),
            Self::SFOReport(msg) => msg.to_bytes(),
        };
        buffer.extend(message_buffer);
        buffer
    }
    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut cfdp_buff = vec![0u8; 4];
        buffer.read_exact(&mut cfdp_buff)?;
        if cfdp_buff != USER_OPS_IDENTIFIER.to_vec() {
            return Err(PDUError::UnexpectedIdentifier(
                cfdp_buff,
                USER_OPS_IDENTIFIER.to_vec(),
            ));
        }

        let mut u8_buff = [0u8];
        buffer.read_exact(&mut u8_buff)?;
        let message_type =
            MessageType::from_u8(u8_buff[0]).ok_or(PDUError::MessageType(u8_buff[0]))?;
        match message_type {
            MessageType::ProxyPutRequest => {
                Ok(Self::ProxyPutRequest(ProxyPutRequest::parse(buffer)?))
            }
            MessageType::ProxyMessageToUser => {
                Ok(Self::ProxyMessageToUser(ProxyMessageToUser::parse(buffer)?))
            }
            MessageType::ProxyFilestoreRequest => Ok(Self::ProxyFilestoreRequest(
                FilestoreRequest::parse(buffer)?,
            )),
            MessageType::ProxyFilestoreResponse => Ok(Self::ProxyFilestoreResponse(
                FilestoreResponse::parse(buffer)?,
            )),
            MessageType::ProxyFaultHandlerOverride => Ok(Self::ProxyFaultHandlerOverride(
                FaultHandlerOverride::decode(buffer)?,
            )),
            MessageType::ProxyTransmissionMode => Ok(Self::ProxyTransmissionMode(
                ProxyTransmissionMode::parse(buffer)?,
            )),
            MessageType::ProxyFlowLabel => Ok(Self::ProxyFlowLabel(ProxyFlowLabel::parse(buffer)?)),
            MessageType::ProxySegmentationControl => Ok(Self::ProxySegmentationControl(
                ProxySegmentationControl::parse(buffer)?,
            )),
            MessageType::ProxyPutResponse => {
                Ok(Self::ProxyPutResponse(ProxyPutResponse::parse(buffer)?))
            }
            MessageType::ProxyPutCancel => Ok(Self::ProxyPutCancel),
            MessageType::OriginatingTransactionIDMessage => {
                Ok(Self::OriginatingTransactionIDMessage(
                    OriginatingTransactionIDMessage::parse(buffer)?,
                ))
            }
            MessageType::ProxyClosureRequest => Err(PDUError::MessageType(u8_buff[0])),
            MessageType::DirectoryListingRequest => Ok(Self::DirectoryListingRequest(
                DirectoryListingRequest::parse(buffer)?,
            )),
            MessageType::DirectoryListingResponse => Ok(Self::DirectoryListingResponse(
                DirectoryListingResponse::parse(buffer)?,
            )),
            MessageType::RemoteStatusReportRequest => Ok(Self::RemoteStatusReportRequest(
                RemoteStatusReportRequest::parse(buffer)?,
            )),
            MessageType::RemoteStatusReportResponse => Ok(Self::RemoteStatusReportResponse(
                RemoteStatusReportResponse::parse(buffer)?,
            )),
            MessageType::RemoteSuspendRequest => Ok(Self::RemoteSuspendRequest(
                RemoteSuspendRequest::parse(buffer)?,
            )),
            MessageType::RemoteSuspendResponse => Ok(Self::RemoteSuspendResponse(
                RemoteSuspendResponse::parse(buffer)?,
            )),
            MessageType::RemoteResumeRequest => Ok(Self::RemoteResumeRequest(
                RemoteResumeRequest::parse(buffer)?,
            )),
            MessageType::RemoteResumeResponse => Ok(Self::RemoteResumeResponse(
                RemoteResumeResponse::parse(buffer)?,
            )),
            MessageType::SFORequest => Ok(Self::SFORequest(SFORequest::parse(buffer)?)),
            MessageType::SFOMessageToUser => {
                Ok(Self::SFOMessageToUser(SFOMessageToUser::parse(buffer)?))
            }
            MessageType::SFOFlowLabel => Ok(Self::SFOFlowLabel(SFOFlowLabel::parse(buffer)?)),
            MessageType::SFOFaultHandlerOverride => Ok(Self::SFOFaultHandlerOverride(
                FaultHandlerOverride::decode(buffer)?,
            )),
            MessageType::SFOFilestoreRequest => {
                Ok(Self::SFOFilestoreRequest(FilestoreRequest::parse(buffer)?))
            }
            MessageType::SFOReport => Ok(Self::SFOReport(SFOReport::parse(buffer)?)),
            MessageType::SFOFilestoreResponse => Ok(Self::SFOFilestoreResponse(
                FilestoreResponse::parse(buffer)?,
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservedMessageHeader {
    message_type: MessageType,
    length: u8,
    // length x 8 bytes
    value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginatingTransactionIDMessage {
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
}
impl OriginatingTransactionIDMessage {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);
        buffer
    }
    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyPutRequest {
    destination_entity_id: Vec<u8>,
    source_filename: Vec<u8>,
    destination_filename: Vec<u8>,
}
impl ProxyPutRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.destination_entity_id.len() as u8];
        buffer.extend(self.destination_entity_id);

        buffer.push(self.source_filename.len() as u8);
        buffer.extend(self.source_filename);

        buffer.push(self.destination_filename.len() as u8);
        buffer.extend(self.destination_filename);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let destination_entity_id = read_length_value_pair(buffer)?;
        let source_filename = read_length_value_pair(buffer)?;
        let destination_filename = read_length_value_pair(buffer)?;

        Ok(Self {
            destination_entity_id,
            source_filename,
            destination_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyPutResponse {
    condition: Condition,
    delivery_code: DeliveryCode,
    file_status: FileStatusCode,
}
impl ProxyPutResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let byte = ((self.condition as u8) << 4)
            | ((self.delivery_code as u8) << 2)
            | self.file_status as u8;
        vec![byte]
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let byte = u8_buff[0];
        let condition = {
            let possible_condition = (byte & 0xF0) >> 4;
            Condition::from_u8(possible_condition)
                .ok_or(PDUError::InvalidCondition(possible_condition))?
        };

        let delivery_code = {
            let possible_code = (byte & 0x4) >> 2;
            DeliveryCode::from_u8(possible_code)
                .ok_or(PDUError::InvalidDeliveryCode(possible_code))?
        };

        let file_status = {
            let possible_status = byte & 0x3;
            FileStatusCode::from_u8(possible_status)
                .ok_or(PDUError::InvalidFileStatus(possible_status))?
        };

        Ok(Self {
            condition,
            delivery_code,
            file_status,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyMessageToUser {
    message_text: Vec<u8>,
}
impl ProxyMessageToUser {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.message_text.len() as u8];
        buffer.extend(self.message_text);
        buffer
    }
    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let message_text = read_length_value_pair(buffer)?;
        Ok(Self { message_text })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyTransmissionMode {
    mode: TransmissionMode,
}
impl ProxyTransmissionMode {
    pub fn to_bytes(self) -> Vec<u8> {
        vec![self.mode as u8]
    }
    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mode = {
            let mut u8_buff = [0u8; 1];
            buffer.read_exact(&mut u8_buff)?;
            let possible_mode = u8_buff[0];
            TransmissionMode::from_u8(possible_mode)
                .ok_or(PDUError::InvalidTransmissionMode(possible_mode))?
        };
        Ok(Self { mode })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyFlowLabel {
    value: Vec<u8>,
}
impl ProxyFlowLabel {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.value.len() as u8];
        buffer.extend(self.value);
        buffer
    }
    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let value = read_length_value_pair(buffer)?;
        Ok(Self { value })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxySegmentationControl {
    control: SegmentationControl,
}
impl ProxySegmentationControl {
    pub fn to_bytes(self) -> Vec<u8> {
        vec![self.control as u8]
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let control = {
            let mut u8_buff = [0u8; 1];
            buffer.read_exact(&mut u8_buff)?;
            let possible_control = u8_buff[0];
            SegmentationControl::from_u8(possible_control)
                .ok_or(PDUError::InvalidSegmentControl(possible_control))?
        };

        Ok(Self { control })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyFilestoreRequest {
    value: Vec<u8>,
}
impl ProxyFilestoreRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.value.len() as u8];
        buffer.extend(self.value);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let value = read_length_value_pair(buffer)?;
        Ok(Self { value })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyFilestoreResponse {
    value: Vec<u8>,
}
impl ProxyFilestoreResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.value.len() as u8];
        buffer.extend(self.value);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let value = read_length_value_pair(buffer)?;
        Ok(Self { value })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryListingRequest {
    directory_name: Vec<u8>,
    directory_filename: Vec<u8>,
}
impl DirectoryListingRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.directory_name.len() as u8];
        buffer.extend(self.directory_name);

        buffer.push(self.directory_filename.len() as u8);
        buffer.extend(self.directory_filename);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let directory_name = read_length_value_pair(buffer)?;
        let directory_filename = read_length_value_pair(buffer)?;

        Ok(Self {
            directory_name,
            directory_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListingResponseCode {
    Successful = 0x007F,
    Unsuccessful = 0x80FF,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryListingResponse {
    response_code: u8,
    directory_name: Vec<u8>,
    directory_filename: Vec<u8>,
}
impl DirectoryListingResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.response_code];

        buffer.push(self.directory_name.len() as u8);
        buffer.extend(self.directory_name);

        buffer.push(self.directory_filename.len() as u8);
        buffer.extend(self.directory_filename);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let response_code = u8_buff[0];

        let directory_name = read_length_value_pair(buffer)?;
        let directory_filename = read_length_value_pair(buffer)?;

        Ok(Self {
            response_code,
            directory_name,
            directory_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteStatusReportRequest {
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
    report_filename: Vec<u8>,
}
impl RemoteStatusReportRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);

        buffer.push(self.report_filename.len() as u8);
        buffer.extend(self.report_filename);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        buffer.read_exact(&mut u8_buff)?;
        let filename_len = u8_buff[0];
        let mut report_filename = vec![0u8; filename_len as usize];
        buffer.read_exact(&mut report_filename)?;

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
            report_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteStatusReportResponse {
    transaction_status: TransactionStatus,
    response_code: bool,
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
}
impl RemoteStatusReportResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte: u8 = ((self.transaction_status as u8) << 6) | (self.response_code as u8);
        buffer.push(first_byte);

        let second_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(second_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let transaction_status = {
            let status = (first_byte & 0xC0) >> 6;
            TransactionStatus::from_u8(status).ok_or(PDUError::InvalidTransactionStatus(status))?
        };

        let response_code = (first_byte & 0x1) != 0;

        buffer.read_exact(&mut u8_buff)?;
        let second_byte = u8_buff[0];

        let entity_id_len = ((second_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (second_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        Ok(Self {
            transaction_status,
            response_code,
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSuspendRequest {
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
}
impl RemoteSuspendRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSuspendResponse {
    suspend_indication: bool,
    transaction_status: TransactionStatus,
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
}
impl RemoteSuspendResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte: u8 =
            ((self.suspend_indication as u8) << 7) | ((self.transaction_status as u8) << 5);
        buffer.push(first_byte);

        let second_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(second_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let suspend_indication = ((first_byte & 0x80) >> 7) != 0;
        let transaction_status = {
            let status = (first_byte & 0x60) >> 5;
            TransactionStatus::from_u8(status).ok_or(PDUError::InvalidTransactionStatus(status))?
        };

        buffer.read_exact(&mut u8_buff)?;
        let second_byte = u8_buff[0];

        let entity_id_len = ((second_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (second_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        Ok(Self {
            suspend_indication,
            transaction_status,
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteResumeRequest {
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
}
impl RemoteResumeRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteResumeResponse {
    suspend_indication: bool,
    transaction_status: TransactionStatus,
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
}
impl RemoteResumeResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte: u8 =
            ((self.suspend_indication as u8) << 7) | ((self.transaction_status as u8) << 5);
        buffer.push(first_byte);

        let second_byte = (((self.source_entity_id.len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.len() as u8 - 1u8) & 0x3);
        buffer.push(second_byte);

        buffer.extend(self.source_entity_id);
        buffer.extend(self.transaction_sequence_number);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let suspend_indication = ((first_byte & 0x80) >> 7) != 0;
        let transaction_status = {
            let status = (first_byte & 0x30) >> 5;
            TransactionStatus::from_u8(status).ok_or(PDUError::InvalidTransactionStatus(status))?
        };

        buffer.read_exact(&mut u8_buff)?;
        let second_byte = u8_buff[0];

        let entity_id_len = ((second_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (second_byte & 0x7) + 1;

        let mut source_entity_id = vec![0u8; entity_id_len as usize];
        buffer.read_exact(&mut source_entity_id)?;

        let mut transaction_sequence_number = vec![0u8; transaction_seq_len as usize];
        buffer.read_exact(&mut transaction_sequence_number)?;

        Ok(Self {
            suspend_indication,
            transaction_status,
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SFORequest {
    trace_control: TraceControl,
    transmission_mode: TransmissionMode,
    segment_control: SegmentationControl,
    closure_request: bool,
    prior_waypoints_count: u8,
    request_label: Vec<u8>,
    source_entity_id: Vec<u8>,
    destination_entity_id: Vec<u8>,
    source_filename: Vec<u8>,
    destination_filename: Vec<u8>,
}
impl SFORequest {
    pub fn to_bytes(self) -> Vec<u8> {
        // trace control flag is a u2 but haven't seen a definition of it yet
        let first_byte: u8 = ((self.trace_control as u8) << 6)
            | ((self.transmission_mode as u8) << 5)
            | ((self.segment_control as u8) << 4)
            | ((self.closure_request as u8) << 3);
        let mut buffer = vec![first_byte as u8];
        buffer.push(self.prior_waypoints_count);

        buffer.push(self.request_label.len() as u8);
        buffer.extend(self.request_label);

        buffer.push(self.source_entity_id.len() as u8);
        buffer.extend(self.source_entity_id);

        buffer.push(self.destination_entity_id.len() as u8);
        buffer.extend(self.destination_entity_id);

        buffer.push(self.source_filename.len() as u8);
        buffer.extend(self.source_filename);

        buffer.push(self.destination_filename.len() as u8);
        buffer.extend(self.destination_filename);

        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let trace_control = {
            let possible_control = (first_byte & 0xc0) >> 6;
            TraceControl::from_u8(possible_control)
                .ok_or(PDUError::InvalidTraceControl(possible_control))?
        };

        let transmission_mode = {
            let possible_mode = (first_byte & 0x20) >> 5;
            TransmissionMode::from_u8(possible_mode)
                .ok_or(PDUError::InvalidTransmissionMode(possible_mode))?
        };

        let segment_control = {
            let possible_segment = (first_byte & 0x10) >> 4;
            SegmentationControl::from_u8(possible_segment)
                .ok_or(PDUError::InvalidSegmentControl(possible_segment))?
        };

        let closure_request = ((first_byte & 0x8) >> 3) != 0;

        buffer.read_exact(&mut u8_buff)?;
        let prior_waypoints_count = u8_buff[0];

        let request_label = read_length_value_pair(buffer)?;
        let source_entity_id = read_length_value_pair(buffer)?;
        let destination_entity_id = read_length_value_pair(buffer)?;
        let source_filename = read_length_value_pair(buffer)?;
        let destination_filename = read_length_value_pair(buffer)?;

        Ok(Self {
            trace_control,
            transmission_mode,
            segment_control,
            closure_request,
            prior_waypoints_count,
            request_label,
            source_entity_id,
            destination_entity_id,
            source_filename,
            destination_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SFOMessageToUser {
    message: Vec<u8>,
}
impl SFOMessageToUser {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.message.len() as u8];
        buffer.extend(self.message);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let message = read_length_value_pair(buffer)?;

        Ok(Self { message })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SFOFlowLabel {
    flow_label: Vec<u8>,
}
impl SFOFlowLabel {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.flow_label.len() as u8];
        buffer.extend(self.flow_label);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let flow_label = read_length_value_pair(buffer)?;

        Ok(Self { flow_label })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SFOFilestoreRequest {
    filestore_message: Vec<u8>,
}
impl SFOFilestoreRequest {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.filestore_message.len() as u8];
        buffer.extend(self.filestore_message);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let filestore_message = read_length_value_pair(buffer)?;

        Ok(Self { filestore_message })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SFOReport {
    request_label: Vec<u8>,
    source_entity_id: Vec<u8>,
    destination_entity_id: Vec<u8>,
    reporting_entity_id: Vec<u8>,
    prior_waypoints: u8,
    report_code: u8,
    condition: Condition,
    direction: Direction,
    delivery_code: DeliveryCode,
    file_status: FileStatusCode,
}
impl SFOReport {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.request_label.len() as u8];
        buffer.extend(self.request_label);
        buffer.push(self.source_entity_id.len() as u8);
        buffer.extend(self.source_entity_id);
        buffer.push(self.destination_entity_id.len() as u8);
        buffer.extend(self.destination_entity_id);
        buffer.push(self.reporting_entity_id.len() as u8);
        buffer.extend(self.reporting_entity_id);
        buffer.push(self.prior_waypoints);
        buffer.push(self.report_code);
        let last_byte: u8 = self.condition.to_u8().unwrap() << 4
            | self.direction.to_u8().unwrap() << 3
            | self.delivery_code.to_u8().unwrap() << 2
            | self.file_status.to_u8().unwrap();
        buffer.push(last_byte);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let mut u8_buff = [0u8; 1];
        let request_label = read_length_value_pair(buffer)?;
        let source_entity_id = read_length_value_pair(buffer)?;
        let destination_entity_id = read_length_value_pair(buffer)?;
        let reporting_entity_id = read_length_value_pair(buffer)?;

        buffer.read_exact(&mut u8_buff)?;
        let prior_waypoints = u8_buff[0];

        buffer.read_exact(&mut u8_buff)?;
        let report_code = u8_buff[0];

        buffer.read_exact(&mut u8_buff)?;
        let last_byte = u8_buff[0];

        let condition = {
            let possible_condition = (last_byte & 0xf0) >> 4;
            Condition::from_u8(possible_condition)
                .ok_or(PDUError::InvalidCondition(possible_condition))?
        };
        let direction = {
            let possible_direction = (last_byte & 0x8) >> 3;
            Direction::from_u8(possible_direction)
                .ok_or(PDUError::InvalidDirection(possible_direction))?
        };

        let delivery_code = {
            let possible_delivery = (last_byte & 0x4) >> 2;
            DeliveryCode::from_u8(possible_delivery)
                .ok_or(PDUError::InvalidDeliveryCode(possible_delivery))?
        };

        let file_status = {
            let status = last_byte & 0x3;
            FileStatusCode::from_u8(status).ok_or(PDUError::InvalidDeliveryCode(status))?
        };

        Ok(Self {
            request_label,
            source_entity_id,
            destination_entity_id,
            reporting_entity_id,
            prior_waypoints,
            report_code,
            condition,
            direction,
            delivery_code,
            file_status,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SFOFilestoreResponse {
    filestore_response: Vec<u8>,
}
impl SFOFilestoreResponse {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buffer = vec![self.filestore_response.len() as u8];
        buffer.extend_from_slice(&self.filestore_response);
        buffer
    }

    pub fn parse<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let filestore_response = read_length_value_pair(buffer)?;

        Ok(Self { filestore_response })
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::too_many_arguments)]
    use super::*;

    use crate::{
        assert_err,
        pdu::{
            fault_handler::HandlerCode,
            filestore::{DenyStatus, FilestoreAction, FilestoreStatus, RenameStatus},
        },
    };

    use rstest::rstest;

    #[rstest]
    fn sfo_response_roundtrip(
        #[values((0..20).collect(), (50..255).collect(), (0..10).collect(), "beep booop, I'm a bot".as_bytes().to_vec())]
        input_request: Vec<u8>,
    ) {
        let expected = SFOFilestoreResponse {
            filestore_response: input_request,
        };
        let buffer = expected.clone().to_bytes();
        let recovered = SFOFilestoreResponse::parse(&mut buffer.as_slice()).unwrap();
        assert_eq!(expected, recovered)
    }

    #[rstest]
    #[case((0..5).collect(), vec![1u8, 3, 5, 7, 9], 3475392u32.to_be_bytes().to_vec(), 1948582103u64.to_be_bytes().to_vec(), 5, 255, Direction::ToReceiver, DeliveryCode::Complete,)]
    #[case((200..243).collect(), vec![2u8, 4, 12, 55, 192], 555184857u64.to_be_bytes().to_vec(), 128374u32.to_be_bytes().to_vec(), 153, 2, Direction::ToSender, DeliveryCode::Incomplete )]
    fn sfo_report_roundtrip(
        #[case] request_label: Vec<u8>,
        #[case] source_entity_id: Vec<u8>,
        #[case] destination_entity_id: Vec<u8>,
        #[case] reporting_entity_id: Vec<u8>,
        #[case] prior_waypoints: u8,
        #[case] report_code: u8,
        #[values(
            Condition::NoError,
            Condition::PositiveLimitReached,
            Condition::NakLimitReached
        )]
        condition: Condition,
        #[case] direction: Direction,
        #[case] delivery_code: DeliveryCode,
        #[values(
            FileStatusCode::Discarded,
            FileStatusCode::Unreported,
            FileStatusCode::Retained
        )]
        file_status: FileStatusCode,
    ) {
        let expected = SFOReport {
            request_label,
            source_entity_id,
            destination_entity_id,
            reporting_entity_id,
            prior_waypoints,
            report_code,
            condition,
            direction,
            delivery_code,
            file_status,
        };
        let buffer = expected.clone().to_bytes();
        let recovered = SFOReport::parse(&mut buffer.as_slice()).unwrap();
        assert_eq!(expected, recovered)
    }

    #[test]
    fn user_op_bad_sync() {
        let mut buffer = USER_OPS_IDENTIFIER.to_vec();
        buffer[0] -= 10;
        assert_err!(
            UserOperation::decode(&mut &buffer[..]),
            Err(PDUError::UnexpectedIdentifier(_, _))
        )
    }

    #[rstest]
    #[case::transaction_id(UserOperation::OriginatingTransactionIDMessage(
        OriginatingTransactionIDMessage{
            source_entity_id: 2467867u32.to_be_bytes().to_vec(),
            transaction_sequence_number: 11123132u32.to_be_bytes().to_vec()
        })
    )]
    #[case::proxy_put_request(
        UserOperation::ProxyPutRequest(ProxyPutRequest{
            destination_entity_id: 2398u32.to_be_bytes().to_vec(),
            source_filename: "test_please.txt".as_bytes().to_vec(),
            destination_filename: "new_test_please.dat".as_bytes().to_vec()
        })
    )]
    #[case::proxy_put_response(
        UserOperation::ProxyPutResponse(ProxyPutResponse{
            condition: Condition::CancelReceived,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported
        })
    )]
    #[case::proxy_message(
        UserOperation::ProxyMessageToUser(ProxyMessageToUser{
            message_text: "Test Hello World".as_bytes().to_vec()
        })
    )]
    #[case::proxy_filestore_request(UserOperation::ProxyFilestoreRequest(
        FilestoreRequest{
            action_code: FilestoreAction::AppendFile,
            first_filename: "/the/first/file/to/append.dat".as_bytes().to_vec(),
            second_filename: "/an/additional/file/to/append.txt".as_bytes().to_vec(),
        }
    ))]
    #[case::proxy_filestore_response(UserOperation::ProxyFilestoreResponse(
        FilestoreResponse{
            action_and_status: FilestoreStatus::DenyDirectory(DenyStatus::NotAllowed),
            first_filename: "/this/is/a/test/directory/".as_bytes().to_vec(),
            second_filename: vec![],
            filestore_message: vec![]
        }
    ))]
    #[case::proxy_fault_override(UserOperation::ProxyFaultHandlerOverride(
        FaultHandlerOverride{
            fault_handler_code: HandlerCode::IgnoreError
        }
    ))]
    #[case::transmission_mode(UserOperation::ProxyTransmissionMode(
        ProxyTransmissionMode{
            mode: TransmissionMode::Unacknowledged
        }
    ))]
    #[case::flow_label(UserOperation::ProxyFlowLabel(
        ProxyFlowLabel{
            value: "THis is a test".as_bytes().to_vec()
        }
    ))]
    #[case::segmentation_control(UserOperation::ProxySegmentationControl(
        ProxySegmentationControl{
            control: SegmentationControl::Preserved
        }
    ))]
    #[case::proxy_put_cancel(UserOperation::ProxyPutCancel)]
    #[case::directory_listing_request(UserOperation::DirectoryListingRequest(
        DirectoryListingRequest{
            directory_name: "/home/user/help".as_bytes().to_vec(),
            directory_filename: "/home/me/this_is_Result.txt".as_bytes().to_vec(),
        }
    ))]
    #[case::directory_listing_response(
        UserOperation::DirectoryListingResponse(DirectoryListingResponse{
            response_code: 215u8,
            directory_name: "/home/user/help22".as_bytes().to_vec(),
            directory_filename: "/home/me/this_is_Result11.txt".as_bytes().to_vec(),
        })
    )]
    #[case::remote_staus_report_request(
        UserOperation::RemoteStatusReportRequest(RemoteStatusReportRequest{
            source_entity_id: 786567183u32.to_be_bytes().to_vec(),
            transaction_sequence_number: (u32::MAX - 3u32).to_be_bytes().to_vec(),
            report_filename: "foobar".as_bytes().to_vec(),
        })

    )]
    #[case::remote_status_report_response(
        UserOperation::RemoteStatusReportResponse(
            RemoteStatusReportResponse{
                transaction_status: TransactionStatus::Unrecognized,
                response_code: true,
                source_entity_id: 130875758u32.to_be_bytes().to_vec(),
                transaction_sequence_number: 27374848u32.to_be_bytes().to_vec(),
            }
    ))]
    #[case::remote_suspend_request(
        UserOperation::RemoteSuspendRequest(
            RemoteSuspendRequest{
                source_entity_id: 8845748u32.to_be_bytes().to_vec(),
                transaction_sequence_number: (u32::MAX - u32::MAX /2).to_be_bytes().to_vec(),
            }
    ))]
    #[case::remote_suspend_response(
        UserOperation::RemoteSuspendResponse(
            RemoteSuspendResponse{
                suspend_indication: true,
                transaction_status: TransactionStatus::Terminated,
                source_entity_id: u32::MAX.to_be_bytes().to_vec(),
                transaction_sequence_number: 7823454u32.to_be_bytes().to_vec(),
            }
    ))]
    #[case::remote_resume_request(
        UserOperation::RemoteResumeRequest(
            RemoteResumeRequest{
                source_entity_id: 20058583u32.to_be_bytes().to_vec(),
                transaction_sequence_number: 850895721u32.to_be_bytes().to_vec(),
            }
    ))]
    #[case::remote_resume_response(
        UserOperation::RemoteResumeResponse(
            RemoteResumeResponse{
                suspend_indication: true,
                transaction_status: TransactionStatus::Active,
                source_entity_id: 2045853u32.to_be_bytes().to_vec(),
                transaction_sequence_number: 85790329u32.to_be_bytes().to_vec(),
            }
    ))]
    #[case::sfo_request(
        UserOperation::SFORequest(
            SFORequest{
                trace_control: TraceControl::BothDirections,
                transmission_mode: TransmissionMode::Unacknowledged,
                segment_control: SegmentationControl::Preserved,
                closure_request: true,
                prior_waypoints_count: u8::MAX,
                request_label: vec![135u8, 85, 88, 127, 129,],
                source_entity_id: 873123u32.to_be_bytes().to_vec(),
                destination_entity_id: 9887373u32.to_be_bytes().to_vec(),
                source_filename: "/test/test/test.test".as_bytes().to_vec(),
                destination_filename: "notest/notest/notest".as_bytes().to_vec(),
            }
    ))]
    #[case::sfo_message(UserOperation::SFOMessageToUser(
        SFOMessageToUser{
            message: "This is a test message!".as_bytes().to_vec(),
    }))]
    #[case::sfo_flow_label(
        UserOperation::SFOFlowLabel(
            SFOFlowLabel{
                flow_label: vec![1u8, 3u8, 5u8, 7u8, 11u8, 13u8, 17u8]
            }
    ))]
    #[case::sfo_fault_handler_override(
        UserOperation::SFOFaultHandlerOverride(
            FaultHandlerOverride{
                fault_handler_code: HandlerCode::NoticeOfSuspension
            }
    ))]
    #[case::sfo_filestore_request(UserOperation::SFOFilestoreRequest(
        FilestoreRequest{
            action_code: FilestoreAction::RemoveDirectory,
            first_filename: "/home/user/ops".as_bytes().to_vec(),
            second_filename: vec![],
        }
    ))]
    #[case::sfo_filestore_response(UserOperation::SFOFilestoreResponse(
        FilestoreResponse{
           action_and_status: FilestoreStatus::RenameFile(RenameStatus::NotPerformed),
           first_filename: "/this/is/a/test.dat".as_bytes().to_vec(),
           second_filename: "/another/test/file.txt".as_bytes().to_vec(),
           filestore_message: vec![]
        }
    ))]
    #[case::sfo_report(UserOperation::SFOReport(
        SFOReport{
            request_label: vec![1u8, 2u8, 4u8, 8u8, 16u8, 32u8, 64u8],
            source_entity_id: 2847837u32.to_be_bytes().to_vec(),
            destination_entity_id: 7573837u32.to_be_bytes().to_vec(),
            reporting_entity_id: 995857u32.to_be_bytes().to_vec(),
            prior_waypoints: 83u8,
            report_code: 213u8,
            condition: Condition::CheckLimitReached,
            direction: Direction::ToSender,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::FilestoreRejection
        }
    ))]
    fn user_op_roundtrip(#[case] expected: UserOperation) {
        let buffer = expected.clone().encode();
        let recovered = UserOperation::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }
}
