use std::io::Read;

use camino::Utf8PathBuf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use super::{
    error::{PDUError, PDUResult},
    fault_handler::FaultHandlerOverride,
    filestore::{FileStoreRequest, FileStoreResponse},
    header::{
        read_length_value_pair, Condition, DeliveryCode, Direction, FileStatusCode, MessageType,
        PDUEncode, SegmentationControl, TraceControl, TransactionStatus, TransmissionMode,
    },
    ops::{EntityID, FlowLabel, MessageToUser, TransactionSeqNum},
};

const USER_OPS_IDENTIFIER: &[u8] = "cfdp".as_bytes();

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyOperation {
    ProxyPutRequest(ProxyPutRequest),
    ProxyMessageToUser(MessageToUser),
    ProxyFileStoreRequest(FileStoreRequest),
    ProxyFaultHandlerOverride(FaultHandlerOverride),
    ProxyTransmissionMode(TransmissionMode),
    ProxyFlowLabel(FlowLabel),
    ProxySegmentationControl(ProxySegmentationControl),
    ProxyPutCancel,
}
impl ProxyOperation {
    pub fn get_message_type(&self) -> MessageType {
        match self {
            Self::ProxyPutRequest(_) => MessageType::ProxyPutRequest,
            Self::ProxyMessageToUser(_) => MessageType::ProxyMessageToUser,
            Self::ProxyFileStoreRequest(_) => MessageType::ProxyFileStoreRequest,
            Self::ProxyFaultHandlerOverride(_) => MessageType::ProxyFaultHandlerOverride,
            Self::ProxyTransmissionMode(_) => MessageType::ProxyTransmissionMode,
            Self::ProxyFlowLabel(_) => MessageType::ProxyFlowLabel,
            Self::ProxySegmentationControl(_) => MessageType::ProxySegmentationControl,
            Self::ProxyPutCancel => MessageType::ProxyPutCancel,
        }
    }

    fn encoded_len(&self) -> u16 {
        match self {
            Self::ProxyPutRequest(inner) => inner.encoded_len(),
            Self::ProxyMessageToUser(inner) => inner.encoded_len(),
            // add one here to account for the len field
            Self::ProxyFileStoreRequest(inner) => 1 + inner.encoded_len(),
            Self::ProxyFaultHandlerOverride(inner) => inner.encoded_len(),
            Self::ProxyTransmissionMode(inner) => inner.encoded_len(),
            Self::ProxyFlowLabel(inner) => inner.encoded_len(),
            Self::ProxySegmentationControl(inner) => inner.encoded_len(),
            Self::ProxyPutCancel => 0,
        }
    }
    fn encode(self) -> Vec<u8> {
        match self {
            Self::ProxyPutRequest(msg) => msg.encode(),
            Self::ProxyMessageToUser(msg) => msg.encode(),
            Self::ProxyFileStoreRequest(msg) => {
                let mut bytes = msg.encode();
                bytes.insert(0, bytes.len() as u8);
                bytes
            }
            Self::ProxyFaultHandlerOverride(msg) => msg.encode(),
            Self::ProxyTransmissionMode(msg) => msg.encode(),
            Self::ProxyFlowLabel(msg) => msg.encode(),
            Self::ProxySegmentationControl(msg) => msg.encode(),
            Self::ProxyPutCancel => vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserResponse {
    ProxyPut(ProxyPutResponse),
    ProxyFileStore(FileStoreResponse),
    DirectoryListing(DirectoryListingResponse),
    RemoteStatusReport(RemoteStatusReportResponse),
    RemoteResume(RemoteResumeResponse),
    RemoteSuspend(RemoteSuspendResponse),
}
impl UserResponse {
    pub fn encoded_len(&self) -> u16 {
        match self {
            Self::ProxyPut(inner) => inner.encoded_len(),
            Self::ProxyFileStore(inner) => 1 + inner.encoded_len(),
            Self::DirectoryListing(inner) => inner.encoded_len(),
            Self::RemoteStatusReport(inner) => inner.encoded_len(),
            Self::RemoteResume(inner) => inner.encoded_len(),
            Self::RemoteSuspend(inner) => inner.encoded_len(),
        }
    }
    pub fn get_message_type(&self) -> MessageType {
        match self {
            Self::ProxyPut(_) => MessageType::ProxyPutResponse,
            Self::ProxyFileStore(_) => MessageType::ProxyFileStoreResponse,
            Self::DirectoryListing(_) => MessageType::DirectoryListingResponse,
            Self::RemoteStatusReport(_) => MessageType::RemoteStatusReportResponse,
            Self::RemoteSuspend(_) => MessageType::RemoteSuspendResponse,
            Self::RemoteResume(_) => MessageType::RemoteResumeResponse,
        }
    }
    fn encode(self) -> Vec<u8> {
        match self {
            Self::ProxyPut(msg) => msg.encode(),
            Self::ProxyFileStore(msg) => {
                let mut bytes = msg.encode();
                bytes.insert(0, bytes.len() as u8);
                bytes
            }
            Self::DirectoryListing(msg) => msg.encode(),
            Self::RemoteStatusReport(msg) => msg.encode(),
            Self::RemoteSuspend(msg) => msg.encode(),
            Self::RemoteResume(msg) => msg.encode(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserRequest {
    DirectoryListing(DirectoryListingRequest),
    RemoteStatusReport(RemoteStatusReportRequest),
    RemoteSuspend(RemoteSuspendRequest),
    RemoteResume(RemoteResumeRequest),
}
impl UserRequest {
    pub fn encoded_len(&self) -> u16 {
        match self {
            Self::DirectoryListing(inner) => inner.encoded_len(),
            Self::RemoteStatusReport(inner) => inner.encoded_len(),
            Self::RemoteSuspend(inner) => inner.encoded_len(),
            Self::RemoteResume(inner) => inner.encoded_len(),
        }
    }
    pub fn get_message_type(&self) -> MessageType {
        match self {
            Self::DirectoryListing(_) => MessageType::DirectoryListingRequest,
            Self::RemoteStatusReport(_) => MessageType::RemoteStatusReportRequest,
            Self::RemoteSuspend(_) => MessageType::RemoteSuspendRequest,
            Self::RemoteResume(_) => MessageType::RemoteResumeRequest,
        }
    }

    fn encode(self) -> Vec<u8> {
        match self {
            Self::DirectoryListing(msg) => msg.encode(),
            Self::RemoteStatusReport(msg) => msg.encode(),
            Self::RemoteSuspend(msg) => msg.encode(),
            Self::RemoteResume(msg) => msg.encode(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// User Operations are transported as the paylod of a [MessageToUser]
/// in a metadataPDU. A reserved "CFDP" identifier is used to delinate User Operations.
pub enum UserOperation {
    OriginatingTransactionIDMessage(OriginatingTransactionIDMessage),
    ProxyOperation(ProxyOperation),
    Response(UserResponse),
    Request(UserRequest),

    SFORequest(SFORequest),
    SFOMessageToUser(MessageToUser),
    SFOFlowLabel(FlowLabel),
    SFOFaultHandlerOverride(FaultHandlerOverride),
    SFOFileStoreRequest(FileStoreRequest),
    SFOFileStoreResponse(FileStoreResponse),
    SFOReport(SFOReport),
}

impl UserOperation {
    pub fn get_message_type(&self) -> MessageType {
        match self {
            Self::OriginatingTransactionIDMessage(_) => {
                MessageType::OriginatingTransactionIDMessage
            }
            Self::ProxyOperation(op) => op.get_message_type(),
            Self::Response(msg) => msg.get_message_type(),
            Self::Request(response) => response.get_message_type(),
            Self::SFORequest(_) => MessageType::SFORequest,
            Self::SFOMessageToUser(_) => MessageType::SFOMessageToUser,
            Self::SFOFlowLabel(_) => MessageType::SFOFlowLabel,
            Self::SFOFaultHandlerOverride(_) => MessageType::SFOFaultHandlerOverride,
            Self::SFOFileStoreRequest(_) => MessageType::SFOFileStoreRequest,
            Self::SFOFileStoreResponse(_) => MessageType::SFOFileStoreResponse,
            Self::SFOReport(_) => MessageType::SFOReport,
        }
    }
}
impl PDUEncode for UserOperation {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        USER_OPS_IDENTIFIER.len() as u16
            + 1
            + match self {
                Self::OriginatingTransactionIDMessage(inner) => inner.encoded_len(),
                Self::ProxyOperation(inner) => inner.encoded_len(),
                Self::Response(inner) => inner.encoded_len(),
                Self::Request(inner) => inner.encoded_len(),
                Self::SFORequest(inner) => inner.encoded_len(),
                Self::SFOMessageToUser(inner) => inner.encoded_len(),
                Self::SFOFlowLabel(inner) => inner.encoded_len(),
                Self::SFOFaultHandlerOverride(inner) => inner.encoded_len(),
                // add one to accound for the length field
                Self::SFOFileStoreRequest(inner) => 1 + inner.encoded_len(),
                // add one to accound for the length field
                Self::SFOFileStoreResponse(inner) => 1 + inner.encoded_len(),
                Self::SFOReport(inner) => inner.encoded_len(),
            }
    }
    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = USER_OPS_IDENTIFIER.to_vec();
        buffer.push(self.get_message_type() as u8);
        let message_buffer = match self {
            Self::OriginatingTransactionIDMessage(msg) => msg.encode(),
            Self::ProxyOperation(op) => op.encode(),
            Self::Response(msg) => msg.encode(),
            Self::Request(msg) => msg.encode(),

            Self::SFORequest(msg) => msg.encode(),
            Self::SFOMessageToUser(msg) => msg.encode(),
            Self::SFOFlowLabel(msg) => msg.encode(),
            Self::SFOFaultHandlerOverride(msg) => msg.encode(),
            Self::SFOFileStoreRequest(msg) => {
                let mut bytes = msg.encode();
                bytes.insert(0, bytes.len() as u8);
                bytes
            }
            Self::SFOFileStoreResponse(msg) => {
                let mut bytes = msg.encode();
                bytes.insert(0, bytes.len() as u8);
                bytes
            }
            Self::SFOReport(msg) => msg.encode(),
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
            MessageType::ProxyPutRequest => Ok(Self::ProxyOperation(
                ProxyOperation::ProxyPutRequest(ProxyPutRequest::decode(buffer)?),
            )),
            MessageType::ProxyMessageToUser => Ok(Self::ProxyOperation(
                ProxyOperation::ProxyMessageToUser(MessageToUser::decode(buffer)?),
            )),
            MessageType::ProxyFileStoreRequest => {
                let mut u8_buff = [0u8];
                buffer.read_exact(&mut u8_buff)?;
                Ok(Self::ProxyOperation(ProxyOperation::ProxyFileStoreRequest(
                    FileStoreRequest::decode(buffer)?,
                )))
            }
            MessageType::ProxyFileStoreResponse => {
                let mut u8_buff = [0u8];
                buffer.read_exact(&mut u8_buff)?;
                Ok(Self::Response(UserResponse::ProxyFileStore(
                    FileStoreResponse::decode(buffer)?,
                )))
            }

            MessageType::ProxyFaultHandlerOverride => Ok(Self::ProxyOperation(
                ProxyOperation::ProxyFaultHandlerOverride(FaultHandlerOverride::decode(buffer)?),
            )),
            MessageType::ProxyTransmissionMode => Ok(Self::ProxyOperation(
                ProxyOperation::ProxyTransmissionMode(TransmissionMode::decode(buffer)?),
            )),
            MessageType::ProxyFlowLabel => Ok(Self::ProxyOperation(
                ProxyOperation::ProxyFlowLabel(FlowLabel::decode(buffer)?),
            )),

            MessageType::ProxySegmentationControl => Ok(Self::ProxyOperation(
                ProxyOperation::ProxySegmentationControl(ProxySegmentationControl::decode(buffer)?),
            )),
            MessageType::ProxyPutResponse => Ok(Self::Response(UserResponse::ProxyPut(
                ProxyPutResponse::decode(buffer)?,
            ))),
            MessageType::ProxyPutCancel => Ok(Self::ProxyOperation(ProxyOperation::ProxyPutCancel)),
            MessageType::OriginatingTransactionIDMessage => {
                Ok(Self::OriginatingTransactionIDMessage(
                    OriginatingTransactionIDMessage::decode(buffer)?,
                ))
            }
            MessageType::ProxyClosureRequest => Err(PDUError::MessageType(u8_buff[0])),
            MessageType::DirectoryListingRequest => Ok(Self::Request(
                UserRequest::DirectoryListing(DirectoryListingRequest::decode(buffer)?),
            )),
            MessageType::RemoteStatusReportRequest => Ok(Self::Request(
                UserRequest::RemoteStatusReport(RemoteStatusReportRequest::decode(buffer)?),
            )),
            MessageType::RemoteSuspendRequest => Ok(Self::Request(UserRequest::RemoteSuspend(
                RemoteSuspendRequest::decode(buffer)?,
            ))),
            MessageType::RemoteResumeRequest => Ok(Self::Request(UserRequest::RemoteResume(
                RemoteResumeRequest::decode(buffer)?,
            ))),
            MessageType::DirectoryListingResponse => Ok(Self::Response(
                UserResponse::DirectoryListing(DirectoryListingResponse::decode(buffer)?),
            )),
            MessageType::RemoteStatusReportResponse => Ok(Self::Response(
                UserResponse::RemoteStatusReport(RemoteStatusReportResponse::decode(buffer)?),
            )),
            MessageType::RemoteSuspendResponse => Ok(Self::Response(UserResponse::RemoteSuspend(
                RemoteSuspendResponse::decode(buffer)?,
            ))),
            MessageType::RemoteResumeResponse => Ok(Self::Response(UserResponse::RemoteResume(
                RemoteResumeResponse::decode(buffer)?,
            ))),
            MessageType::SFORequest => Ok(Self::SFORequest(SFORequest::decode(buffer)?)),
            MessageType::SFOMessageToUser => {
                Ok(Self::SFOMessageToUser(MessageToUser::decode(buffer)?))
            }
            MessageType::SFOFlowLabel => Ok(Self::SFOFlowLabel(FlowLabel::decode(buffer)?)),
            MessageType::SFOFaultHandlerOverride => Ok(Self::SFOFaultHandlerOverride(
                FaultHandlerOverride::decode(buffer)?,
            )),
            MessageType::SFOFileStoreRequest => {
                let mut u8_buff = [0u8];
                buffer.read_exact(&mut u8_buff)?;
                Ok(Self::SFOFileStoreRequest(FileStoreRequest::decode(buffer)?))
            }
            MessageType::SFOReport => Ok(Self::SFOReport(SFOReport::decode(buffer)?)),
            MessageType::SFOFileStoreResponse => {
                let mut u8_buff = [0u8];
                buffer.read_exact(&mut u8_buff)?;
                Ok(Self::SFOFileStoreResponse(FileStoreResponse::decode(
                    buffer,
                )?))
            }
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
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
}
impl PDUEncode for OriginatingTransactionIDMessage {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + self.source_entity_id.encoded_len() + self.transaction_sequence_number.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());
        buffer
    }
    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let source_entity_id = {
            let mut buff = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut buff)?;
            EntityID::try_from(buff)?
        };

        let transaction_sequence_number = {
            let mut buff = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut buff)?;
            TransactionSeqNum::try_from(buff)?
        };

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyPutRequest {
    pub destination_entity_id: EntityID,
    pub source_filename: Utf8PathBuf,
    pub destination_filename: Utf8PathBuf,
}
impl PDUEncode for ProxyPutRequest {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + self.destination_entity_id.encoded_len()
            + 1
            + self.source_filename.as_str().len() as u16
            + 1
            + self.destination_filename.as_str().len() as u16
    }
    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.destination_entity_id.encoded_len() as u8];
        buffer.extend(self.destination_entity_id.to_be_bytes());

        let source_name = self.source_filename.as_str().as_bytes();
        buffer.push(source_name.len() as u8);
        buffer.extend(source_name);

        let dest_name = self.destination_filename.as_str().as_bytes();
        buffer.push(dest_name.len() as u8);
        buffer.extend(dest_name);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let destination_entity_id = EntityID::try_from(read_length_value_pair(buffer)?)?;
        let source_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);
        let destination_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        Ok(Self {
            destination_entity_id,
            source_filename,
            destination_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyPutResponse {
    pub condition: Condition,
    pub delivery_code: DeliveryCode,
    pub file_status: FileStatusCode,
}
impl PDUEncode for ProxyPutResponse {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1
    }

    fn encode(self) -> Vec<u8> {
        let byte = ((self.condition as u8) << 4)
            | ((self.delivery_code as u8) << 2)
            | self.file_status as u8;
        vec![byte]
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
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
pub struct ProxySegmentationControl {
    control: SegmentationControl,
}
impl PDUEncode for ProxySegmentationControl {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1
    }

    fn encode(self) -> Vec<u8> {
        vec![self.control as u8]
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
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
pub struct DirectoryListingRequest {
    pub directory_name: Utf8PathBuf,
    pub directory_filename: Utf8PathBuf,
}
impl PDUEncode for DirectoryListingRequest {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + self.directory_name.as_str().len() as u16
            + 1
            + self.directory_filename.as_str().len() as u16
    }
    fn encode(self) -> Vec<u8> {
        let dir_name = self.directory_name.as_str().as_bytes();
        let mut buffer = vec![dir_name.len() as u8];
        buffer.extend(dir_name);

        let dir_filename = self.directory_filename.as_str().as_bytes();
        buffer.push(dir_filename.len() as u8);
        buffer.extend(dir_filename);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let directory_name = Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);
        let directory_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        Ok(Self {
            directory_name,
            directory_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
#[repr(u8)]
pub enum ListingResponseCode {
    Successful = 0x00,
    Unsuccessful = 0x80,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryListingResponse {
    pub response_code: ListingResponseCode,
    pub directory_name: Utf8PathBuf,
    pub directory_filename: Utf8PathBuf,
}
impl PDUEncode for DirectoryListingResponse {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + 1
            + self.directory_name.as_str().len() as u16
            + 1
            + self.directory_filename.as_str().len() as u16
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.response_code as u8];

        let dir_name = self.directory_name.as_str().as_bytes();
        buffer.push(dir_name.len() as u8);
        buffer.extend(dir_name);

        let dir_filename = self.directory_filename.as_str().as_bytes();
        buffer.push(dir_filename.len() as u8);
        buffer.extend(dir_filename);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let response_code = {
            let possible = u8_buff[0];
            ListingResponseCode::from_u8(possible).ok_or(PDUError::InvalidListingCode(possible))?
        };

        let directory_name = Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        let directory_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        Ok(Self {
            response_code,
            directory_name,
            directory_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteStatusReportRequest {
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
    pub report_filename: Utf8PathBuf,
}
impl PDUEncode for RemoteStatusReportRequest {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + self.source_entity_id.encoded_len()
            + self.transaction_sequence_number.encoded_len()
            + 1
            + self.report_filename.as_str().len() as u16
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());

        let filename = self.report_filename.as_str().as_bytes();
        buffer.push(filename.len() as u8);
        buffer.extend(filename);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let source_entity_id = {
            let mut tmp_buffer = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            EntityID::try_from(tmp_buffer)?
        };

        let transaction_sequence_number = {
            let mut tmp_buffer = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            TransactionSeqNum::try_from(tmp_buffer)?
        };
        let report_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
            report_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteStatusReportResponse {
    pub transaction_status: TransactionStatus,
    pub response_code: bool,
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
}
impl PDUEncode for RemoteStatusReportResponse {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + 1 + self.source_entity_id.encoded_len() + self.transaction_sequence_number.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte: u8 = ((self.transaction_status as u8) << 6) | (self.response_code as u8);
        buffer.push(first_byte);

        let second_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(second_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
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

        let source_entity_id = {
            let mut tmp_buffer = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            EntityID::try_from(tmp_buffer)?
        };

        let transaction_sequence_number = {
            let mut tmp_buffer = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            TransactionSeqNum::try_from(tmp_buffer)?
        };

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
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
}
impl PDUEncode for RemoteSuspendRequest {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + self.source_entity_id.encoded_len() + self.transaction_sequence_number.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let source_entity_id = {
            let mut tmp_buffer = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            EntityID::try_from(tmp_buffer)?
        };

        let transaction_sequence_number = {
            let mut tmp_buffer = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            TransactionSeqNum::try_from(tmp_buffer)?
        };

        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSuspendResponse {
    pub suspend_indication: bool,
    pub transaction_status: TransactionStatus,
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
}
impl PDUEncode for RemoteSuspendResponse {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + 1 + self.transaction_sequence_number.encoded_len() + self.source_entity_id.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte: u8 =
            ((self.suspend_indication as u8) << 7) | ((self.transaction_status as u8) << 5);
        buffer.push(first_byte);

        let second_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(second_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
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

        let source_entity_id = {
            let mut tmp_buffer = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            EntityID::try_from(tmp_buffer)?
        };

        let transaction_sequence_number = {
            let mut tmp_buffer = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            TransactionSeqNum::try_from(tmp_buffer)?
        };

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
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
}
impl PDUEncode for RemoteResumeRequest {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + self.source_entity_id.encoded_len() + self.transaction_sequence_number.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(first_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let entity_id_len = ((first_byte & 0x70) >> 4) + 1;
        let transaction_seq_len = (first_byte & 0x7) + 1;

        let source_entity_id = {
            let mut tmp_buffer = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            EntityID::try_from(tmp_buffer)?
        };

        let transaction_sequence_number = {
            let mut tmp_buffer = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            TransactionSeqNum::try_from(tmp_buffer)?
        };
        Ok(Self {
            source_entity_id,
            transaction_sequence_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteResumeResponse {
    pub suspend_indication: bool,
    pub transaction_status: TransactionStatus,
    pub source_entity_id: EntityID,
    pub transaction_sequence_number: TransactionSeqNum,
}
impl PDUEncode for RemoteResumeResponse {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1 + 1 + self.source_entity_id.encoded_len() + self.transaction_sequence_number.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        let first_byte: u8 =
            ((self.suspend_indication as u8) << 7) | ((self.transaction_status as u8) << 5);
        buffer.push(first_byte);

        let second_byte = (((self.source_entity_id.encoded_len() as u8 - 1u8) & 0x3) << 4)
            | ((self.transaction_sequence_number.encoded_len() as u8 - 1u8) & 0x3);
        buffer.push(second_byte);

        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
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

        let source_entity_id = {
            let mut tmp_buffer = vec![0u8; entity_id_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            EntityID::try_from(tmp_buffer)?
        };

        let transaction_sequence_number = {
            let mut tmp_buffer = vec![0u8; transaction_seq_len as usize];
            buffer.read_exact(&mut tmp_buffer)?;
            TransactionSeqNum::try_from(tmp_buffer)?
        };
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
    source_entity_id: EntityID,
    destination_entity_id: EntityID,
    source_filename: Utf8PathBuf,
    destination_filename: Utf8PathBuf,
}
impl PDUEncode for SFORequest {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // trace control, transmission mode, segment mode and closure reques
        1
        // prior way points
        + 1
        // request label len + message
        + 1 + self.request_label.len() as u16
        // source entity id len + val
        + 1 + self.source_entity_id.encoded_len()
        // destination entity id len + val
        + 1 + self.destination_entity_id.encoded_len()
        // source filename len + val
        + 1 + self.source_filename.as_str().len() as u16
        // destination filename len + val
        + 1 + self.destination_filename.as_str().len() as u16
    }

    fn encode(self) -> Vec<u8> {
        // trace control flag is a u2 but haven't seen a definition of it yet
        let first_byte: u8 = ((self.trace_control as u8) << 6)
            | ((self.transmission_mode as u8) << 5)
            | ((self.segment_control as u8) << 4)
            | ((self.closure_request as u8) << 3);
        let mut buffer = vec![first_byte];
        buffer.push(self.prior_waypoints_count);

        buffer.push(self.request_label.len() as u8);
        buffer.extend(self.request_label);

        buffer.push(self.source_entity_id.encoded_len() as u8);
        buffer.extend(self.source_entity_id.to_be_bytes());

        buffer.push(self.destination_entity_id.encoded_len() as u8);
        buffer.extend(self.destination_entity_id.to_be_bytes());

        let source_name = self.source_filename.as_str().as_bytes();
        buffer.push(source_name.len() as u8);
        buffer.extend(source_name);

        let destination_name = self.destination_filename.as_str().as_bytes();
        buffer.push(destination_name.len() as u8);
        buffer.extend(destination_name);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
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
        let source_entity_id = EntityID::try_from(read_length_value_pair(buffer)?)?;
        let destination_entity_id = EntityID::try_from(read_length_value_pair(buffer)?)?;
        let source_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);
        let destination_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

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
pub struct SFOReport {
    request_label: Vec<u8>,
    source_entity_id: EntityID,
    destination_entity_id: EntityID,
    reporting_entity_id: EntityID,
    prior_waypoints: u8,
    report_code: u8,
    condition: Condition,
    direction: Direction,
    delivery_code: DeliveryCode,
    file_status: FileStatusCode,
}
impl PDUEncode for SFOReport {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // label len + message
        1 + self.request_label.len() as u16
        // source entity id len + value
        + 1 + self.source_entity_id.encoded_len()
        // destination entity id len + value
        + 1 + self.destination_entity_id.encoded_len()
        // reporting entity id len + value
        + 1 + self.reporting_entity_id.encoded_len()
        // prior waypoints
        + 1
        // report code
        + 1
        // condition, direction, code, status
        + 1
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.request_label.len() as u8];
        buffer.extend(self.request_label);
        buffer.push(self.source_entity_id.encoded_len() as u8);
        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.push(self.destination_entity_id.encoded_len() as u8);
        buffer.extend(self.destination_entity_id.to_be_bytes());
        buffer.push(self.reporting_entity_id.encoded_len() as u8);
        buffer.extend(self.reporting_entity_id.to_be_bytes());
        buffer.push(self.prior_waypoints);
        buffer.push(self.report_code);
        let last_byte: u8 = ((self.condition as u8) << 4)
            | ((self.direction as u8) << 3)
            | ((self.delivery_code as u8) << 2)
            | (self.file_status as u8);
        buffer.push(last_byte);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        let request_label = read_length_value_pair(buffer)?;
        let source_entity_id = EntityID::try_from(read_length_value_pair(buffer)?)?;
        let destination_entity_id = EntityID::try_from(read_length_value_pair(buffer)?)?;
        let reporting_entity_id = EntityID::try_from(read_length_value_pair(buffer)?)?;

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

#[cfg(test)]
mod test {
    #![allow(clippy::too_many_arguments)]
    use super::*;

    use crate::{
        assert_err,
        pdu::{
            fault_handler::HandlerCode,
            filestore::{DenyStatus, FileStoreAction, FileStoreStatus, RenameStatus},
        },
    };

    use rstest::rstest;
    use rstest_reuse::{self, template, *};

    #[rstest]
    #[case(
        (0..5).collect(),
        EntityID::from(3_u8),
        EntityID::from(3475392_u32),
        EntityID::from(1948582103_u64),
        5,
        255,
        Direction::ToReceiver,
        DeliveryCode::Complete,
    )]
    #[case(
        (200..243).collect(),
        EntityID::from(12_u16),
        EntityID::from(555184857_u64),
        EntityID::from(128374_u32),
        153,
        2,
        Direction::ToSender,
        DeliveryCode::Incomplete
    )]
    fn sfo_report_roundtrip(
        #[case] request_label: Vec<u8>,
        #[case] source_entity_id: EntityID,
        #[case] destination_entity_id: EntityID,
        #[case] reporting_entity_id: EntityID,
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
        let buffer = expected.clone().encode();
        let recovered = SFOReport::decode(&mut buffer.as_slice()).unwrap();
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

    #[template]
    #[rstest]
    #[case::transaction_id(UserOperation::OriginatingTransactionIDMessage(
        OriginatingTransactionIDMessage{
            source_entity_id: EntityID::from(2467867u32),
            transaction_sequence_number: TransactionSeqNum::from(11123132u32),
        })
    )]
    #[case::proxy_put_request(
        UserOperation::ProxyOperation(ProxyOperation::ProxyPutRequest(ProxyPutRequest{
            destination_entity_id: EntityID::from(2398u32),
            source_filename: "test_please.txt".into(),
            destination_filename: "new_test_please.dat".into()
        }))
    )]
    #[case::proxy_put_response(
        UserOperation::Response(UserResponse::ProxyPut(ProxyPutResponse{
            condition: Condition::CancelReceived,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported
        }))
    )]
    #[case::proxy_message(
        UserOperation::ProxyOperation(ProxyOperation::ProxyMessageToUser(MessageToUser{
            message_text: "Test Hello World".as_bytes().to_vec()
        }))
    )]
    #[case::proxy_filestore_request(UserOperation::ProxyOperation(ProxyOperation::ProxyFileStoreRequest(
        FileStoreRequest{
            action_code: FileStoreAction::AppendFile,
            first_filename: "/the/first/file/to/append.dat".into(),
            second_filename: "/an/additional/file/to/append.txt".into(),
        }
    )))]
    #[case::proxy_filestore_response(UserOperation::Response(UserResponse::ProxyFileStore(
        FileStoreResponse{
            action_and_status: FileStoreStatus::DenyDirectory(DenyStatus::NotAllowed),
            first_filename: "/this/is/a/test/directory/".into(),
            second_filename: "".into(),
            filestore_message: vec![]
        }
    )))]
    #[case::proxy_fault_override(UserOperation::ProxyOperation(ProxyOperation::ProxyFaultHandlerOverride(
        FaultHandlerOverride{
            fault_handler_code: HandlerCode::IgnoreError
        }
    )))]
    #[case::transmission_mode(UserOperation::ProxyOperation(
        ProxyOperation::ProxyTransmissionMode(TransmissionMode::Unacknowledged)
    ))]
    #[case::flow_label(UserOperation::ProxyOperation(ProxyOperation::ProxyFlowLabel(
        FlowLabel{
            value: "THis is a test".as_bytes().to_vec()
        }
    )))]
    #[case::segmentation_control(UserOperation::ProxyOperation(ProxyOperation::ProxySegmentationControl(
        ProxySegmentationControl{
            control: SegmentationControl::Preserved
        }
    )))]
    #[case::proxy_put_cancel(UserOperation::ProxyOperation(ProxyOperation::ProxyPutCancel))]
    #[case::directory_listing_request(UserOperation::Request(UserRequest::DirectoryListing(
        DirectoryListingRequest{
            directory_name: "/home/user/help".into(),
            directory_filename: "/home/me/this_is_Result.txt".into(),
        }
    )))]
    #[case::directory_listing_response(
        UserOperation::Response(UserResponse::DirectoryListing(DirectoryListingResponse{
            response_code: ListingResponseCode::Unsuccessful,
            directory_name: "/home/user/help22".into(),
            directory_filename: "/home/me/this_is_Result11.txt".into(),
        }))
    )]
    #[case::remote_staus_report_request(
        UserOperation::Request(UserRequest::RemoteStatusReport(RemoteStatusReportRequest{
            source_entity_id: EntityID::from(786567183_u32),
            transaction_sequence_number: TransactionSeqNum::from(u32::MAX - 3u32),
            report_filename: "foobar".into(),
        }))

    )]
    #[case::remote_status_report_response(
        UserOperation::Response(UserResponse::RemoteStatusReport(
            RemoteStatusReportResponse{
                transaction_status: TransactionStatus::Unrecognized,
                response_code: true,
                source_entity_id: EntityID::from(30875758_u32),
                transaction_sequence_number: TransactionSeqNum::from(27374848_u32),
            }
    )))]
    #[case::remote_suspend_request(
        UserOperation::Request(UserRequest::RemoteSuspend(
            RemoteSuspendRequest{
                source_entity_id: EntityID::from(8845748_u32),
                transaction_sequence_number: TransactionSeqNum::from(u32::MAX - u32::MAX /2),
            }
    )))]
    #[case::remote_suspend_response(
        UserOperation::Response(UserResponse::RemoteSuspend(
            RemoteSuspendResponse{
                suspend_indication: true,
                transaction_status: TransactionStatus::Terminated,
                source_entity_id: EntityID::from(u32::MAX),
                transaction_sequence_number: TransactionSeqNum::from(7823454u32),
            }
    )))]
    #[case::remote_resume_request(
        UserOperation::Request(UserRequest::RemoteResume(
            RemoteResumeRequest{
                source_entity_id: EntityID::from(20058583_u32),
                transaction_sequence_number: TransactionSeqNum::from(850895721_u32),
            }
    )))]
    #[case::remote_resume_response(
        UserOperation::Response(UserResponse::RemoteResume(
            RemoteResumeResponse{
                suspend_indication: true,
                transaction_status: TransactionStatus::Active,
                source_entity_id: EntityID::from(2045853_u32),
                transaction_sequence_number: TransactionSeqNum::from(85790329_u32),
            }
    )))]
    #[case::sfo_request(
        UserOperation::SFORequest(
            SFORequest{
                trace_control: TraceControl::BothDirections,
                transmission_mode: TransmissionMode::Unacknowledged,
                segment_control: SegmentationControl::Preserved,
                closure_request: true,
                prior_waypoints_count: u8::MAX,
                request_label: vec![135u8, 85, 88, 127, 129,],
                source_entity_id: EntityID::from(873123_u32),
                destination_entity_id: EntityID::from(9887373_u32),
                source_filename: "/test/test/test.test".into(),
                destination_filename: "notest/notest/notest".into(),
            }
    ))]
    #[case::sfo_message(UserOperation::SFOMessageToUser(
        MessageToUser{
            message_text: "This is a test message!".as_bytes().to_vec(),
    }))]
    #[case::sfo_flow_label(
        UserOperation::SFOFlowLabel(
            FlowLabel{
                value: vec![1u8, 3u8, 5u8, 7u8, 11u8, 13u8, 17u8]
            }
    ))]
    #[case::sfo_fault_handler_override(
        UserOperation::SFOFaultHandlerOverride(
            FaultHandlerOverride{
                fault_handler_code: HandlerCode::NoticeOfSuspension
            }
    ))]
    #[case::sfo_filestore_request(UserOperation::SFOFileStoreRequest(
        FileStoreRequest{
            action_code: FileStoreAction::RemoveDirectory,
            first_filename: "/home/user/ops".into(),
            second_filename: "".into(),
        }
    ))]
    #[case::sfo_filestore_response(UserOperation::SFOFileStoreResponse(
        FileStoreResponse{
           action_and_status: FileStoreStatus::RenameFile(RenameStatus::NotPerformed),
           first_filename: "/this/is/a/test.dat".into(),
           second_filename: "/another/test/file.txt".into(),
           filestore_message: vec![]
        }
    ))]
    #[case::sfo_report(UserOperation::SFOReport(
        SFOReport{
            request_label: vec![1u8, 2u8, 4u8, 8u8, 16u8, 32u8, 64u8],
            source_entity_id: EntityID::from(2847837_u32),
            destination_entity_id: EntityID::from(7573837_u32),
            reporting_entity_id: EntityID::from(995857_u32),
            prior_waypoints: 83u8,
            report_code: 213u8,
            condition: Condition::CheckLimitReached,
            direction: Direction::ToSender,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::FileStoreRejection
        }
    ))]
    fn user_ops_setup(#[case] expected: UserOperation) {}

    #[apply(user_ops_setup)]
    fn user_ops_roundtrip(expected: UserOperation) {
        let buffer = expected.clone().encode();
        let recovered = UserOperation::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }

    #[apply(user_ops_setup)]
    fn user_ops_len(expected: UserOperation) {
        assert_eq!(expected.encoded_len(), expected.encode().len() as u16)
    }
}
