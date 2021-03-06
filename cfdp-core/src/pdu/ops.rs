use std::{fmt::Display, io::Read};

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use super::{
    error::{PDUError, PDUResult},
    fault_handler::FaultHandlerOverride,
    filestore::{FileStoreRequest, FileStoreResponse},
    header::{
        read_length_value_pair, Condition, DeliveryCode, FSSEncode, FileSizeFlag,
        FileSizeSensitive, FileStatusCode, NakOrKeepAlive, PDUEncode, SegmentEncode, SegmentedData,
        TransactionStatus,
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityID {
    id: Vec<u8>,
}
impl PDUEncode for EntityID {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.id.len() as u8 - 1_u8];

        buffer.extend(self.id);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        let length = u8_buff[0] + 1;
        let mut id = vec![0u8; length as usize];
        buffer.read_exact(id.as_mut_slice())?;
        Ok(Self { id })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowLabel {
    pub value: Vec<u8>,
}
impl PDUEncode for FlowLabel {
    type PDUType = Self;
    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.value.len() as u8];
        buffer.extend(self.value);
        buffer
    }
    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let value = read_length_value_pair(buffer)?;
        Ok(Self { value })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageToUser {
    pub message_text: Vec<u8>,
}
impl PDUEncode for MessageToUser {
    type PDUType = Self;
    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.message_text.len() as u8];
        buffer.extend(self.message_text);
        buffer
    }
    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let message_text = read_length_value_pair(buffer)?;
        Ok(Self { message_text })
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
pub enum MetadataTLVFieldCode {
    FileStoreRequest = 0x00,
    FileStoreResponse = 0x01,
    MessageToUser = 0x02,
    FaultHandlerOverride = 0x04,
    FlowLabel = 0x05,
    EntityID = 0x06,
}
impl Display for MetadataTLVFieldCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataTLV {
    FileStoreRequest(FileStoreRequest),
    FileStoreResponse(FileStoreResponse),
    MessageToUser(MessageToUser),
    FaultHandlerOverride(FaultHandlerOverride),
    FlowLabel(FlowLabel),
    EntityID(EntityID),
}
impl MetadataTLV {
    pub fn get_field_code(&self) -> MetadataTLVFieldCode {
        match self {
            Self::FileStoreRequest(_) => MetadataTLVFieldCode::FileStoreRequest,
            Self::FileStoreResponse(_) => MetadataTLVFieldCode::FileStoreResponse,
            Self::MessageToUser(_) => MetadataTLVFieldCode::MessageToUser,
            Self::FaultHandlerOverride(_) => MetadataTLVFieldCode::FaultHandlerOverride,
            Self::FlowLabel(_) => MetadataTLVFieldCode::FlowLabel,
            Self::EntityID(_) => MetadataTLVFieldCode::EntityID,
        }
    }
}
impl PDUEncode for MetadataTLV {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![self.get_field_code() as u8];
        let message_buffer = match self {
            Self::FileStoreRequest(msg) => msg.encode(),
            Self::FileStoreResponse(msg) => msg.encode(),
            Self::MessageToUser(msg) => msg.encode(),
            Self::FaultHandlerOverride(msg) => msg.encode(),
            Self::FlowLabel(msg) => msg.encode(),
            Self::EntityID(msg) => msg.encode(),
        };
        buffer.extend(message_buffer);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8];
        buffer.read_exact(&mut u8_buff)?;
        match MetadataTLVFieldCode::from_u8(u8_buff[0]).ok_or(PDUError::MessageType(u8_buff[0]))? {
            MetadataTLVFieldCode::FileStoreRequest => {
                Ok(Self::FileStoreRequest(FileStoreRequest::decode(buffer)?))
            }
            MetadataTLVFieldCode::FileStoreResponse => {
                Ok(Self::FileStoreResponse(FileStoreResponse::decode(buffer)?))
            }
            MetadataTLVFieldCode::MessageToUser => {
                Ok(Self::MessageToUser(MessageToUser::decode(buffer)?))
            }
            MetadataTLVFieldCode::FaultHandlerOverride => Ok(Self::FaultHandlerOverride(
                FaultHandlerOverride::decode(buffer)?,
            )),
            MetadataTLVFieldCode::FlowLabel => Ok(Self::FlowLabel(FlowLabel::decode(buffer)?)),
            MetadataTLVFieldCode::EntityID => Ok(Self::EntityID(EntityID::decode(buffer)?)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
pub enum PDUDirective {
    EoF = 0x04,
    Finished = 0x05,
    Ack = 0x06,
    Metadata = 0x07,
    Nak = 0x08,
    Prompt = 0x09,
    KeepAlive = 0x0C,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
pub enum ACKSubDirective {
    Other = 0b0000,
    Finished = 0b0001,
}

#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
pub enum RecordContinuationState {
    First = 0b01,
    Last = 0b10,
    Unsegmented = 0b11,
    Interim = 0b00,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsegmentedFileData {
    pub offset: FileSizeSensitive,
    pub file_data: Vec<u8>,
}
impl FSSEncode for UnsegmentedFileData {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let mut buffer = self.offset.to_be_bytes();
        buffer.extend(self.file_data);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let offset = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;
        let file_data: Vec<u8> = {
            let mut data: Vec<u8> = vec![];
            buffer.read_to_end(&mut data)?;
            data
        };
        Ok(Self { offset, file_data })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentedFileData {
    pub record_continuation_state: RecordContinuationState,
    pub segment_metadata: Vec<u8>,
    pub offset: FileSizeSensitive,
    pub file_data: Vec<u8>,
}
impl FSSEncode for SegmentedFileData {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let first_byte =
            ((self.record_continuation_state as u8) << 6) | self.segment_metadata.len() as u8;
        let mut buffer = vec![first_byte];

        buffer.extend(self.segment_metadata);
        buffer.extend(self.offset.to_be_bytes());
        buffer.extend(self.file_data);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let record_continuation_state =
            RecordContinuationState::from_u8((u8_buff[0] & 0xC0) >> 6).unwrap();
        let segment_metadata = {
            let metadata_len = u8_buff[0] & 0x3F;
            let mut data = vec![0_u8; metadata_len as usize];
            buffer.read_exact(&mut data)?;
            data
        };

        let offset = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;
        let file_data: Vec<u8> = {
            let mut data: Vec<u8> = vec![];
            buffer.read_to_end(&mut data)?;
            data
        };
        Ok(Self {
            record_continuation_state,
            segment_metadata,
            offset,
            file_data,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileDataPDU {
    Unsegmented(UnsegmentedFileData),
    Segmented(SegmentedFileData),
}
impl SegmentEncode for FileDataPDU {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        match self {
            Self::Unsegmented(message) => message.encode(),
            Self::Segmented(message) => message.encode(),
        }
    }

    fn decode<T: Read>(
        buffer: &mut T,
        segmentation_flag: SegmentedData,
        file_size_flag: FileSizeFlag,
    ) -> PDUResult<Self::PDUType> {
        match segmentation_flag {
            SegmentedData::Present => Ok(Self::Segmented(SegmentedFileData::decode(
                buffer,
                file_size_flag,
            )?)),
            SegmentedData::NotPresent => Ok(Self::Unsegmented(UnsegmentedFileData::decode(
                buffer,
                file_size_flag,
            )?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operations {
    EoF(EndOfFile),
    Finished(Finished),
    Ack(AckPDU),
    Metadata(MetadataPDU),
    Nak(NakPDU),
    Prompt(PromptPDU),
    KeepAlive(KeepAlivePDU),
}
impl Operations {
    pub fn get_directive(&self) -> PDUDirective {
        match self {
            Self::EoF(_) => PDUDirective::EoF,
            Self::Finished(_) => PDUDirective::Finished,
            Self::Ack(_) => PDUDirective::Ack,
            Self::Metadata(_) => PDUDirective::Metadata,
            Self::Nak(_) => PDUDirective::Nak,
            Self::Prompt(_) => PDUDirective::Prompt,
            Self::KeepAlive(_) => PDUDirective::KeepAlive,
        }
    }
}
impl FSSEncode for Operations {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![self.get_directive() as u8];
        let message = match self {
            Self::EoF(msg) => msg.encode(),
            Self::Finished(msg) => msg.encode(),
            Self::Ack(msg) => msg.encode(),
            Self::Metadata(msg) => msg.encode(),
            Self::Nak(msg) => msg.encode(),
            Self::Prompt(msg) => msg.encode(),
            Self::KeepAlive(msg) => msg.encode(),
        };
        buffer.extend(message);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        match PDUDirective::from_u8(u8_buff[0]).ok_or(PDUError::InvalidDirective(u8_buff[0]))? {
            PDUDirective::EoF => Ok(Self::EoF(EndOfFile::decode(buffer, file_size_flag)?)),
            PDUDirective::Finished => Ok(Self::Finished(Finished::decode(buffer)?)),
            PDUDirective::Ack => Ok(Self::Ack(AckPDU::decode(buffer)?)),
            PDUDirective::Metadata => {
                Ok(Self::Metadata(MetadataPDU::decode(buffer, file_size_flag)?))
            }
            PDUDirective::Nak => Ok(Self::Nak(NakPDU::decode(buffer, file_size_flag)?)),
            PDUDirective::Prompt => Ok(Self::Prompt(PromptPDU::decode(buffer)?)),
            PDUDirective::KeepAlive => Ok(Self::KeepAlive(KeepAlivePDU::decode(
                buffer,
                file_size_flag,
            )?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndOfFile {
    pub condition: Condition,
    pub checksum: u32,
    pub file_size: FileSizeSensitive,
    pub fault_location: Option<EntityID>,
}
impl FSSEncode for EndOfFile {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let first_byte = (self.condition as u8) << 4;
        let mut buffer = vec![first_byte];

        buffer.extend(self.checksum.to_be_bytes());
        buffer.extend(self.file_size.to_be_bytes());
        if let Some(fault) = self.fault_location {
            buffer.push(MetadataTLVFieldCode::EntityID as u8);
            buffer.extend(fault.encode());
        };
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let condition = {
            let possible_conditon = (u8_buff[0] & 0xF0) >> 4;
            Condition::from_u8(possible_conditon)
                .ok_or(PDUError::InvalidCondition(possible_conditon))?
        };
        let checksum = {
            let mut u32_buff = [0_u8; 4];
            buffer.read_exact(&mut u32_buff)?;
            u32::from_be_bytes(u32_buff)
        };

        let file_size = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;

        let fault_location = match &condition {
            Condition::NoError => None,
            _ => {
                let type_code = {
                    let mut u8_buff = [0_u8; 1];
                    buffer.read_exact(&mut u8_buff)?;
                    u8_buff[0]
                };
                match MetadataTLVFieldCode::from_u8(type_code)
                    .ok_or(PDUError::MessageType(type_code))?
                {
                    MetadataTLVFieldCode::EntityID => Ok(Some(EntityID::decode(buffer)?)),
                    code => Err(PDUError::UnexpectedMessage(
                        MetadataTLVFieldCode::EntityID.to_string(),
                        code.to_string(),
                    )),
                }?
            }
        };

        Ok(Self {
            condition,
            checksum,
            file_size,
            fault_location,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finished {
    pub condition: Condition,
    pub delivery_code: DeliveryCode,
    pub file_status: FileStatusCode,
    pub filestore_response: Vec<FileStoreResponse>,
    pub fault_location: Option<EntityID>,
}
impl PDUEncode for Finished {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let first_byte = ((self.condition as u8) << 4)
            | ((self.delivery_code as u8) << 2)
            | self.file_status as u8;
        let mut buffer = vec![first_byte];
        self.filestore_response.into_iter().for_each(|response| {
            let msg = response.encode();
            buffer.push(MetadataTLVFieldCode::FileStoreResponse as u8);
            buffer.push(msg.len() as u8);
            buffer.extend(msg);
        });

        if let Some(fault) = self.fault_location {
            buffer.push(MetadataTLVFieldCode::EntityID as u8);
            buffer.extend(fault.encode());
        };

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let condition = {
            let possible_conditon = (u8_buff[0] & 0xF0) >> 4;
            Condition::from_u8(possible_conditon)
                .ok_or(PDUError::InvalidCondition(possible_conditon))?
        };

        let delivery_code = {
            let possible_delivery = (u8_buff[0] & 0x4) >> 2;
            DeliveryCode::from_u8(possible_delivery)
                .ok_or(PDUError::InvalidDeliveryCode(possible_delivery))?
        };

        let file_status = {
            let possible_status = u8_buff[0] & 0x3;
            FileStatusCode::from_u8(possible_status)
                .ok_or(PDUError::InvalidFileStatus(possible_status))?
        };

        let mut remaining_msg: Vec<u8> = vec![];
        let mut filestore_response = vec![];
        let mut fault_location = None;

        buffer.read_to_end(&mut remaining_msg)?;
        let remaining_buffer = &mut remaining_msg.as_slice();

        while !remaining_buffer.is_empty() {
            let type_code = {
                let mut u8_buff = [0_u8; 1];
                remaining_buffer.read_exact(&mut u8_buff)?;
                u8_buff[0]
            };
            match (
                &condition,
                MetadataTLVFieldCode::from_u8(type_code).ok_or(PDUError::MessageType(type_code))?,
            ) {
                // When no error is present, the only TLVS will be FileStoreResponses
                (Condition::NoError, MetadataTLVFieldCode::FileStoreResponse) => {
                    let value = read_length_value_pair(remaining_buffer)?;
                    filestore_response.push(FileStoreResponse::decode(&mut value.as_slice())?)
                }
                // Any other TLV code is unexpectd. Probably a bit flip error.
                (Condition::NoError, code) => {
                    return Err(PDUError::UnexpectedMessage(
                        MetadataTLVFieldCode::FileStoreResponse.to_string(),
                        code.to_string(),
                    ))
                }
                // Otherwise look for FileStore Responses first
                (_, MetadataTLVFieldCode::FileStoreResponse) => {
                    let value = read_length_value_pair(remaining_buffer)?;
                    filestore_response.push(FileStoreResponse::decode(&mut value.as_slice())?)
                }
                // Then a Fault Entity ID
                (_, MetadataTLVFieldCode::EntityID) => {
                    fault_location = Some(EntityID::decode(remaining_buffer)?)
                }
                // Any other combination of TLV codes is unexpected
                (_, code) => {
                    return Err(PDUError::UnexpectedMessage(
                        MetadataTLVFieldCode::EntityID.to_string(),
                        code.to_string(),
                    ))
                }
            };
        }

        Ok(Self {
            condition,
            delivery_code,
            file_status,
            filestore_response,
            fault_location,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositiveAcknowledgePDU {
    /// Only valid for EoF and Finished Directives
    pub directive: PDUDirective,
    /// The only valid non-zero sub-code is [ACKSubDirective::Finished]
    /// EoF should always be coupled with [ACKSubDirective::Other]
    pub directive_subtype_code: ACKSubDirective,
    pub condition: Condition,
    pub transaction_status: TransactionStatus,
}
type AckPDU = PositiveAcknowledgePDU;
impl PDUEncode for PositiveAcknowledgePDU {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let first_byte = ((self.directive as u8) << 4) | (self.directive_subtype_code as u8);
        let second_byte = ((self.condition as u8) << 4) | self.transaction_status as u8;

        vec![first_byte, second_byte]
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        let (directive, directive_subtype_code) = {
            let possible_major = (u8_buff[0] & 0xF0) >> 4;
            let possible_minor = u8_buff[0] & 0xF;
            match (
                PDUDirective::from_u8(possible_major)
                    .ok_or(PDUError::InvalidDirective(possible_major))?,
                ACKSubDirective::from_u8(possible_minor)
                    .ok_or(PDUError::InvalidACKDirectiveSubType(possible_minor))?,
            ) {
                (PDUDirective::EoF, ACKSubDirective::Other) => {
                    (PDUDirective::EoF, ACKSubDirective::Other)
                }
                (PDUDirective::Finished, ACKSubDirective::Finished) => {
                    (PDUDirective::Finished, ACKSubDirective::Finished)
                }
                (PDUDirective::Finished, other) => {
                    return Err(PDUError::InvalidACKDirectiveSubType(other as u8))
                }
                (code, _) => return Err(PDUError::InvalidDirective(code as u8)),
            }
        };

        buffer.read_exact(&mut u8_buff)?;

        let condition = {
            let possible_conditon = (u8_buff[0] & 0xF0) >> 4;
            Condition::from_u8(possible_conditon)
                .ok_or(PDUError::InvalidCondition(possible_conditon))?
        };

        let transaction_status = {
            let status = u8_buff[0] & 0x3;
            TransactionStatus::from_u8(status).ok_or(PDUError::InvalidTransactionStatus(status))?
        };

        Ok(Self {
            directive,
            directive_subtype_code,
            condition,
            transaction_status,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataPDU {
    pub closure_requested: bool,
    pub checksum_type: u8,
    pub file_size: FileSizeSensitive,
    pub source_filename: Vec<u8>,
    pub destination_filename: Vec<u8>,
    pub options: Vec<MetadataTLV>,
}
impl FSSEncode for MetadataPDU {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let first_byte = ((self.closure_requested as u8) << 6) | self.checksum_type;
        let mut buffer = vec![first_byte];
        buffer.extend(self.file_size.to_be_bytes());

        buffer.push(self.source_filename.len() as u8);
        buffer.extend(self.source_filename);

        buffer.push(self.destination_filename.len() as u8);
        buffer.extend(self.destination_filename);

        self.options
            .into_iter()
            .for_each(|op| buffer.extend(op.encode()));

        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];
        let closure_requested = ((first_byte & 0x40) >> 6) != 0;
        let checksum_type = first_byte & 0xF;

        let file_size = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;

        let source_filename = read_length_value_pair(buffer)?;
        let destination_filename = read_length_value_pair(buffer)?;

        let mut options = vec![];
        let mut options_vec = vec![];

        buffer.read_to_end(&mut options_vec)?;
        let remaining_buffer = &mut options_vec.as_slice();

        while !remaining_buffer.is_empty() {
            options.push(MetadataTLV::decode(remaining_buffer)?);
        }

        Ok(Self {
            closure_requested,
            checksum_type,
            file_size,
            source_filename,
            destination_filename,
            options,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRequestForm {
    pub start_offset: FileSizeSensitive,
    pub end_offset: FileSizeSensitive,
}
impl FSSEncode for SegmentRequestForm {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let mut buffer = self.start_offset.to_be_bytes();
        buffer.extend(self.end_offset.to_be_bytes());
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let start_offset = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;
        let end_offset = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;
        Ok(Self {
            start_offset,
            end_offset,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegativeAcknowldegmentPDU {
    pub start_of_scope: FileSizeSensitive,
    pub end_of_scope: FileSizeSensitive,
    // 2 x FileSizeSensitive x N length for N requests.
    pub segment_requests: Vec<SegmentRequestForm>,
}
type NakPDU = NegativeAcknowldegmentPDU;
impl FSSEncode for NegativeAcknowldegmentPDU {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        let mut buffer = self.start_of_scope.to_be_bytes();
        buffer.extend(self.end_of_scope.to_be_bytes());
        self.segment_requests
            .into_iter()
            .for_each(|req| buffer.extend(req.encode()));

        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let start_of_scope = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;
        let end_of_scope = FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?;

        let mut segment_requests = vec![];

        let mut remaining_vec = vec![];
        buffer.read_to_end(&mut remaining_vec)?;
        let remaining_buffer = &mut remaining_vec.as_slice();

        while !remaining_buffer.is_empty() {
            segment_requests.push(SegmentRequestForm::decode(
                remaining_buffer,
                file_size_flag,
            )?)
        }

        Ok(Self {
            start_of_scope,
            end_of_scope,
            segment_requests,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptPDU {
    pub nak_or_keep_alive: NakOrKeepAlive,
}
impl PDUEncode for PromptPDU {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        vec![(self.nak_or_keep_alive as u8) << 7]
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let nak_or_keep_alive = {
            let possible = (u8_buff[0] & 0x80) >> 7;
            NakOrKeepAlive::from_u8(possible).ok_or(PDUError::InvalidPrompt(possible))?
        };
        Ok(Self { nak_or_keep_alive })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeepAlivePDU {
    pub progress: FileSizeSensitive,
}
impl FSSEncode for KeepAlivePDU {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        self.progress.to_be_bytes()
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        Ok(Self {
            progress: FileSizeSensitive::from_be_bytes(buffer, file_size_flag)?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::pdu::{
        AppendStatus, CreateFileStatus, DenyStatus, FileStoreAction, FileStoreResponse,
        FileStoreStatus, HandlerCode, RenameStatus, ReplaceStatus, UserOperation,
    };

    use rstest::rstest;

    #[rstest]
    #[case(MetadataTLVFieldCode::FileStoreRequest, "FileStoreRequest".to_owned() )]
    #[case(MetadataTLVFieldCode::FileStoreResponse, "FileStoreResponse".to_owned() )]
    #[case(MetadataTLVFieldCode::MessageToUser, "MessageToUser".to_owned() )]
    #[case(MetadataTLVFieldCode::FaultHandlerOverride, "FaultHandlerOverride".to_owned() )]
    #[case(MetadataTLVFieldCode::FlowLabel, "FlowLabel".to_owned() )]
    #[case(MetadataTLVFieldCode::EntityID, "EntityID".to_owned() )]
    fn metadatatlv_display(#[case] input: MetadataTLVFieldCode, #[case] expected: String) {
        let printed = format!("{}", input);
        assert_eq!(expected, printed)
    }

    #[rstest]
    #[case(
        FileDataPDU::Unsegmented(UnsegmentedFileData{
            offset: FileSizeSensitive::Large(34574292984u64),
            file_data: (0..255).step_by(3).collect::<Vec<u8>>()
        })
    )]
    #[case(
        FileDataPDU::Unsegmented(UnsegmentedFileData{
            offset: FileSizeSensitive::Small(12357u32),
            file_data: (0..255).step_by(2).collect::<Vec<u8>>()
        })
    )]
    fn unsgemented_data(#[case] expected: FileDataPDU) {
        let bytes = expected.clone().encode();
        let file_size_flag = match &expected {
            FileDataPDU::Unsegmented(UnsegmentedFileData {
                offset: FileSizeSensitive::Large(_),
                ..
            }) => FileSizeFlag::Large,
            FileDataPDU::Unsegmented(UnsegmentedFileData {
                offset: FileSizeSensitive::Small(_),
                ..
            }) => FileSizeFlag::Small,
            _ => unreachable!(),
        };
        let recovered = FileDataPDU::decode(
            &mut bytes.as_slice(),
            SegmentedData::NotPresent,
            file_size_flag,
        )
        .unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    #[case(
        SegmentedFileData{
            record_continuation_state: RecordContinuationState::First,
            segment_metadata: vec![3_u8, 138, 255, 0, 133, 87],
            offset: FileSizeSensitive::Large(717237408u64),
            file_data: (0..255).step_by(3).collect::<Vec<u8>>()
        }
    )]
    #[case(
        SegmentedFileData{
            record_continuation_state: RecordContinuationState::First,
            segment_metadata: vec![0_u8, 123, 83, 99, 108, 47, 32],
            offset: FileSizeSensitive::Small(u32::MAX - 1_u32),
            file_data: (0..255).step_by(8).collect::<Vec<u8>>()
        }
    )]
    fn segmented_data(
        #[values(
            RecordContinuationState::First,
            RecordContinuationState::Last,
            RecordContinuationState::Unsegmented,
            RecordContinuationState::Interim
        )]
        record_continuation_state: RecordContinuationState,
        #[case] mut input: SegmentedFileData,
    ) {
        input.record_continuation_state = record_continuation_state;

        let expected = FileDataPDU::Segmented(input);
        let bytes = expected.clone().encode();
        let file_size_flag = match &expected {
            FileDataPDU::Segmented(SegmentedFileData {
                offset: FileSizeSensitive::Large(_),
                ..
            }) => FileSizeFlag::Large,
            FileDataPDU::Segmented(SegmentedFileData {
                offset: FileSizeSensitive::Small(_),
                ..
            }) => FileSizeFlag::Small,
            _ => unreachable!(),
        };
        let recovered = FileDataPDU::decode(
            &mut bytes.as_slice(),
            SegmentedData::Present,
            file_size_flag,
        )
        .unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn end_of_file_no_error(
        #[values(
            FileSizeSensitive::Large(7573910375u64),
            FileSizeSensitive::Small(194885483u32)
        )]
        file_size: FileSizeSensitive,
    ) {
        let file_size_flag = match &file_size {
            FileSizeSensitive::Large(_) => FileSizeFlag::Large,
            FileSizeSensitive::Small(_) => FileSizeFlag::Small,
        };

        let end_of_file = EndOfFile {
            condition: Condition::NoError,
            checksum: 7580274u32,
            file_size,
            fault_location: None,
        };
        let expected = Operations::EoF(end_of_file);

        let buffer = expected.clone().encode();

        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag).unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn end_of_file_with_error(
        #[values(
            Condition::PositiveLimitReached,
            Condition::KeepAliveLimitReached,
            Condition::InvalidTransmissionMode,
            Condition::FileStoreRejection,
            Condition::FileChecksumFailure,
            Condition::FilesizeError,
            Condition::NakLimitReached
        )]
        condition: Condition,
        #[values(
            FileSizeSensitive::Large(7573910375u64),
            FileSizeSensitive::Small(194885483u32)
        )]
        file_size: FileSizeSensitive,
        #[values(
            EntityID{id: 3u8.to_be_bytes().to_vec()},
            EntityID{id: 18484u16.to_be_bytes().to_vec()},
            EntityID{id: (u32::MAX - 34).to_be_bytes().to_vec()},
            EntityID{id: (u64::MAX - 3473819).to_be_bytes().to_vec()},
        )]
        entity: EntityID,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match &file_size {
            FileSizeSensitive::Large(_) => FileSizeFlag::Large,
            FileSizeSensitive::Small(_) => FileSizeFlag::Small,
        };

        let end_of_file = EndOfFile {
            condition,
            checksum: 857583994u32,
            file_size,
            fault_location: Some(entity),
        };
        let expected = Operations::EoF(end_of_file);

        let buffer = expected.clone().encode();

        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn finished_no_error(
        #[values(DeliveryCode::Complete, DeliveryCode::Incomplete)] delivery_code: DeliveryCode,
        #[values(
            FileStatusCode::Discarded,
            FileStatusCode::FileStoreRejection,
            FileStatusCode::Retained,
            FileStatusCode::Unreported
        )]
        file_status: FileStatusCode,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Finished(Finished {
            condition: Condition::NoError,
            delivery_code,
            file_status,
            filestore_response: vec![
                FileStoreResponse {
                    action_and_status: FileStoreStatus::CreateFile(CreateFileStatus::Successful),
                    first_filename: "/path/to/a/file".as_bytes().to_vec(),
                    second_filename: vec![],
                    filestore_message: vec![],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::RenameFile(
                        RenameStatus::NewFilenameAlreadyExists,
                    ),
                    first_filename: "/path/to/a/file".as_bytes().to_vec(),
                    second_filename: "/path/to/a/new/file".as_bytes().to_vec(),
                    filestore_message: vec![1_u8, 3, 58],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::ReplaceFile(ReplaceStatus::NotAllowed),
                    first_filename: "/the/first/file".as_bytes().to_vec(),
                    second_filename: "/the/newest/file".as_bytes().to_vec(),
                    filestore_message: "a use message".as_bytes().to_vec(),
                },
            ],
            fault_location: None,
        });
        let buffer = expected.clone().encode();

        let recovered = Operations::decode(&mut buffer.as_slice(), FileSizeFlag::Large)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn finished_with_error(
        #[values(DeliveryCode::Complete, DeliveryCode::Incomplete)] delivery_code: DeliveryCode,
        #[values(
            FileStatusCode::Discarded,
            FileStatusCode::FileStoreRejection,
            FileStatusCode::Retained,
            FileStatusCode::Unreported
        )]
        file_status: FileStatusCode,
        #[values(
            Condition::InactivityDetected,
            Condition::InvalidFileStructure,
            Condition::CheckLimitReached,
            Condition::UnsupportedChecksumType,
            Condition::SuspendReceived,
            Condition::CancelReceived
        )]
        condition: Condition,
        #[values(
            EntityID{id: 3u8.to_be_bytes().to_vec()},
            EntityID{id: 18484u16.to_be_bytes().to_vec()},
            EntityID{id: (738274784u32).to_be_bytes().to_vec()},
            EntityID{id: (97845632986u64).to_be_bytes().to_vec()},
        )]
        entity: EntityID,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Finished(Finished {
            condition,
            delivery_code,
            file_status,
            filestore_response: vec![
                FileStoreResponse {
                    action_and_status: FileStoreStatus::CreateFile(CreateFileStatus::Successful),
                    first_filename: "/some/path/to/a/file".as_bytes().to_vec(),
                    second_filename: vec![],
                    filestore_message: vec![],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::AppendFile(
                        AppendStatus::Filename2DoesNotExist,
                    ),
                    first_filename: "/path/to/a/file".as_bytes().to_vec(),
                    second_filename: "/path/to/a/new/file".as_bytes().to_vec(),
                    filestore_message: vec![1_u8, 3, 58],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::DenyDirectory(DenyStatus::NotPerformed),
                    first_filename: "/the/first/dir/".as_bytes().to_vec(),
                    second_filename: "".as_bytes().to_vec(),
                    filestore_message: "Error occurred it seems.".as_bytes().to_vec(),
                },
            ],
            fault_location: Some(entity),
        });
        let buffer = expected.clone().encode();

        let recovered = Operations::decode(&mut buffer.as_slice(), FileSizeFlag::Large)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    #[case(PDUDirective::EoF, ACKSubDirective::Other)]
    #[case(PDUDirective::Finished, ACKSubDirective::Finished)]
    fn ackpdu(
        #[case] directive: PDUDirective,
        #[case] directive_subtype_code: ACKSubDirective,
        #[values(
            Condition::NoError,
            Condition::InvalidTransmissionMode,
            Condition::FileStoreRejection,
            Condition::FileChecksumFailure,
            Condition::CancelReceived
        )]
        condition: Condition,
        #[values(
            TransactionStatus::Undefined,
            TransactionStatus::Active,
            TransactionStatus::Terminated,
            TransactionStatus::Unrecognized
        )]
        transaction_status: TransactionStatus,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Ack(AckPDU {
            directive,
            directive_subtype_code,
            condition,
            transaction_status,
        });
        let buffer = expected.clone().encode();
        let recovered = Operations::decode(&mut buffer.as_slice(), FileSizeFlag::Small)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![
        MetadataTLV::FileStoreRequest(
            FileStoreRequest{
                action_code: FileStoreAction::AppendFile,
                first_filename: "/the/first/file/to/append.dat".as_bytes().to_vec(),
                second_filename: "/an/additional/file/to/append.txt".as_bytes().to_vec(),
            }
        ),
        MetadataTLV::FileStoreResponse(
            FileStoreResponse{
                action_and_status: FileStoreStatus::AppendFile(AppendStatus::NotAllowed),
                first_filename: "/this/is/a/test/directory/".as_bytes().to_vec(),
                second_filename: "the/sescond/bad/file.txt".as_bytes().to_vec(),
                filestore_message: vec![0_u8, 1, 3, 5]
            }
        ),
        MetadataTLV::MessageToUser(
            MessageToUser{
                message_text: UserOperation::ProxyPutCancel.encode()
            }
        )
    ])]
    #[case(vec![
        MetadataTLV::FaultHandlerOverride(
            FaultHandlerOverride{fault_handler_code: HandlerCode::IgnoreError}
        ),
        MetadataTLV::FlowLabel(
            FlowLabel{value: vec![0_u8, 3, 5, 17, 91, 135]}
        ),
    ])]
    #[case(vec![MetadataTLV::EntityID(EntityID{id: 18574_u16.to_be_bytes().to_vec()})])]
    fn metadata_pdu(
        #[values(true, false)] closure_requested: bool,
        #[values(2_u8, 15_u8, 3_u8)] checksum_type: u8,
        #[values(
            FileSizeSensitive::Small(184574_u32),
            FileSizeSensitive::Large(7574839485_u64)
        )]
        file_size: FileSizeSensitive,
        #[case] options: Vec<MetadataTLV>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match &file_size {
            FileSizeSensitive::Small(_) => FileSizeFlag::Small,
            FileSizeSensitive::Large(_) => FileSizeFlag::Large,
        };

        let expected = Operations::Metadata(MetadataPDU {
            closure_requested,
            checksum_type,
            file_size,
            source_filename: "/the/source/filename.txt".as_bytes().to_vec(),
            destination_filename: "/the/destination/filename.dat".as_bytes().to_vec(),
            options,
        });
        let buffer = expected.clone().encode();
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    #[case(
        FileSizeSensitive::Small(124_u32),
        FileSizeSensitive::Small(412_u32),
        vec![
            SegmentRequestForm{
                start_offset: FileSizeSensitive::Small(124_u32),
                end_offset: FileSizeSensitive::Small(204_u32)
            },
            SegmentRequestForm{
                start_offset: FileSizeSensitive::Small(312_u32),
                end_offset: FileSizeSensitive::Small(412_u32)
            },
        ]
    )]
    #[case(
        FileSizeSensitive::Large(32_u64),
        FileSizeSensitive::Large(582872_u64),
        vec![
            SegmentRequestForm{
                start_offset: FileSizeSensitive::Large(32_u64),
                end_offset: FileSizeSensitive::Large(816_u64)
            },
            SegmentRequestForm{
                start_offset: FileSizeSensitive::Large(1024_u64),
                end_offset: FileSizeSensitive::Large(1536_u64)
            },
            SegmentRequestForm{
                start_offset: FileSizeSensitive::Large(582360_u64),
                end_offset: FileSizeSensitive::Large(582872_u64)
            },
        ]
    )]
    fn nak_pdu(
        #[case] start_of_scope: FileSizeSensitive,
        #[case] end_of_scope: FileSizeSensitive,
        #[case] segment_requests: Vec<SegmentRequestForm>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match &start_of_scope {
            FileSizeSensitive::Small(_) => FileSizeFlag::Small,
            FileSizeSensitive::Large(_) => FileSizeFlag::Large,
        };
        let expected = Operations::Nak(NakPDU {
            start_of_scope,
            end_of_scope,
            segment_requests,
        });
        let buffer = expected.clone().encode();
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn prompt_pdu(
        #[values(NakOrKeepAlive::KeepAlive, NakOrKeepAlive::Nak)] nak_or_keep_alive: NakOrKeepAlive,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Prompt(PromptPDU { nak_or_keep_alive });
        let buffer = expected.clone().encode();
        let recovered = Operations::decode(&mut buffer.as_slice(), FileSizeFlag::Small)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn keepalive_pdu(
        #[values(
            FileSizeSensitive::Small(0_u32),
            FileSizeSensitive::Small(32_u32),
            FileSizeSensitive::Small(65532_u32),
            FileSizeSensitive::Large(12_u64),
            FileSizeSensitive::Large(65536_u64),
            FileSizeSensitive::Large(8573732_u64)
        )]
        progress: FileSizeSensitive,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match &progress {
            FileSizeSensitive::Small(_) => FileSizeFlag::Small,
            FileSizeSensitive::Large(_) => FileSizeFlag::Large,
        };
        let expected = Operations::KeepAlive(KeepAlivePDU { progress });
        let buffer = expected.clone().encode();
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }
}
