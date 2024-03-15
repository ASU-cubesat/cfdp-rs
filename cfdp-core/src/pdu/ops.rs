use byteorder::{BigEndian, ReadBytesExt};
use camino::Utf8PathBuf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::{
    fmt::{self, Display},
    io::Read,
};

use super::{
    error::{PDUError, PDUResult},
    fault_handler::FaultHandlerOverride,
    filestore::{FileStoreRequest, FileStoreResponse},
    header::{
        read_length_value_pair, Condition, DeliveryCode, FSSEncode, FileSizeFlag, FileStatusCode,
        NakOrKeepAlive, PDUEncode, SegmentEncode, SegmentedData, TransactionStatus,
    },
    UserOperation,
};
use crate::filestore::ChecksumType;

macro_rules! impl_id {
    ( $prim:ty, $id:expr ) => {
        impl From<$prim> for EntityID {
            fn from(val: $prim) -> Self {
                $id(val)
            }
        }
    };
}
pub type EntityID = VariableID;
pub type TransactionSeqNum = VariableID;
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
/// All possible variable length IDs as defined in CCSDS standard.
pub enum VariableID {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
}
impl_id!(u8, VariableID::U8);
impl_id!(u16, VariableID::U16);
impl_id!(u32, VariableID::U32);
impl_id!(u64, VariableID::U64);
impl TryFrom<Vec<u8>> for VariableID {
    type Error = PDUError;

    /// attempt to construct an ID from a Vec of big endian bytes.
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let length = value.len();
        match length {
            1 => Ok(Self::from(u8::from_be_bytes(
                value
                    .try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))),
            2 => Ok(Self::from(u16::from_be_bytes(
                value
                    .try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))),
            4 => Ok(Self::from(u32::from_be_bytes(
                value
                    .try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))),
            8 => Ok(Self::from(u64::from_be_bytes(
                value
                    .try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))),
            other => Err(PDUError::UnknownIDLength(other as u8)),
        }
    }
}
impl VariableID {
    /// Incerement the internal counter of the ID. This is useful for sequence numbers.
    pub fn increment(&mut self) {
        match self {
            Self::U8(val) => *val = val.overflowing_add(1).0,
            Self::U16(val) => *val = val.overflowing_add(1).0,
            Self::U32(val) => *val = val.overflowing_add(1).0,
            Self::U64(val) => *val = val.overflowing_add(1).0,
        };
    }

    /// Return the current counter value and then increment.
    pub fn get_and_increment(&mut self) -> Self {
        let current = *self;
        self.increment();
        current
    }

    /// Convert the internal counter to Big endian bytes.
    pub fn to_be_bytes(self) -> Vec<u8> {
        match self {
            VariableID::U8(val) => val.to_be_bytes().to_vec(),
            VariableID::U16(val) => val.to_be_bytes().to_vec(),
            VariableID::U32(val) => val.to_be_bytes().to_vec(),
            VariableID::U64(val) => val.to_be_bytes().to_vec(),
        }
    }

    /// convert underlying ID to u64
    pub fn to_u64(&self) -> u64 {
        match self {
            VariableID::U8(val) => *val as u64,
            VariableID::U16(val) => *val as u64,
            VariableID::U32(val) => *val as u64,
            VariableID::U64(val) => *val,
        }
    }
}
impl PDUEncode for VariableID {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // largest values supported are u64s so this should always
        // be castable down to a u8 even.
        match self {
            Self::U8(_) => 1,
            Self::U16(_) => 2,
            Self::U32(_) => 4,
            Self::U64(_) => 8,
        }
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.encoded_len() as u8 - 1_u8];

        buffer.extend(self.to_be_bytes());
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        let length: u8 = u8_buff[0] + 1;
        let mut id = vec![0u8; length as usize];
        buffer.read_exact(id.as_mut_slice())?;
        Self::try_from(id)
    }
}

impl fmt::Display for VariableID {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_u64())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowLabel {
    pub value: Vec<u8>,
}
impl PDUEncode for FlowLabel {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // len
        1
        // msg
        + self.value.len() as u16
    }

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
/// A holder for all possible Messages to the User.
///
/// An implementation my have a unique message definiton or include
/// human readable text.
pub struct MessageToUser {
    pub message_text: Vec<u8>,
}
impl PDUEncode for MessageToUser {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // message len
        1
        // message
        + self.message_text.len() as u16
    }

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
impl From<UserOperation> for MessageToUser {
    fn from(op: UserOperation) -> Self {
        Self {
            message_text: op.encode(),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// The possible field codes for a Metadata TLV field.
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
/// All TLV fields related to Metadata
pub enum MetadataTLV {
    FileStoreRequest(FileStoreRequest),
    FileStoreResponse(FileStoreResponse),
    MessageToUser(MessageToUser),
    FaultHandlerOverride(FaultHandlerOverride),
    FlowLabel(FlowLabel),
    EntityID(VariableID),
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

    fn encoded_len(&self) -> u16 {
        1 + match self {
            Self::FileStoreRequest(req) => req.encoded_len(),
            Self::FileStoreResponse(inner) => inner.encoded_len(),
            Self::MessageToUser(inner) => inner.encoded_len(),
            Self::FaultHandlerOverride(inner) => inner.encoded_len(),
            Self::FlowLabel(inner) => inner.encoded_len(),
            Self::EntityID(inner) => inner.encoded_len(),
        }
    }

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
            MetadataTLVFieldCode::EntityID => Ok(Self::EntityID(VariableID::decode(buffer)?)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// The possible directive types of a PDU, used to distinguish the PDUs.
pub enum PDUDirective {
    /// End of File PDU
    EoF = 0x04,
    /// Finished PDU
    Finished = 0x05,
    /// Positive Acknowledgement PDU
    Ack = 0x06,
    /// Metadata PDU
    Metadata = 0x07,
    /// Negative Acknowledgement PDU
    Nak = 0x08,
    /// Prompt PDU
    Prompt = 0x09,
    /// A Keep alive PDU
    KeepAlive = 0x0C,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// Subdirective codes for Positive acknowledgement PDUs.
pub enum ACKSubDirective {
    Other = 0b0000,
    Finished = 0b0001,
}

#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// Continuation state of a record.
pub enum RecordContinuationState {
    First = 0b01,
    Last = 0b10,
    Unsegmented = 0b11,
    Interim = 0b00,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Holds File data beginning at the given offset.
pub struct UnsegmentedFileData {
    /// Byte offset into the file where this data begins.
    pub offset: u64,
    pub file_data: Vec<u8>,
}
impl FSSEncode for UnsegmentedFileData {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        self.file_data.len() as u16 + file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let mut buffer = match file_size_flag {
            FileSizeFlag::Small => (self.offset as u32).to_be_bytes().to_vec(),
            FileSizeFlag::Large => self.offset.to_be_bytes().to_vec(),
        };
        buffer.extend(self.file_data);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

        let file_data: Vec<u8> = {
            let mut data: Vec<u8> = vec![];
            buffer.read_to_end(&mut data)?;
            data
        };
        Ok(Self { offset, file_data })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Holds segmented file data. Segmentation is implementation dependent.
pub struct SegmentedFileData {
    pub record_continuation_state: RecordContinuationState,
    pub segment_metadata: Vec<u8>,
    pub offset: u64,
    pub file_data: Vec<u8>,
}
impl FSSEncode for SegmentedFileData {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        1 + self.segment_metadata.len() as u16
            + file_size_flag.encoded_len()
            + self.file_data.len() as u16
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let first_byte =
            ((self.record_continuation_state as u8) << 6) | self.segment_metadata.len() as u8;
        let mut buffer = vec![first_byte];

        buffer.extend(self.segment_metadata);
        match file_size_flag {
            FileSizeFlag::Small => buffer.extend((self.offset as u32).to_be_bytes()),
            FileSizeFlag::Large => buffer.extend(self.offset.to_be_bytes()),
        };
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

        let offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
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
/// A holder for both possible types of File data PDUs.
pub enum FileDataPDU {
    Unsegmented(UnsegmentedFileData),
    Segmented(SegmentedFileData),
}
impl SegmentEncode for FileDataPDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        match self {
            Self::Unsegmented(message) => message.encoded_len(file_size_flag),
            Self::Segmented(message) => message.encoded_len(file_size_flag),
        }
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        match self {
            Self::Unsegmented(message) => message.encode(file_size_flag),
            Self::Segmented(message) => message.encode(file_size_flag),
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
/// All operations PDUs
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

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        1 + match self {
            Self::EoF(eof) => eof.encoded_len(file_size_flag),
            Self::Finished(finished) => finished.encoded_len(),
            Self::Ack(ack) => ack.encoded_len(),
            Self::Metadata(metadata) => metadata.encoded_len(file_size_flag),
            Self::Nak(nak) => nak.encoded_len(file_size_flag),
            Self::Prompt(prompt) => prompt.encoded_len(),
            Self::KeepAlive(keepalive) => keepalive.encoded_len(file_size_flag),
        }
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![self.get_directive() as u8];
        let message = match self {
            Self::EoF(msg) => msg.encode(file_size_flag),
            Self::Finished(msg) => msg.encode(),
            Self::Ack(msg) => msg.encode(),
            Self::Metadata(msg) => msg.encode(file_size_flag),
            Self::Nak(msg) => msg.encode(file_size_flag),
            Self::Prompt(msg) => msg.encode(),
            Self::KeepAlive(msg) => msg.encode(file_size_flag),
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
    pub file_size: u64,
    pub fault_location: Option<VariableID>,
}
impl FSSEncode for EndOfFile {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        //  condidition (4 bits + 4 bits spare)
        //  checksum (4 bytes)
        //  File_size (FSS)
        //  Fault Location (0 or TLV of fault location)
        5 + file_size_flag.encoded_len()
            + if let Some(fault) = self.fault_location {
                // Space for TLV code and the length field
                2 + fault.encoded_len()
            } else {
                0
            }
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let first_byte = (self.condition as u8) << 4;
        let mut buffer = vec![first_byte];

        buffer.extend(self.checksum.to_be_bytes());
        match file_size_flag {
            FileSizeFlag::Small => buffer.extend((self.file_size as u32).to_be_bytes()),
            FileSizeFlag::Large => buffer.extend(self.file_size.to_be_bytes()),
        };
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

        let file_size = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

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
                    MetadataTLVFieldCode::EntityID => Ok(Some(VariableID::decode(buffer)?)),
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
    pub fault_location: Option<VariableID>,
}
impl PDUEncode for Finished {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        //  Condition + delivery coe + status (1 byte)
        //  Filestore Responses: TLV of each response
        //  Fault Location (0 or TLV)
        1 + self
            .filestore_response
            .iter()
            .fold(0_u16, |acc, file_response| {
                acc
                        // TLV code
                        + 1
                        // TLV Length
                        + 1
                        + file_response.encoded_len()
            })
            + if let Some(fault) = self.fault_location {
                // Space for TLV code and the length field
                2 + fault.encoded_len()
            } else {
                0
            }
    }

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
                    fault_location = Some(VariableID::decode(remaining_buffer)?)
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
pub(crate) type AckPDU = PositiveAcknowledgePDU;
impl PDUEncode for PositiveAcknowledgePDU {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // Ack is 4 fields packed into 2 bytes
        2
    }

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
    pub checksum_type: ChecksumType,
    pub file_size: u64,
    pub source_filename: Utf8PathBuf,
    pub destination_filename: Utf8PathBuf,
    pub options: Vec<MetadataTLV>,
}
impl FSSEncode for MetadataPDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        // closure + checksum (1 byte)
        // file size (FSS)
        // source filename (1 + len )
        // destination filename (1 + len)
        // options (TLV per option)
        1 + file_size_flag.encoded_len()
            + 1
            + self.source_filename.as_str().len() as u16
            + 1
            + self.destination_filename.as_str().len() as u16
            + self
                .options
                .iter()
                .fold(0_u16, |acc, opt| acc + opt.encoded_len())
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let first_byte = ((self.closure_requested as u8) << 6) | (self.checksum_type as u8);
        let mut buffer = vec![first_byte];
        match file_size_flag {
            FileSizeFlag::Small => buffer.extend((self.file_size as u32).to_be_bytes()),
            FileSizeFlag::Large => buffer.extend(self.file_size.to_be_bytes()),
        };

        let source_name = self.source_filename.as_str().as_bytes();
        buffer.push(source_name.len() as u8);
        buffer.extend(source_name);

        let destination_name = self.destination_filename.as_str().as_bytes();
        buffer.push(destination_name.len() as u8);
        buffer.extend(destination_name);

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
        let checksum_type = {
            let possible = first_byte & 0xF;
            ChecksumType::from_u8(possible).ok_or(PDUError::InvalidChecksumType(possible))?
        };

        let file_size = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

        let source_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        let destination_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SegmentRequestForm {
    pub start_offset: u64,
    pub end_offset: u64,
}
impl From<(u32, u32)> for SegmentRequestForm {
    fn from(vals: (u32, u32)) -> Self {
        Self {
            start_offset: vals.0.into(),
            end_offset: vals.1.into(),
        }
    }
}
impl From<(u64, u64)> for SegmentRequestForm {
    fn from(vals: (u64, u64)) -> Self {
        Self {
            start_offset: vals.0,
            end_offset: vals.1,
        }
    }
}
impl FSSEncode for SegmentRequestForm {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        2 * file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        match file_size_flag {
            FileSizeFlag::Small => {
                let mut buffer = (self.start_offset as u32).to_be_bytes().to_vec();
                buffer.extend((self.end_offset as u32).to_be_bytes());
                buffer
            }
            FileSizeFlag::Large => {
                let mut buffer = self.start_offset.to_be_bytes().to_vec();
                buffer.extend(self.end_offset.to_be_bytes());
                buffer
            }
        }
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let start_offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
        let end_offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
        Ok(Self {
            start_offset,
            end_offset,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegativeAcknowledgmentPDU {
    pub start_of_scope: u64,
    pub end_of_scope: u64,
    // 2 x FileSizeSensitive x N length for N requests.
    pub segment_requests: Vec<SegmentRequestForm>,
}
pub(crate) type NakPDU = NegativeAcknowledgmentPDU;
impl FSSEncode for NegativeAcknowledgmentPDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        self.segment_requests
            .iter()
            .fold(0, |acc, seg| acc + seg.encoded_len(file_size_flag))
            + 2 * file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let mut buffer = match file_size_flag {
            FileSizeFlag::Small => {
                let mut buffer = (self.start_of_scope as u32).to_be_bytes().to_vec();
                buffer.extend((self.end_of_scope as u32).to_be_bytes());
                buffer
            }
            FileSizeFlag::Large => {
                let mut buffer = self.start_of_scope.to_be_bytes().to_vec();
                buffer.extend(self.end_of_scope.to_be_bytes());
                buffer
            }
        };
        self.segment_requests
            .into_iter()
            .for_each(|req| buffer.extend(req.encode(file_size_flag)));

        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let start_of_scope = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
        let end_of_scope = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

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

impl NegativeAcknowledgmentPDU {
    /// returns the maximum number of nak segments which can fit into one PDU given the payload length
    pub fn max_nak_num(file_size_flag: FileSizeFlag, payload_len: u32) -> u32 {
        (payload_len - 2 * file_size_flag.encoded_len() as u32)
            / (2 * file_size_flag.encoded_len() as u32)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptPDU {
    pub nak_or_keep_alive: NakOrKeepAlive,
}
impl PDUEncode for PromptPDU {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1
    }

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
    pub progress: u64,
}
impl FSSEncode for KeepAlivePDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        match file_size_flag {
            FileSizeFlag::Small => (self.progress as u32).to_be_bytes().to_vec(),
            FileSizeFlag::Large => self.progress.to_be_bytes().to_vec(),
        }
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        Ok(Self {
            progress: match file_size_flag {
                FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
                FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
            },
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::pdu::{
        AppendStatus, CreateFileStatus, DenyStatus, FileStoreAction, FileStoreResponse,
        FileStoreStatus, HandlerCode, ProxyOperation, RenameStatus, ReplaceStatus, UserOperation,
    };

    use rstest::rstest;

    #[rstest]
    #[case(VariableID::from(1_u8), VariableID::from(2_u8))]
    #[case(VariableID::from(300_u16), VariableID::from(301_u16))]
    #[case(VariableID::from(867381_u32), VariableID::from(867382_u32))]
    #[case(VariableID::from(857198297_u64), VariableID::from(857198298_u64))]
    fn increment_varible_id(#[case] id: VariableID, #[case] expected: VariableID) {
        let mut id = id;
        id.increment();
        assert_eq!(expected, id)
    }

    #[test]
    fn get_and_increment() {
        let mut id = VariableID::from(123_u8);
        let id2 = id.get_and_increment();
        assert_eq!(VariableID::from(123_u8), id2);
        assert_eq!(VariableID::from(124_u8), id);
    }

    #[rstest]
    fn variableid_encode(
        #[values(
            VariableID::from(1_u8),
            VariableID::from(300_u16),
            VariableID::from(867381_u32),
            VariableID::from(857198297_u64)
        )]
        id: VariableID,
    ) {
        let buff = id.encode();
        let recovered =
            VariableID::decode(&mut buff.as_slice()).expect("Unable to decode VariableID");

        assert_eq!(id, recovered)
    }

    #[rstest]
    #[case(MetadataTLVFieldCode::FileStoreRequest, "FileStoreRequest".to_owned() )]
    #[case(MetadataTLVFieldCode::FileStoreResponse, "FileStoreResponse".to_owned() )]
    #[case(MetadataTLVFieldCode::MessageToUser, "MessageToUser".to_owned() )]
    #[case(MetadataTLVFieldCode::FaultHandlerOverride, "FaultHandlerOverride".to_owned() )]
    #[case(MetadataTLVFieldCode::FlowLabel, "FlowLabel".to_owned() )]
    #[case(MetadataTLVFieldCode::EntityID, "EntityID".to_owned() )]
    fn metadatatlv_display(#[case] input: MetadataTLVFieldCode, #[case] expected: String) {
        let printed = format!("{input}");
        assert_eq!(expected, printed)
    }

    #[rstest]
    #[case(
        FileDataPDU::Unsegmented(UnsegmentedFileData{
            offset: 34574292984_u64,
            file_data: (0..255).step_by(3).collect::<Vec<u8>>()
        })
    )]
    #[case(
        FileDataPDU::Unsegmented(UnsegmentedFileData{
            offset: 12357_u64,
            file_data: (0..255).step_by(2).collect::<Vec<u8>>()
        })
    )]
    fn unsgemented_data(#[case] expected: FileDataPDU) {
        let file_size_flag = match &expected {
            FileDataPDU::Unsegmented(UnsegmentedFileData { offset: val, .. })
                if val <= &u32::MAX.into() =>
            {
                FileSizeFlag::Small
            }
            _ => FileSizeFlag::Large,
        };

        let bytes = expected.clone().encode(file_size_flag);

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
            offset: 717237408_u64,
            file_data: (0..255).step_by(3).collect::<Vec<u8>>()
        }
    )]
    #[case(
        SegmentedFileData{
            record_continuation_state: RecordContinuationState::First,
            segment_metadata: vec![0_u8, 123, 83, 99, 108, 47, 32],
            offset: (u32::MAX - 1_u32).into(),
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

        let file_size_flag = match &input {
            SegmentedFileData { offset: val, .. } if val <= &u32::MAX.into() => FileSizeFlag::Small,
            _ => FileSizeFlag::Large,
        };
        let expected = FileDataPDU::Segmented(input);

        let bytes = expected.clone().encode(file_size_flag);

        let recovered = FileDataPDU::decode(
            &mut bytes.as_slice(),
            SegmentedData::Present,
            file_size_flag,
        )
        .unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn end_of_file_no_error(#[values(7573910375_u64, 194885483_u64)] file_size: u64) {
        let file_size_flag = match file_size <= u32::MAX.into() {
            false => FileSizeFlag::Large,
            true => FileSizeFlag::Small,
        };

        let end_of_file = EndOfFile {
            condition: Condition::NoError,
            checksum: 7580274_u32,
            file_size,
            fault_location: None,
        };
        let expected = Operations::EoF(end_of_file);

        let buffer = expected.clone().encode(file_size_flag);

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
        #[values(7573910375_u64, 194885483_u64)] file_size: u64,
        #[values(
            VariableID::from(3u8),
            VariableID::from(18484u16),
            VariableID::from(u32::MAX - 34),
            VariableID::from(u64::MAX - 3473819),
        )]
        entity: VariableID,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match file_size <= u32::MAX.into() {
            false => FileSizeFlag::Large,
            true => FileSizeFlag::Small,
        };

        let end_of_file = EndOfFile {
            condition,
            checksum: 857583994u32,
            file_size,
            fault_location: Some(entity),
        };
        let expected = Operations::EoF(end_of_file);

        let buffer = expected.clone().encode(file_size_flag);

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
                    first_filename: "/path/to/a/file".into(),
                    second_filename: "".into(),
                    filestore_message: vec![],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::RenameFile(
                        RenameStatus::NewFilenameAlreadyExists,
                    ),
                    first_filename: "/path/to/a/file".into(),
                    second_filename: "/path/to/a/new/file".into(),
                    filestore_message: vec![1_u8, 3, 58],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::ReplaceFile(ReplaceStatus::NotAllowed),
                    first_filename: "/the/first/file".into(),
                    second_filename: "/the/newest/file".into(),
                    filestore_message: "a use message".as_bytes().to_vec(),
                },
            ],
            fault_location: None,
        });
        let buffer = expected.clone().encode(FileSizeFlag::Small);

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
            VariableID::from(3u8),
            VariableID::from(18484u16),
            VariableID::from(738274784u32),
            VariableID::from(97845632986u64)
        )]
        entity: VariableID,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Finished(Finished {
            condition,
            delivery_code,
            file_status,
            filestore_response: vec![
                FileStoreResponse {
                    action_and_status: FileStoreStatus::CreateFile(CreateFileStatus::Successful),
                    first_filename: "/some/path/to/a/file".into(),
                    second_filename: "".into(),
                    filestore_message: vec![],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::AppendFile(
                        AppendStatus::Filename2DoesNotExist,
                    ),
                    first_filename: "/path/to/a/file".into(),
                    second_filename: "/path/to/a/new/file".into(),
                    filestore_message: vec![1_u8, 3, 58],
                },
                FileStoreResponse {
                    action_and_status: FileStoreStatus::DenyDirectory(DenyStatus::NotPerformed),
                    first_filename: "/the/first/dir/".into(),
                    second_filename: "".into(),
                    filestore_message: "Error occurred it seems.".as_bytes().to_vec(),
                },
            ],
            fault_location: Some(entity),
        });
        let buffer = expected.clone().encode(FileSizeFlag::Small);

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
        let buffer = expected.clone().encode(FileSizeFlag::Small);
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
                first_filename: "/the/first/file/to/append.dat".into(),
                second_filename: "/an/additional/file/to/append.txt".into(),
            }
        ),
        MetadataTLV::FileStoreResponse(
            FileStoreResponse{
                action_and_status: FileStoreStatus::AppendFile(AppendStatus::NotAllowed),
                first_filename: "/this/is/a/test/directory/".into(),
                second_filename: "the/sescond/bad/file.txt".into(),
                filestore_message: vec![0_u8, 1, 3, 5]
            }
        ),
        MetadataTLV::MessageToUser(
            MessageToUser::from(UserOperation::ProxyOperation(ProxyOperation::ProxyPutCancel))
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
    #[case(vec![MetadataTLV::EntityID(VariableID::from(18574_u16))])]
    fn metadata_pdu(
        #[values(true, false)] closure_requested: bool,
        #[values(ChecksumType::Null, ChecksumType::Modular)] checksum_type: ChecksumType,
        #[values(184574_u64, 7574839485_u64)] file_size: u64,
        #[case] options: Vec<MetadataTLV>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match file_size <= u32::MAX.into() {
            true => FileSizeFlag::Small,
            false => FileSizeFlag::Large,
        };

        let expected = Operations::Metadata(MetadataPDU {
            closure_requested,
            checksum_type,
            file_size,
            source_filename: "/the/source/filename.txt".into(),
            destination_filename: "/the/destination/filename.dat".into(),
            options,
        });
        let buffer = expected.clone().encode(file_size_flag);
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    #[case(
        124_u64,
        412_u64,
        vec![
            SegmentRequestForm{
                start_offset: 124_u64,
                end_offset: 204_u64
            },
            SegmentRequestForm{
                start_offset: 312_u64,
                end_offset: 412_u64
            },
        ]
    )]
    #[case(
        32_u64,
        582872_u64,
        vec![
            SegmentRequestForm{
                start_offset: 32_u64,
                end_offset: 816_u64
            },
            SegmentRequestForm{
                start_offset: 1024_u64,
                end_offset: 1536_u64
            },
            SegmentRequestForm{
                start_offset: 582360_u64,
                end_offset: 582872_u64
            },
        ]
    )]
    fn nak_pdu(
        #[case] start_of_scope: u64,
        #[case] end_of_scope: u64,
        #[case] segment_requests: Vec<SegmentRequestForm>,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Nak(NakPDU {
            start_of_scope,
            end_of_scope,
            segment_requests,
        });
        let buffer = expected.clone().encode(file_size_flag);
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn prompt_pdu(
        #[values(NakOrKeepAlive::KeepAlive, NakOrKeepAlive::Nak)] nak_or_keep_alive: NakOrKeepAlive,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Prompt(PromptPDU { nak_or_keep_alive });
        let buffer = expected.clone().encode(file_size_flag);
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn keepalive_pdu(
        #[values(0_u64, 32_u64, 65532_u64, 12_u64, 65536_u64, 8573732_u64)] progress: u64,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::KeepAlive(KeepAlivePDU { progress });
        let buffer = expected.clone().encode(file_size_flag);
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }
}
