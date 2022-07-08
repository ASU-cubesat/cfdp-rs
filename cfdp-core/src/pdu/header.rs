use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use std::io::Read;

use super::error::{PDUError, PDUResult};

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
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
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum FileSizeSensitive {
    Small = 32,
    Large = 64,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum Condition {
    NoError = 0b0000,
    PositiveLimitReached = 0b0001,
    KeepAliveLimitReached = 0b0010,
    InvalidTransmissionMode = 0b0011,
    FilestoreRejection = 0b0100,
    FileChecksumFailure = 0b0101,
    FilesizeError = 0b0110,
    NakLimitReached = 0b0111,
    InactivityDetected = 0b1000,
    InvalidFileStructure = 0b1001,
    CheckLimitReached = 0b1010,
    UnsupportedChecksumType = 0b1011,
    SuspendReceived = 0b1110,
    CancelReceived = 0b1111,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum U3 {
    Zero = 0b000,
    One = 0b001,
    Two = 0b010,
    Three = 0b011,
    Four = 0b100,
    Five = 0b101,
    Six = 0b110,
    Seven = 0b111,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum PDUType {
    FileDirective = 0,
    FileData = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum Direction {
    ToReceiver = 0,
    ToSender = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum TransmissionMode {
    Acknowledged = 0,
    Unacknowledged = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum TraceControl {
    NoTrace = 0x0,
    SourceOnly = 0x1,
    DestinationOnly = 0x2,
    BothDirections = 0x3,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum CRCFlag {
    NotPresent = 0,
    Present = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum FileSizeFlag {
    Small = 0,
    Large = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum SegmentationControl {
    NotPreserved = 0,
    Preserved = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
enum SegmentedData {
    NotPresent = 0,
    Present = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum FieldCode {
    FilestoreRequest = 0x00,
    FilestoreResponse = 0x01,
    MessageToUser = 0x02,
    FaultHandlerOverrides = 0x04,
    FlowLabel = 0x05,
    EntityID = 0x06,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum NakOrKeepAlive {
    Nak = 0,
    KeepAlive = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum DeliveryCode {
    Complete = 0,
    Incomplete = 1,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum FileStatusCode {
    Discarded = 0b00,
    FilestoreRejection = 0b01,
    Retained = 0b10,
    Unreported = 0b11,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum TransactionStatus {
    Undefined = 0b00,
    Active = 0b01,
    Terminated = 0b10,
    Unrecognized = 0b11,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, ToPrimitive, FromPrimitive)]
pub enum MessageType {
    ProxyPutRequest = 0x00,
    ProxyMessageToUser = 0x01,
    ProxyFilestoreRequest = 0x02,
    ProxyFaultHandlerOverride = 0x03,
    ProxyTransmissionMode = 0x04,
    ProxyFlowLabel = 0x05,
    ProxySegmentationControl = 0x06,
    ProxyPutResponse = 0x07,
    ProxyFilestoreResponse = 0x08,
    ProxyPutCancel = 0x09,
    OriginatingTransactionIDMessage = 0x0A,
    ProxyClosureRequest = 0x0B,
    DirectoryListingRequest = 0x10,
    DirectoryListingResponse = 0x11,
    RemoteStatusReportRequest = 0x20,
    RemoteStatusReportResponse = 0x21,
    RemoteSuspendRequest = 0x30,
    RemoteSuspendResponse = 0x31,
    RemoteResumeRequest = 0x38,
    RemoteResumeResponse = 0x39,
    SFORequest = 0x40,
    SFOMessageToUser = 0x41,
    SFOFlowLabel = 0x42,
    SFOFaultHandlerOverride = 0x43,
    SFOFilestoreRequest = 0x44,
    SFOReport = 0x45,
    SFOFilestoreResponse = 0x46,
}

pub trait PDUEncode {
    type PDUType;
    fn encode(self) -> Vec<u8>;
    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PDUHeader {
    version: U3,
    pdu_type: PDUType,
    direction: Direction,
    transmission_mode: TransmissionMode,
    crc_flag: CRCFlag,
    large_file_flag: FileSizeFlag,
    pdu_data_field_length: u16,
    segmentation_control: SegmentationControl,
    entity_ids_length: U3,
    segment_metadata_flag: SegmentedData,
    transaction_sequence_number_length: U3,
    source_entity_id: Vec<u8>,
    transaction_sequence_number: Vec<u8>,
    destination_entity_id: Vec<u8>,
}
// impl PDUEncode for PDUHeader {
//     type PDUType = Self;
//     fn encode(self) -> Vec<u8> {
//         let mut buffer = Vec::<u8>::new();
//         buffer
//     }

//     fn decode<T: Read>(buffer: &mut T) -> PDUResult<PDUType> {
//         Ok(Self {})
//     }
// }

pub fn read_length_value_pair<T: Read>(buffer: &mut T) -> PDUResult<Vec<u8>> {
    let mut u8_buff = [0u8; 1];
    buffer.read_exact(&mut u8_buff)?;
    let length = u8_buff[0];
    let mut vector = vec![0u8; length as usize];
    buffer.read_exact(vector.as_mut_slice())?;
    Ok(vector)
}

pub fn read_type<T: Read>(buffer: &mut T) -> PDUResult<MessageType> {
    let mut u8_buff = [0u8];
    buffer.read_exact(&mut u8_buff)?;
    MessageType::from_u8(u8_buff[0]).ok_or(PDUError::MessageType(u8_buff[0]))
}

pub fn read_type_length_value<T: Read>(buffer: &mut T) -> PDUResult<(MessageType, Vec<u8>)> {
    let message_type = read_type(buffer)?;
    let vector = read_length_value_pair(buffer)?;

    Ok((message_type, vector))
}

#[cfg(test)]
mod test {
    use super::*;

    use num_traits::ToPrimitive;
    use rstest::rstest;

    #[rstest]
    fn read_lv(
        #[values(
            "Hello World",
            "Goodbye world!>",
            "A much longer message really but we need to be sure.",
            ""
        )]
        input_message: &str,
    ) {
        let mut buffer: Vec<u8> = vec![input_message.as_bytes().len() as u8];
        buffer.extend_from_slice(input_message.as_bytes());
        let mut input_buffer = &buffer[..];
        assert_ne!(0, input_buffer.len());
        let recovered = read_length_value_pair(&mut input_buffer).unwrap();
        assert_eq!(input_message.as_bytes(), recovered)
    }

    #[rstest]
    fn read_tlv(
        #[values(
            MessageType::ProxyPutCancel,
            MessageType::ProxyClosureRequest,
            MessageType::SFOReport
        )]
        message_type: MessageType,
        #[values(
            "Hello World",
            "Goodbye world!>",
            "A much longer message really but we need to be sure."
        )]
        input_message: &str,
    ) {
        let mut buffer: Vec<u8> = vec![message_type.to_u8().unwrap()];
        buffer.push(input_message.as_bytes().len() as u8);
        buffer.extend_from_slice(input_message.as_bytes());
        let mut input_buffer = &buffer[..];
        let recovered = read_type_length_value(&mut input_buffer).unwrap();
        assert_eq!(message_type, recovered.0);
        assert_eq!(input_message.as_bytes(), recovered.1)
    }
}
