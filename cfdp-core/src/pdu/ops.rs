use super::{
    filestore::FilestoreResponse,
    header::{
        Condition, DeliveryCode, FileSizeSensitive, FileStatusCode, NakOrKeepAlive, PDUDirective,
        TransactionStatus,
    },
};

pub enum Operations {
    Metadata(MetadataPDU),
    FileData(FileDataPDU),
    Nak(NakPDU),
    Prompt(PromptPDU),
    KeepAlive(KeepAlivePDU),
    EoF(EndOfFile),
    Finished(Finished),
    Ack(AckPDU),
}
impl Operations {
    pub fn get_directive(&self) -> Option<PDUDirective> {
        match self {
            Self::Metadata(_) => Some(PDUDirective::Metadata),
            Self::FileData(_) => None,
            Self::Nak(_) => Some(PDUDirective::Nak),
            Self::Prompt(_) => Some(PDUDirective::Prompt),
            Self::KeepAlive(_) => Some(PDUDirective::KeepAlive),
            Self::EoF(_) => Some(PDUDirective::EoF),
            Self::Finished(_) => Some(PDUDirective::Finished),
            Self::Ack(_) => Some(PDUDirective::Ack),
        }
    }
}

pub struct MetadataPDU {
    pub closure_requested: bool,
    pub checksum_type: u8,
    pub file_size: FileSizeSensitive,
    pub source_filename: Vec<u8>,
    pub destination_filename: Vec<u8>,
    pub options: Vec<u8>,
}

pub enum RecordContinutionState {
    First = 0b01,
    Last = 0b10,
    Unsegmented = 0b11,
    Interim = 0b00,
}
pub enum FileDataPDU {
    Unsegmented(UnsegmentedFileData),
    Segmented(SegmentedFileData),
}
pub struct UnsegmentedFileData {
    pub segment_offset: FileSizeSensitive,
    pub file_data: Vec<u8>,
}
pub struct SegmentedFileData {
    pub record_continuation_state: RecordContinutionState,
    pub segment_metadata: Vec<u8>,
    pub segment_offset: FileSizeSensitive,
    pub file_data: Vec<u8>,
}

pub struct SegmentRequestForm {
    pub start_offset: FileSizeSensitive,
    pub end_offset: FileSizeSensitive,
}
pub struct NegativeAcknowldegmentPDU {
    pub directive: PDUDirective,
    pub start_of_scope: FileSizeSensitive,
    pub end_of_scope: FileSizeSensitive,
    // 64 x N length for N requests.
    pub segment_requests: Vec<SegmentRequestForm>,
}
type NakPDU = NegativeAcknowldegmentPDU;

pub struct PromptPDU {
    pub directive: PDUDirective,
    pub nak_or_keep_alive: NakOrKeepAlive,
}

pub struct KeepAlivePDU {
    pub directive: PDUDirective,
    pub value: FileSizeSensitive,
}

pub struct EndOfFile {
    pub directive: PDUDirective,
    pub condition_code: Condition,
    pub checksum: u32,
    pub filesize: FileSizeSensitive,
    pub fault_location: Vec<u8>,
}

pub struct Finished {
    pub directive: PDUDirective,
    pub condition: Condition,
    pub delivery_code: DeliveryCode,
    pub file_status: FileStatusCode,
    pub filestore_response: Vec<FilestoreResponse>,
    pub fault_location: Vec<u8>,
}

pub struct PositiveAcknowledgePDU {
    pub directive: PDUDirective,
    pub directive_subtype_code: u8,
    pub conidition: Condition,
    pub transaction_status: TransactionStatus,
}
type AckPDU = PositiveAcknowledgePDU;
