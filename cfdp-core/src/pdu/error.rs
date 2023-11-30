use std::string::FromUtf8Error;
use thiserror::Error;

use super::filestore::FileStoreAction;

pub type PDUResult<T> = Result<T, PDUError>;
#[derive(Error, Debug)]
/// Errors which can occurr during a PDU encoding/decoding.
pub enum PDUError {
    #[error("Invalid Message type value: {0:}.")]
    /// Received a message with an unknown message type.
    MessageType(u8),

    #[error("Unexpected Message type. Expected {0:?}, got {1:?}.")]
    /// Received a message type which did not match the expectation.
    UnexpectedMessage(String, String),

    #[error("Unexpected Message Identifier. Received ({0:?}), Expected ({1:?}).")]
    /// Received an unknown message identifier.
    /// This is currently only used when attempting to decode user operations.
    UnexpectedIdentifier(Vec<u8>, Vec<u8>),

    #[error("Invalid Condition value: {0:}.")]
    /// Unknown transaction condidtion value.
    InvalidCondition(u8),

    #[error("Invalid ChecksumType: {0:}.")]
    /// Checksum type done not match a known value.
    InvalidChecksumType(u8),

    #[error("Invalid Direction value: {0:}.")]
    /// Unknown transaction direction value.
    InvalidDirection(u8),

    #[error("Invalid Directive value: {0:}.")]
    /// Unknown Transaction driective value.
    InvalidDirective(u8),

    #[error("Invalid Delivery Code: {0:}.")]
    /// Unknown transaction delivery code value.
    InvalidDeliveryCode(u8),

    #[error("Invalid TransactionState: {0:}.")]
    /// Unknown [TransactionState](crate::transaction::TransactionState) value.
    InvalidState(u8),

    #[error("Invalid File Status: {0:}.")]
    /// Unknown [FileStatusCode](crate::pdu::FileStatusCode) value
    InvalidFileStatus(u8),

    #[error("Inavlid Trace Control {0:}.")]
    /// Unknown [TraceControl](crate::pdu::TraceControl) value.
    InvalidTraceControl(u8),

    #[error("Invalid Transmission Mode {0:}.")]
    /// Unknown [TransmissionMode](crate::pdu::TransmissionMode) value.
    InvalidTransmissionMode(u8),

    #[error("Invalid Segment Control Mode {0:}.")]
    /// Unknown [SegmentationControl](crate::pdu::SegmentationControl) value.
    InvalidSegmentControl(u8),

    #[error("Invalid Transaction Status {0:}.")]
    /// Unknown [TransactionStatus](crate::pdu::TransactionStatus) value.
    InvalidTransactionStatus(u8),

    #[error("Invalid FileStore Action {0:}.")]
    /// Unknown [FileStoreAction](crate::filestore::FileStoreAction) value.
    InvalidFileStoreAction(u8),

    #[error("Inavlid FileStore Status {0:} for Action {1:?}.")]
    /// Unknown status returned as the result of a filestore action.
    InvalidFileStoreStatus(u8, FileStoreAction),

    #[error("Invalid Fault Handler Code: {0:}.")]
    /// Unknown [HandlerCode](crate::pdu::HandlerCode) value.
    InvalidFaultHandlerCode(u8),

    #[error("Invalid ACK SubDirective Code: {0:}.")]
    /// Unknown Acknowledged sub-directive code value.
    InvalidACKDirectiveSubType(u8),

    #[error("Invalid Prompt value {0:}.")]
    /// Unknown [PromptPDU](crate::pdu::PromptPDU) type value.
    InvalidPrompt(u8),

    #[error("Invalid CCSDS Version Code: {0:}.")]
    /// Unkonwn CCSDS packet version.
    InvalidVersion(u8),

    #[error("Invalid PDU Type {0:}.")]
    /// Received an unkown [PDUType](crate::pdu::PDUType) value.
    InvalidPDUType(u8),

    #[error("Invalid CRC Flag {0:}.")]
    /// Unknown [CRCFlag](crate::pdu::CRCFlag) value
    InvalidCRCFlag(u8),

    #[error("Invalid File Size Flag {0:}.")]
    /// Unkonwn flag value for [FileSizeFlag](crate::pdu::FileSizeFlag)
    InvalidFileSizeFlag(u8),

    #[error("Invalid Segment Metadata Flag {0:}.")]
    /// Unkonwn value for [SegmentedData](crate::pdu::SegmentedData) flag.
    InvalidSegmentMetadataFlag(u8),

    #[error("CRC Failure on PDU. Expected 0x{0:X} Receieved 0x{1:X}")]
    /// PDU crc16 failure
    CRCFailure(u16, u16),

    #[error("Error Reading PDU Buffer. {0:}")]
    /// [std::io::Error] occurred when reading PDU from input byte stream.
    ReadError(#[from] std::io::Error),

    #[error("Bad length for Variable Identifier (not a power a of 2) {0:}.")]
    /// Length of the ID variable identifier was not a power of 2.
    UnknownIDLength(u8),

    #[error("Unable to decode filename. {0}")]
    /// Error occurred converting bytes to UTF-8 compliant filename.
    InvalidFileName(#[from] FromUtf8Error),

    #[error("Invalid Directory Listing Response Code: {0}. ")]
    /// Unkonwn code value for a [ListingResponseCode](crate::pdu::ListingResponseCode)
    InvalidListingCode(u8),
}
