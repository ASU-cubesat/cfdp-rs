use std::string::FromUtf8Error;
use thiserror::Error;

use super::filestore::FileStoreAction;

pub type PDUResult<T> = Result<T, PDUError>;
#[derive(Error, Debug)]
pub enum PDUError {
    #[error("Invalid Message type value: {0:}.")]
    MessageType(u8),

    #[error("Unexpected Message type. Expected {0:?}, got {1:?}.")]
    UnexpectedMessage(String, String),

    #[error("Unexpected Message Identifier. Received ({0:?}), Expected ({1:?}).")]
    UnexpectedIdentifier(Vec<u8>, Vec<u8>),

    #[error("Invalid Condition value: {0:}.")]
    InvalidCondition(u8),

    #[error("Invalid ChecksumType: {0:}.")]
    InvalidChecksumType(u8),

    #[error("Invalid Direction value: {0:}.")]
    InvalidDirection(u8),

    #[error("Invalid Directive value: {0:}.")]
    InvalidDirective(u8),

    #[error("Invalid Delivery Code: {0:}.")]
    InvalidDeliveryCode(u8),

    #[error("Invalid File Status: {0:}.")]
    InvalidFileStatus(u8),

    #[error("Inavlide Trace Control {0:}.")]
    InvalidTraceControl(u8),

    #[error("Invalid Transmission Mode {0:}.")]
    InvalidTransmissionMode(u8),

    #[error("Invalid Segment Control Mode {0:}.")]
    InvalidSegmentControl(u8),

    #[error("Invalid Transaction Status {0:}.")]
    InvalidTransactionStatus(u8),

    #[error("Invalid FileStore Action {0:}.")]
    InvalidFileStoreAction(u8),

    #[error("Inavlid FileStore Status {0:} for Action {1:?}.")]
    InvalidFileStoreStatus(u8, FileStoreAction),

    #[error("Invalid Fault Handler Code: {0:}.")]
    InvalidFaultHandlerCode(u8),

    #[error("Invalid ACK SubDirective Code: {0:}.")]
    InvalidACKDirectiveSubType(u8),

    #[error("Invalid Prompt value {0:}.")]
    InvalidPrompt(u8),

    #[error("Invalid CCSDS Version Code: {0:}.")]
    InvalidVersion(u8),

    #[error("Invalid PDU Type {0:}.")]
    InvalidPDUType(u8),

    #[error("Invalid CRC Flag {0:}.")]
    InvalidCRCFlag(u8),

    #[error("Invalid File Size Flag {0:}.")]
    InvalidFileSizeFlag(u8),

    #[error("Invalid Segment Metadata Flag {0:}.")]
    InvalidSegmentMetadataFlag(u8),

    #[error("CRC Failure on PDU. Expected 0x{0:X} Receieved 0x{1:X}")]
    CRCFailure(u16, u16),

    #[error("Error Reading PDU Buffer. {0:}")]
    ReadError(#[from] std::io::Error),

    #[error("Bad length for Variable Identifier (not a power a of 2) {0:}.")]
    UnkownIDLength(u8),

    #[error("Unable to decode filename. {0}")]
    InvalidFileName(#[from] FromUtf8Error),

    #[error("Invalid Directory Listing Response Code: {0}. ")]
    InvalidListingCode(u8),
}
