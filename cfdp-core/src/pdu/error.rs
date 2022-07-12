use std::fmt;

use super::filestore::FilestoreAction;

#[derive(Debug)]
pub enum PDUError {
    MessageType(u8),
    UnexpectedMessage(String, String),
    UnexpectedIdentifier(Vec<u8>, Vec<u8>),
    InvalidCondition(u8),
    InvalidDirection(u8),
    InvalidDirective(u8),
    InvalidDeliveryCode(u8),
    InvalidFileStatus(u8),
    InvalidTraceControl(u8),
    InvalidTransmissionMode(u8),
    InvalidSegmentControl(u8),
    InvalidTransactionStatus(u8),
    InvalidFilestoreAction(u8),
    InvalidFilestoreStatus(u8, FilestoreAction),
    InvalidFaultHandlerCode(u8),
    InvalidACKDirectiveSubType(u8),
    InvalidPrompt(u8),
    InvalidVersion(u8),
    InvalidPDUType(u8),
    InvalidCRCFlag(u8),
    InvalidFileSizeFlag(u8),
    InvalidSegmentMetadataFlag(u8),
    ReadError(std::io::Error),
}
impl fmt::Display for PDUError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            &Self::MessageType(val) => write!(f, "Invalid Message type value: {:}.", val),
            Self::UnexpectedMessage(m1, m2) => {
                write!(
                    f,
                    "Unexpected Message type. Expected {:?}, got {:?}.",
                    m1, m2
                )
            }
            Self::UnexpectedIdentifier(m1, m2) => write!(
                f,
                "Unexpected Message Identifier. Received ({:?}), Expected ({:?}).",
                m1, m2
            ),
            &Self::InvalidCondition(val) => write!(f, "Invalid Condition value: {:}.", val),
            &Self::InvalidDirective(val) => write!(f, "Invalid Directive value: {:}.", val),
            &Self::InvalidDirection(val) => write!(f, "Invalid Direction value: {:}.", val),
            &Self::InvalidDeliveryCode(val) => write!(f, "Invalid Delivery Code: {:}.", val),
            &Self::InvalidFileStatus(val) => write!(f, "Invalid File Status: {:}.", val),
            &Self::InvalidTraceControl(val) => write!(f, "Inavlide Trace Control {:}.", val),
            &Self::InvalidTransmissionMode(val) => {
                write!(f, "Inavlide Transmission Mode {:}.", val)
            }
            &Self::InvalidSegmentControl(val) => {
                write!(f, "Invalid Segment Control Mode {:}.", val)
            }
            &Self::InvalidTransactionStatus(val) => {
                write!(f, "Invalid Transaction Status {:}.", val)
            }
            &Self::InvalidFilestoreAction(val) => write!(f, "Inavlide Filestore Action {:}.", val),
            Self::InvalidFilestoreStatus(val, action) => write!(
                f,
                "Inavlid Filestore Status {:} for Action {:?}.",
                val, action
            ),
            &Self::InvalidFaultHandlerCode(val) => {
                write!(f, "Invalid Fault Handler Code: {:}.", val)
            }

            Self::InvalidACKDirectiveSubType(val) => {
                write!(f, "Invalid ACK SubDirective Code: {:}.", val)
            }
            Self::InvalidPrompt(val) => write!(f, "Invalid Prompt value {:}.", val),
            Self::InvalidVersion(val) => {
                write!(f, "Invalid CCSDS Version Code: {:}.", val)
            }
            Self::InvalidPDUType(val) => write!(f, "Invalid PDU Type {:}.", val),
            Self::InvalidCRCFlag(val) => write!(f, "Invalid CRC Flag {:}.", val),
            Self::InvalidFileSizeFlag(val) => write!(f, "Invalid File Size Flag {:}.", val),
            Self::InvalidSegmentMetadataFlag(val) => {
                write!(f, "Invalid Segment Metadata Flag {:}.", val)
            }
            Self::ReadError(source) => write!(f, "Error Reading PDU Buffer. {:}", source),
        }
    }
}
impl std::error::Error for PDUError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MessageType(_) => None,
            Self::UnexpectedMessage(_, _) => None,
            Self::UnexpectedIdentifier(_, _) => None,
            Self::InvalidCondition(_) => None,
            Self::InvalidDirection(_) => None,
            Self::InvalidDirective(_) => None,
            Self::InvalidDeliveryCode(_) => None,
            Self::InvalidFileStatus(_) => None,
            Self::InvalidTraceControl(_) => None,
            Self::InvalidTransmissionMode(_) => None,
            Self::InvalidSegmentControl(_) => None,
            Self::InvalidTransactionStatus(_) => None,
            Self::InvalidFilestoreAction(_) => None,
            Self::InvalidFilestoreStatus(_, _) => None,
            Self::InvalidFaultHandlerCode(_) => None,
            Self::InvalidACKDirectiveSubType(_) => None,
            Self::InvalidPrompt(_) => None,
            Self::InvalidVersion(_) => None,
            Self::InvalidPDUType(_) => None,
            Self::InvalidCRCFlag(_) => None,
            Self::InvalidFileSizeFlag(_) => None,
            Self::InvalidSegmentMetadataFlag(_) => None,
            Self::ReadError(source) => Some(source),
        }
    }
}

pub type PDUResult<T> = Result<T, PDUError>;

impl From<std::io::Error> for PDUError {
    fn from(err: std::io::Error) -> Self {
        Self::ReadError(err)
    }
}
