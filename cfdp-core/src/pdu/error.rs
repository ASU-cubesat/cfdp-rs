use std::fmt;

use super::{filestore::FilestoreAction, header::MessageType};

#[derive(Debug)]
pub enum PDUError {
    MessageType(u8),
    UnexpectedMessage(MessageType, MessageType),
    UnexpectedIdentifier(Vec<u8>, Vec<u8>),
    InvalidCondition(u8),
    InvalidDirection(u8),
    InvalidDeliveryCode(u8),
    InvalidFileStatus(u8),
    InvalidTraceControl(u8),
    InvalidTransmissionMode(u8),
    InvalidSegmentControl(u8),
    InvalidTransactionStatus(u8),
    InvalidFilestoreAction(u8),
    InvalidFilestoreStatus(u8, FilestoreAction),
    InvalidFaultHandlerCode(u8),
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
            Self::InvalidDeliveryCode(_) => None,
            Self::InvalidFileStatus(_) => None,
            Self::InvalidTraceControl(_) => None,
            Self::InvalidTransmissionMode(_) => None,
            Self::InvalidSegmentControl(_) => None,
            Self::InvalidTransactionStatus(_) => None,
            Self::InvalidFilestoreAction(_) => None,
            Self::InvalidFilestoreStatus(_, _) => None,
            Self::InvalidFaultHandlerCode(_) => None,
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
