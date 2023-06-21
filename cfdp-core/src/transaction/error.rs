use std::num::TryFromIntError;

use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::{
    daemon::{Indication, Report},
    filestore::FileStoreError,
    pdu::{TransactionSeqNum, TransmissionMode, VariableID, PDU},
};

use super::config::TransactionID;

pub type TransactionResult<T> = Result<T, TransactionError>;
#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("FileStore error during Transaction: {0}")]
    FileStore(#[from] Box<FileStoreError>),

    #[error("Error Communicating with transport: {0}")]
    Transport(#[from] Box<SendError<(VariableID, PDU)>>),

    #[error("Error transferring Indication {0}")]
    UserMessage(#[from] Box<SendError<Indication>>),

    #[error("No open file in transaction: {0:?}")]
    NoFile(TransactionID),

    #[error("Error during daemon thread listening: {0}")]
    Daemon(String),

    #[error("Error converting from integer: {0}")]
    IntConverstion(#[from] TryFromIntError),

    #[error("Transaction (ID: {0:?}, Mode: {1:?}) received unexpected PDU {2:}.")]
    UnexpectedPDU(TransactionSeqNum, TransmissionMode, String),

    #[error("Metadata missing for transaction: {0:?}.")]
    MissingMetadata(TransactionID),

    #[error("No NAKs present. Cannot send missing data.")]
    MissingNak,

    #[error("No Checksum received. Cannot verify file integrity.")]
    NoChecksum,

    #[error("Unable to send report to Daemon process. {0:?}.")]
    Report(#[from] SendError<Report>),

    #[error("No Transaction status for code {0}.")]
    InvalidStatus(u8),
}
impl From<FileStoreError> for TransactionError {
    fn from(error: FileStoreError) -> Self {
        Self::FileStore(Box::new(error))
    }
}
impl From<SendError<(VariableID, PDU)>> for TransactionError {
    fn from(error: SendError<(VariableID, PDU)>) -> Self {
        Self::Transport(Box::new(error))
    }
}
impl From<SendError<Indication>> for TransactionError {
    fn from(error: SendError<Indication>) -> Self {
        Self::UserMessage(Box::new(error))
    }
}
