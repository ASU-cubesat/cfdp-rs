use std::{collections::HashMap, fmt};

use camino::Utf8PathBuf;
use num_derive::FromPrimitive;

use crate::{
    filestore::ChecksumType,
    pdu::{
        CRCFlag, Condition, EntityID, FaultHandlerAction, FileSizeFlag, FileStoreRequest,
        MessageToUser, SegmentedData, TransactionSeqNum, TransmissionMode, VariableID,
    },
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct TransactionID(pub EntityID, pub TransactionSeqNum);

impl fmt::Display for TransactionID {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.0.to_u64(), self.1.to_u64())
    }
}

impl TransactionID {
    pub fn from<T, U>(entity_id: T, seq_num: U) -> Self
    where
        VariableID: From<T>,
        VariableID: From<U>,
    {
        Self(EntityID::from(entity_id), TransactionSeqNum::from(seq_num))
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, FromPrimitive)]
pub enum TransactionState {
    Active,
    Suspended,
    Terminated,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Metadata {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// The size of the file being transferred in this transaction.
    pub file_size: u64,
    /// List of any filestore requests to take after transaction is complete
    pub filestore_requests: Vec<FileStoreRequest>,
    /// Any Messages to user received either from the metadataPDU or as input
    pub message_to_user: Vec<MessageToUser>,
    /// Flag to track whether Transaction Closure will be requested.
    pub closure_requested: bool,
    /// Flag to track what kind of [Checksum](crate::filestore::ChecksumType) will be used in this transaction.
    pub checksum_type: ChecksumType,
}

#[derive(Clone)]
pub struct TransactionConfig {
    /// Identification number of the source (this) entity. See [EntityID]
    pub source_entity_id: EntityID,
    /// Identification number of the destination (remote) entity. See [EntityID]
    pub destination_entity_id: EntityID,
    /// CFDP [TransmissionMode]
    pub transmission_mode: TransmissionMode,
    /// The sequence number in Big Endian Bytes.
    pub sequence_number: TransactionSeqNum,
    /// Flag to indicate whether or not the file size fits inside a [u32]
    pub file_size_flag: FileSizeFlag,
    /// A Mapping of actions to take when each condition is reached.
    /// See also [Condition] and [FaultHandlerAction].
    pub fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// The maximum length a file segment sent to Destination can be.
    /// u16 will be larger than any possible CCSDS packet size.
    pub file_size_segment: u16,
    /// Flag indicating whether or not CRCs will be present on the PDUs
    pub crc_flag: CRCFlag,
    /// Flag indicating whether file metadata is included with FileData
    pub segment_metadata_flag: SegmentedData,
    /// Maximum count of timeouts on a timer before a fault is generated.
    pub max_count: u32,
    /// Maximum amount of time without activity before the inactivity timer increments its count.
    pub inactivity_timeout: i64,
    /// Maximum amount of time without activity before the NAK timer increments its count.
    pub nak_timeout: i64,
    /// Maximum amount of time without activity before the ACK timer increments its count.
    pub ack_timeout: i64,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn id_from_u8() {
        let id = TransactionID::from(3_u8, 5_u8);

        assert_eq!(TransactionID(VariableID::U8(3), VariableID::U8(5)), id)
    }

    #[test]
    fn id_from_u16() {
        let id = TransactionID::from(13_u16, 541_u16);

        assert_eq!(
            TransactionID(VariableID::U16(13_u16), VariableID::U16(541_u16)),
            id,
        )
    }

    #[test]
    fn id_from_u32() {
        let id = TransactionID::from(13_u32, 541_u32);

        assert_eq!(
            TransactionID(VariableID::U32(13_u32), VariableID::U32(541_u32)),
            id,
        )
    }

    #[test]
    fn id_from_u64() {
        let id = TransactionID::from(13_u64, 541_u64);

        assert_eq!(TransactionID(VariableID::U64(13), VariableID::U64(541)), id,)
    }

    #[test]
    fn id_from_mixed() {
        let id = TransactionID::from(13_u8, 541_u64);

        assert_eq!(TransactionID(VariableID::U8(13), VariableID::U64(541)), id,)
    }
}
