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
#[derive(Debug, Clone, PartialEq, Eq, FromPrimitive)]
pub enum TransactionState {
    Active,
    Suspended,
    Terminated,
}

#[cfg_attr(test, derive(Debug, Clone, PartialEq))]
pub struct Metadata {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// The size of the file being transfered in this transaction.
    pub file_size: u64,
    /// List of any filestore requests to take after transaction is complete
    pub filestore_requests: Vec<FileStoreRequest>,
    /// Any Messages to user received either from the metadataPDU or as input
    pub message_to_user: Vec<MessageToUser>,
    /// Flag to track whether Transaciton Closure will be requested.
    pub closure_requested: bool,
    /// Flag to track what kind of [Checksum](crate::filestore::ChecksumType) will be used in this transaction.
    pub checksum_type: ChecksumType,
}

#[cfg_attr(test, derive(Clone))]
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
    /// Maximum count of timeouts on a [Timer] before a fault is generated.
    pub max_count: u32,
    /// Maximum amount timeof without activity before the inactivity [Timer] increments its count.
    pub inactivity_timeout: i64,
    /// Maximum amount timeof without activity before the NAK [Timer] increments its count.
    pub nak_timeout: i64,
    /// Maximum amount timeof without activity before the ACK [Timer] increments its count.
    pub ack_timeout: i64,
    // used when a proxy put request is received to originate a transaction
    pub send_proxy_response: bool,
}

#[cfg(test)]
pub(crate) mod test {
    use crate::pdu::VariableID;

    use super::*;

    use rstest::fixture;

    #[fixture]
    #[once]
    pub(crate) fn default_config() -> TransactionConfig {
        TransactionConfig {
            source_entity_id: VariableID::from(12_u16),
            destination_entity_id: VariableID::from(15_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            sequence_number: VariableID::from(3_u16),
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: HashMap::new(),
            file_size_segment: 16630_u16,
            crc_flag: CRCFlag::NotPresent,
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: 5_u32,
            inactivity_timeout: 300_i64,
            ack_timeout: 300_i64,
            nak_timeout: 300_i64,
            send_proxy_response: false,
        }
    }
}
