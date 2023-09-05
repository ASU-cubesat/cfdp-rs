use cfdp_core::transaction::TransactionError;

pub(crate) mod recv;
pub(crate) mod send;

pub use recv::RecvTransaction;
pub use send::SendTransaction;

pub type TransactionResult<T> = Result<T, TransactionError>;

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;

    use cfdp_core::{
        pdu::{CRCFlag, FileSizeFlag, SegmentedData, TransmissionMode, VariableID},
        transaction::TransactionConfig,
    };

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
        }
    }
}
