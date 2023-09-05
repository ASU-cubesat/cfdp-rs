use std::{collections::HashMap, io::Read, time::Duration};

use camino::Utf8PathBuf;
use num_traits::FromPrimitive;
use tokio::sync::oneshot;

use crate::{
    filestore::ChecksumType,
    pdu::{
        error::{PDUError, PDUResult},
        CRCFlag, Condition, DeliveryCode, EntityID, FaultHandlerAction, FileStatusCode,
        FileStoreRequest, FileStoreResponse, MessageToUser, NakOrKeepAlive, PDUEncode,
        TransactionSeqNum, TransactionStatus, TransmissionMode,
    },
    transaction::{TransactionID, TransactionState},
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Necessary Configuration for a Put.Request operation
pub struct PutRequest {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// Destination ID of the Request
    pub destination_entity_id: EntityID,
    /// Whether to send in acknowledged or unacknowledged mode
    pub transmission_mode: TransmissionMode,
    /// List of any filestore requests to take after transaction is complete
    pub filestore_requests: Vec<FileStoreRequest>,
    /// Any Messages to user received either from the metadataPDU or as input
    pub message_to_user: Vec<MessageToUser>,
}

#[derive(Debug)]
/// Possible User Primitives sent from a end user application via the user primitive channel
pub enum UserPrimitive {
    /// Initiate a Put transaction with the specified [PutRequest] configuration.
    Put(PutRequest, oneshot::Sender<TransactionID>),
    /// Cancel the give transaction.
    Cancel(TransactionID),
    /// Suspend operations of the given transaction.
    Suspend(TransactionID),
    /// Resume operations of the given transaction.
    Resume(TransactionID),
    /// Report progress of the given transaction.
    Report(TransactionID, oneshot::Sender<Report>),
    /// Send the designated PromptPDU from the given transaction.
    /// This primitive is only valid for Send transactions
    Prompt(TransactionID, NakOrKeepAlive),
}

/// Simple Status Report
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Report {
    pub id: TransactionID,
    pub state: TransactionState,
    pub status: TransactionStatus,
    pub condition: Condition,
}
impl Report {
    pub fn encode(self) -> Vec<u8> {
        let mut buff = self.id.0.encode();
        buff.extend(self.id.1.encode());
        buff.push(self.state as u8);
        buff.push(self.status as u8);
        buff.push(self.condition as u8);
        buff
    }

    pub fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let id = {
            let entity_id = EntityID::decode(buffer)?;
            let sequence_num = TransactionSeqNum::decode(buffer)?;

            TransactionID(entity_id, sequence_num)
        };

        let mut u8_buff = [0_u8; 1];

        let state = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            TransactionState::from_u8(possible).ok_or(PDUError::InvalidState(possible))?
        };

        let status = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            TransactionStatus::from_u8(possible)
                .ok_or(PDUError::InvalidTransactionStatus(possible))?
        };

        let condition = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            Condition::from_u8(possible).ok_or(PDUError::InvalidCondition(possible))?
        };

        Ok(Self {
            id,
            state,
            status,
            condition,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MetadataRecvIndication {
    pub id: TransactionID,
    pub source_filename: Utf8PathBuf,
    pub destination_filename: Utf8PathBuf,
    pub file_size: u64,
    pub transmission_mode: TransmissionMode,
    pub user_messages: Vec<MessageToUser>,
}

#[derive(Debug, Clone)]
pub struct FileSegmentIndication {
    pub id: TransactionID,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct FinishedIndication {
    pub id: TransactionID,
    pub report: Report,
    pub file_status: FileStatusCode,
    pub delivery_code: DeliveryCode,
    pub filestore_responses: Vec<FileStoreResponse>,
}

#[derive(Debug, Clone)]
pub struct SuspendIndication {
    pub id: TransactionID,
    pub condition: Condition,
}

#[derive(Debug, Clone)]
pub struct ResumeIndication {
    pub id: TransactionID,
    pub progress: u64,
}

#[derive(Debug, Clone)]
pub struct FaultIndication {
    pub id: TransactionID,
    pub condition: Condition,
    pub progress: u64,
}

#[derive(Debug, Clone)]
/// Indications how the Daemon and Transactions relay information back to the User application.
/// Indications are issued at necessary points in each Transaction's lifetime.
pub enum Indication {
    /// A new transaction has been initiated as a result of a [PutRequest]
    Transaction(TransactionID),
    /// End of File has been Sent
    EoFSent(TransactionID),
    /// End of File PDU has been received
    EoFRecv(TransactionID),
    /// A running transaction has reached the Finished state.
    /// Receipt of this indications starts and post transaction actions.
    Finished(FinishedIndication),
    /// Metadata has been received for a Receive Transaction
    MetadataRecv(MetadataRecvIndication),
    /// A new file segment has been received
    FileSegmentRecv(FileSegmentIndication),
    /// The associated Transaction has been suspended at the given progress point.
    Suspended(SuspendIndication),
    /// The associated Transaction has been resumed at the given progress point.
    Resumed(ResumeIndication),
    /// Last known status for the given transaction
    Report(Report),
    /// A Fault has been initiated for the given transaction
    Fault(FaultIndication),
    /// An Abandon Fault has been initiated for the given transaction
    Abandon(FaultIndication),
}

/// The way the Nak procedure is implemented is the following:
///  - In Immediate mode, upon reception of each file data PDU, if the received segment is at the end of the file and
///    there is a gap between the previously received segment and the new segment, a nak is sent with the new gap but
///    only after delay has elapsed (if any delay was set).
///    If the NAK timer has timed out, the nak sent covers the gaps from the entire file, not only the last gap.
///    After the EOF is received, the procedure is the same as in deferred mode.
///  - In Deferred mode, a nak covering the gaps from the entire file is sent after the EOF has been received
///    and each time the nak timer times out.
///  - at any time a Prompt NAK can trigger the sending of the complete Nak list.
///
/// The delay parameter is useful when PDUs come out of order to avoid sending NAKs prematurely. One scenario when this may
/// happen is when utilizing multiple links of different latencies. The delay should be set to cover the difference in latency
/// between the slowest link and the fastest link.
/// If the delay is greater than 0, the NAKs will not be sent immediately but only if the gap persists after the delay
/// has passed.
///
/// NAK timer (note that this is different and probably much larger than the delay parameter mentioned above):
/// - In Immediate mode the NAK timer is started at the beginning of the transaction.
/// - In Deferred mode  the NAK timer is started after EOF is received.
/// - If the NAK timer times out and it is determined that new data has been received since the last nak sending,
///   the timer counter is reset to 0.
/// - If the NAK timer expired more than the predefined limit (without any new data being received), the NakLimitReached
///   fault will be raised.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NakProcedure {
    Immediate(Duration /* delay*/),
    Deferred(Duration /* delay */),
}

#[derive(Clone)]
/// Configuration parameters for transactions which may change based on the receiving entity.
pub struct EntityConfig {
    /// Mapping to decide how each fault type should be handled
    pub fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// Maximum file size fragment this entity can receive
    pub file_size_segment: u16,
    // The number of timeouts before a fault is issued on a transaction
    pub default_transaction_max_count: u32,
    // number of seconds for inactivity timers to wait
    pub inactivity_timeout: i64,
    // number of seconds for ACK timers to wait
    pub ack_timeout: i64,
    // number of seconds for NAK timers to wait
    pub nak_timeout: i64,
    /// Flag to determine if the CRC protocol should be used
    pub crc_flag: CRCFlag,
    /// Whether closure whould be requested on Unacknowledged transactions.
    pub closure_requested: bool,
    /// The default ChecksumType to use for file transfers
    pub checksum_type: ChecksumType,
    // for recv transactions - when to send the NAKs (immediately when detected or after EOF)
    pub nak_procedure: NakProcedure,
}
