use log::error;
use std::io::Read;

pub(crate) mod error;
mod fault_handler;
mod filestore;
mod header;
mod ops;
mod user_ops;

pub use fault_handler::*;
pub use filestore::*;
pub use header::*;
pub use ops::*;
pub use user_ops::*;

use error::{PDUError, PDUResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PDUPayload {
    Directive(Operations),
    FileData(FileDataPDU),
}
impl PDUPayload {
    /// computes the total length of the payload without additional encoding/copying
    pub fn get_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        match self {
            Self::Directive(operation) => operation.get_len(file_size_flag),
            Self::FileData(file_data) => file_data.get_len(file_size_flag),
        }
    }

    /// Encodes the payload to a byte stream
    pub fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        match self {
            Self::Directive(operation) => operation.encode(file_size_flag),
            Self::FileData(data) => data.encode(file_size_flag),
        }
    }

    /// Decodes from an input bytestream
    pub fn decode<T: std::io::Read>(
        buffer: &mut T,
        pdu_type: PDUType,
        file_size_flag: FileSizeFlag,
        segmentation_flag: SegmentedData,
    ) -> error::PDUResult<Self> {
        match pdu_type {
            PDUType::FileDirective => {
                Ok(Self::Directive(Operations::decode(buffer, file_size_flag)?))
            }
            PDUType::FileData => Ok(Self::FileData(FileDataPDU::decode(
                buffer,
                segmentation_flag,
                file_size_flag,
            )?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PDU {
    pub header: PDUHeader,
    pub payload: PDUPayload,
}
impl PDUEncode for PDU {
    type PDUType = Self;

    fn get_len(&self) -> u16 {
        self.header.get_len() + self.payload.get_len(self.header.large_file_flag)
    }

    fn encode(self) -> Vec<u8> {
        let crc_flag = self.header.crc_flag;
        let file_size_flag = self.header.large_file_flag;

        let mut buffer = self.header.encode();
        buffer.extend(self.payload.encode(file_size_flag));
        match crc_flag {
            CRCFlag::Present => buffer.extend(crc16_ibm_3740(buffer.as_slice()).to_be_bytes()),
            CRCFlag::NotPresent => {}
        }
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let header = PDUHeader::decode(buffer)?;

        let mut remaining_msg = vec![0_u8; header.pdu_data_field_length as usize];

        buffer.read_exact(remaining_msg.as_mut_slice())?;
        let remaining_buffer = &mut remaining_msg.as_slice();

        let payload = PDUPayload::decode(
            remaining_buffer,
            header.pdu_type.clone(),
            header.large_file_flag,
            header.segment_metadata_flag.clone(),
        )?;

        let received_pdu = Self { header, payload };

        // TODO! Make this CRC check first for a faster failure mode
        match &received_pdu.header.crc_flag {
            CRCFlag::NotPresent => {}
            CRCFlag::Present => {
                let mut u16_buffer = [0_u8; 2];
                buffer.read_exact(&mut u16_buffer)?;
                let crc16 = u16::from_be_bytes(u16_buffer);
                let tmp_buffer = {
                    let input_pdu = received_pdu.clone();

                    let mut temp = input_pdu.encode();
                    // remove the crc from the temporary buffer
                    temp.truncate(temp.len() - 2);
                    temp
                };
                let crc = crc16_ibm_3740(tmp_buffer.as_slice());
                match crc == crc16 {
                    true => {}
                    false => {
                        error!(
                            "CRC FAILURE, {}, {}, {}",
                            crc,
                            crc16,
                            crc.overflowing_add(crc16).0
                        );
                        return Err(PDUError::CRCFailure(crc16, crc));
                    }
                }
            }
        }

        Ok(received_pdu)
    }
}

fn crc16_ibm_3740(message: &[u8]) -> u16 {
    message
        .iter()
        .fold(0xffff, |acc, digit| crc16(*digit as u16, acc))
}

fn crc16(in_char: u16, crc: u16) -> u16 {
    let poly = 0x1021;
    let shift_char = (in_char & 0x00FF) << 8;
    let mut crc = crc ^ shift_char;
    for _ in 0..8 {
        match crc & 0x8000 > 0 {
            true => crc = (crc << 1) ^ poly,
            false => crc <<= 1,
        };
    }
    crc
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::{filestore::ChecksumType, pdu::header::NakOrKeepAlive};

    use rstest::rstest;

    #[rstest]
    #[case("123456789".as_bytes(), 0x29b1_u16)]
    #[case(
        &[
            0x06, 0x00, 0x0c, 0xf0, 0x00, 0x04, 0x00, 0x55,
            0x88, 0x73, 0xc9, 0x00, 0x00, 0x05, 0x21
        ],
        0x75FB
    )]
    fn crc16(#[case] input: &[u8], #[case] expected: u16) {
        let recovered = crc16_ibm_3740(input);
        assert_eq!(expected, recovered)
    }

    #[rstest]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
        fault_location: None,
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
        fault_location: Some(VariableID::from(1_u8)),
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
        fault_location: Some(VariableID::from(15_u16)),
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
        fault_location: Some(VariableID::from(15_u32)),
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
        fault_location: Some(VariableID::from(15_u64)),
    })))]
    #[case(PDUPayload::Directive(Operations::Finished(Finished {
        condition: Condition::NoError,
        delivery_code: DeliveryCode::Complete,
        file_status: FileStatusCode::Retained,
        filestore_response: vec![],
        fault_location: None
    })))]
    #[case(PDUPayload::Directive(Operations::Finished(Finished {
        condition: Condition::NoError,
        delivery_code: DeliveryCode::Complete,
        file_status: FileStatusCode::Retained,
        filestore_response: vec![],
        fault_location: Some(VariableID::from(12_u16))
    })))]
    #[case(PDUPayload::Directive(Operations::Finished(Finished {
        condition: Condition::NoError,
        delivery_code: DeliveryCode::Complete,
        file_status: FileStatusCode::Retained,
        filestore_response: vec![
            FileStoreResponse {
                action_and_status: FileStoreStatus::CreateFile(CreateFileStatus::Successful),
                first_filename: "test".into(),
                second_filename: "".into(),
                filestore_message: "some message here".into(),
            }
        ],
        fault_location: None
    })))]
    #[case(PDUPayload::Directive(Operations::Finished(Finished {
        condition: Condition::NoError,
        delivery_code: DeliveryCode::Complete,
        file_status: FileStatusCode::Retained,
        filestore_response: vec![
            FileStoreResponse {
                action_and_status: FileStoreStatus::CreateFile(CreateFileStatus::Successful),
                first_filename: "test".into(),
                second_filename: "".into(),
                filestore_message: "some message here".into(),
            },
            FileStoreResponse {
                action_and_status: FileStoreStatus::AppendFile(AppendStatus::Successful),
                first_filename: "the_long_filename".into(),
                second_filename: "The second longer name".into(),
                filestore_message: "A very detailed message indeed".into(),
            }
        ],
        fault_location: Some(VariableID::from(55_u64))
    })))]
    #[case(PDUPayload::Directive(
        Operations::Ack(AckPDU{
            directive: PDUDirective::EoF,
            directive_subtype_code: ACKSubDirective::Other,
            condition: Condition::NoError,
            transaction_status: TransactionStatus::Unrecognized,
        })

    ))]
    #[case(PDUPayload::Directive(
        Operations::Metadata(
            MetadataPDU {
                closure_requested: true,
                checksum_type: ChecksumType::Modular,
                file_size: 55_u64,
                source_filename: "the input filename".into(),
                destination_filename: "the output filename".into(),
                options: vec![
                    MetadataTLV::FlowLabel(FlowLabel{ value: vec![1, 2, 3] }),
                    MetadataTLV::FaultHandlerOverride(FaultHandlerOverride { fault_handler_code: HandlerCode::NoticeOfSuspension })

                ]
            }
        )

    ))]
    #[case(PDUPayload::Directive(
        Operations::Nak(
            NakPDU {
                start_of_scope: 12_u64,
                end_of_scope: 239585_u64,
                segment_requests: vec![
                    SegmentRequestForm{
                        start_offset:12,
                        end_offset: 64
                    },
                    SegmentRequestForm{
                        start_offset: 69,
                        end_offset: 4758
                    }
                ]
            }
        )

    ))]
    #[case(PDUPayload::Directive(Operations::Prompt(
        PromptPDU{
            nak_or_keep_alive: NakOrKeepAlive::Nak
        }
    )))]
    #[case(PDUPayload::Directive(Operations::KeepAlive(
        KeepAlivePDU{
            progress: 184
        }
    )))]
    #[case(PDUPayload::FileData(
        FileDataPDU::Unsegmented(
            UnsegmentedFileData {
                offset: 948,
                file_data: (0..12).collect()
            }
        )
    ))]
    #[case(PDUPayload::FileData(
        FileDataPDU::Segmented(
            SegmentedFileData {
                record_continuation_state: RecordContinuationState::First,
                segment_metadata: (0..50).step_by(2).collect(),
                offset: 757,
                file_data: (33..57).step_by(7).collect(),
            }
        )
    ))]
    fn pdu_len(
        #[case] payload: PDUPayload,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) {
        assert_eq!(
            payload.get_len(file_size_flag),
            payload.encode(file_size_flag).len() as u16,
        )
    }

    #[rstest]
    #[case(
        PDUPayload::Directive(Operations::EoF(EndOfFile {
            condition: Condition::NoError,
            checksum: 123749_u32,
            file_size: 7738949_u64,
            fault_location: None,
        }))
    )]
    #[case(
        PDUPayload::FileData(FileDataPDU::Unsegmented(UnsegmentedFileData{
            offset: 16_u64,
            file_data: "test some information".as_bytes().to_vec(),
        }))
    )]
    fn pdu_encoding(
        #[case] payload: PDUPayload,
        #[values(CRCFlag::NotPresent, CRCFlag::Present)] crc_flag: CRCFlag,
    ) -> PDUResult<()> {
        let pdu_data_field_length = payload.clone().encode(FileSizeFlag::Large).len() as u16;
        let pdu_type = match &payload {
            PDUPayload::Directive(_) => PDUType::FileDirective,
            PDUPayload::FileData(_) => PDUType::FileData,
        };

        let expected: PDU = PDU {
            header: PDUHeader {
                version: U3::One,
                pdu_type,
                direction: Direction::ToReceiver,
                transmission_mode: TransmissionMode::Acknowledged,
                crc_flag,
                large_file_flag: FileSizeFlag::Large,
                pdu_data_field_length,
                segmentation_control: SegmentationControl::NotPreserved,
                segment_metadata_flag: SegmentedData::NotPresent,
                source_entity_id: VariableID::from(18_u16),
                transaction_sequence_number: VariableID::from(7533_u16),
                destination_entity_id: VariableID::from(23_u16),
            },
            payload,
        };
        let buffer = expected.clone().encode();
        let recovered = PDU::decode(&mut buffer.as_slice())?;
        assert_eq!(expected, recovered);
        Ok(())
    }
}
