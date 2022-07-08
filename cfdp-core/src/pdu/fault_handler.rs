use std::io::Read;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use super::{
    error::{PDUError, PDUResult},
    header::PDUEncode,
};

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, FromPrimitive)]
/// Fault Handler Codes defined by CCSDS
/// Values of 0b0000 and 0b0101-0b1111 are reserved as of 2022.
pub enum HandlerCode {
    NoticeOfCancellation = 0b0001,
    NoticeOfSuspension = 0b0010,
    IgnoreError = 0b0011,
    AbandonTransaction = 0b0100,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultHandlerOverride {
    pub fault_handler_code: HandlerCode,
}
impl PDUEncode for FaultHandlerOverride {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        vec![self.fault_handler_code as u8]
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let fault_handler_code = {
            let possible_code = u8_buff[0];
            HandlerCode::from_u8(possible_code)
                .ok_or(PDUError::InvalidFaultHandlerCode(possible_code))?
        };
        Ok(Self { fault_handler_code })
    }
}
