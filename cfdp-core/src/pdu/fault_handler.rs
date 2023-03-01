use std::str::FromStr;

use async_trait::async_trait;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use tokio::io::AsyncReadExt;

use super::{
    error::{PDUError, PDUResult},
    header::PDUEncode,
};

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultHandlerAction {
    Cancel,
    Suspend,
    Ignore,
    Abandon,
}
impl FromStr for FaultHandlerAction {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.trim().to_lowercase().as_str() {
            "cancel" => Ok(Self::Cancel),
            "suspend" => Ok(Self::Suspend),
            "ignore" => Ok(Self::Ignore),
            "abandon" => Ok(Self::Abandon),
            _ => Err(()),
        }
    }
}

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
#[async_trait]
impl PDUEncode for FaultHandlerOverride {
    type PDUType = Self;

    fn encode(self) -> Vec<u8> {
        vec![self.fault_handler_code as u8]
    }

    async fn decode<T: AsyncReadExt + std::marker::Unpin + std::marker::Send>(
        buffer: &mut T,
    ) -> PDUResult<Self::PDUType> {
        let fault_handler_code = {
            let possible_code = buffer.read_u8().await?;
            HandlerCode::from_u8(possible_code)
                .ok_or(PDUError::InvalidFaultHandlerCode(possible_code))?
        };
        Ok(Self { fault_handler_code })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("cancel", FaultHandlerAction::Cancel)]
    #[case("suspend", FaultHandlerAction::Suspend)]
    #[case("Ignore", FaultHandlerAction::Ignore)]
    #[case("Abandon", FaultHandlerAction::Abandon)]
    #[case("CANCEL", FaultHandlerAction::Cancel)]
    #[case("SUSPEND", FaultHandlerAction::Suspend)]
    #[case("IGNORE", FaultHandlerAction::Ignore)]
    #[case("ABANDON", FaultHandlerAction::Abandon)]
    fn fault_action(#[case] input: &str, #[case] action: FaultHandlerAction) {
        assert_eq!(action, FaultHandlerAction::from_str(input).unwrap())
    }

    #[test]
    fn fault_error() {
        assert!(FaultHandlerAction::from_str("Hello, World").is_err())
    }
}
