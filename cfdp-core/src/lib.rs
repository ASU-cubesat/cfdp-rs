pub mod daemon;
pub mod filestore;
pub mod pdu;
pub(crate) mod segments;
pub(crate) mod timer;
pub mod transaction;
pub mod transport;
pub mod user;

// Re-exported for convenience
pub use crossbeam_channel::{Receiver, Sender, TryRecvError};

#[cfg(test)]
mod tests {

    #[macro_export]
    macro_rules! assert_err{
        ($expression:expr, $($pattern:tt)+) => {
            match $expression {
                $($pattern)+ => {},
                ref e => panic!("expected {} but got {:?}", stringify!($($pattern)+), e)
            }
        }
    }
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
