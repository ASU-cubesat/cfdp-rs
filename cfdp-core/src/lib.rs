pub mod daemon;
pub mod filestore;
pub mod pdu;

pub mod transaction;
// Re-exported for convenience
pub use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};

// this import is necessary for the template macro in rstest_reuse as of v0.5.0
#[cfg(test)]
#[cfg_attr(test, allow(clippy::single_component_path_imports))]
use rstest_reuse;

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
