/// Daemon related configurations and all [UserPrimitives](crate::daemon::UserPrimitive)
pub mod daemon;

/// Trait for interacting with a custom filestore and a default native implementation.
pub mod filestore;

/// All pdu definitions.
pub mod pdu;

/// Transaction related configurations and [Metadata](crate::transaction::Metadata)
pub mod transaction;

// re-exported for convenience and compatibility with the Primitives.
pub use tokio::sync::oneshot;

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
}
