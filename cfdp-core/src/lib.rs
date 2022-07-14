pub mod filestore;
pub mod pdu;

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
