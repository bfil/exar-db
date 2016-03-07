#[macro_export]
macro_rules! assert_encoded_eq {
    ($left:expr, $right:expr) => ( assert_eq!($left.to_tab_separated_string(), $right) )
}

#[macro_export]
macro_rules! assert_decoded_eq {
    ($left:expr, $right:expr) => (
        assert_eq!(FromTabSeparatedStr::from_tab_separated_str($left), Ok($right))
    )
}
