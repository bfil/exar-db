#![macro_use]

use rand;
use rand::Rng;

pub fn gen_collection_name() -> String {
    rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
}

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
