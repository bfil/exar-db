extern crate rand;

use self::rand::Rng;

pub fn gen_collection_name() -> String {
    rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
}
