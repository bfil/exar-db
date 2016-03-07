use rand;
use rand::Rng;

pub fn random_collection_name() -> String {
    rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
}

pub fn invalid_collection_name() -> String {
    "missing-directory/error".to_owned()
}
