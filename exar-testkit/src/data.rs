use rand;
use rand::Rng;

use tempfile::tempdir;

pub fn random_collection_name() -> String {
    rand::thread_rng().gen_ascii_chars()
                      .take(10)
                      .collect::<String>()
}

pub fn invalid_collection_name() -> String {
    "invalid-directory/error".to_owned()
}

pub fn temp_dir() -> String {
    tempdir().expect("Unable to create temporary directory")
             .into_path().to_str().expect("Unable to create temporary directory")
             .to_owned()
}

pub fn temp_log_file_name() -> String {
    format!("{}.log", random_collection_name())
}

pub fn temp_log_file_path() -> String {
    format!("{}/{}", temp_dir(), temp_log_file_name())
}