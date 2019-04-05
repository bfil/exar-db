pub use super::*;

#[macro_export]
macro_rules! tempfile {
    ($content:expr) => {
        {
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all($content.as_bytes()).unwrap();
            f.flush().unwrap();
            f
        }
    }
}