use super::*;

use std::fs::*;
use std::io::{BufRead, BufReader, BufWriter, Write};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    path: String,
    name: String
}

impl Log {
    pub fn new(path: &str, name: &str) -> Log {
        Log {
            path: path.to_owned(),
            name: name.to_owned()
        }
    }

    pub fn open_reader(&self) -> Result<BufReader<File>, DatabaseError> {
        match OpenOptions::new().read(true).open(self.get_path()) {
            Ok(file) => Ok(BufReader::new(file)),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn open_line_reader(&self) -> Result<IndexedLineReader<BufReader<File>>, DatabaseError> {
        match OpenOptions::new().read(true).open(self.get_path()) {
            Ok(file) => Ok(IndexedLineReader::new(BufReader::new(file), 100000)),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn open_writer(&self) -> Result<BufWriter<File>, DatabaseError> {
        match OpenOptions::new().create(true).write(true).append(true).open(self.get_path()) {
            Ok(file) => Ok(BufWriter::new(file)),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn remove(&self) -> Result<(), DatabaseError> {
        match remove_file(self.get_path()) {
            Ok(()) => Ok(()),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn count_lines(&self) -> Result<usize, DatabaseError> {
        match self.open_reader() {
            Ok(reader) => Ok(reader.lines().count()),
            Err(err) => Err(err)
        }
    }

    pub fn get_path(&self) -> String {
        if self.path.is_empty() {
            format!("{}.log", self.name)
        } else {
            format!("{}/{}.log", self.path, self.name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_get_path() {
        let ref collection_name = testkit::gen_collection_name();
        let log = Log::new("", collection_name);
        assert_eq!(log.get_path(), format!("{}.log", collection_name));
        let log = Log::new("path/to/log", collection_name);
        assert_eq!(log.get_path(), format!("path/to/log/{}.log", collection_name));
    }

    #[test]
    fn test_log() {
        let ref collection_name = testkit::gen_collection_name();
        let log = Log::new("", collection_name);

        assert!(log.open_writer().is_ok());
        assert!(log.open_reader().is_ok());

        assert_eq!(log.count_lines().unwrap(), 0);

        let mut file_writer = log.open_writer().expect("Unable to open file writer");

        assert!(file_writer.write_line("data").is_ok());
        assert_eq!(log.count_lines().unwrap(), 1);

        assert!(log.remove().is_ok());

        assert!(log.open_reader().is_err());
    }
}
