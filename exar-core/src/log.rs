use super::*;

use std::fs::*;
use std::io::{BufReader, BufWriter, Write};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    path: String,
    name: String,
    index_granularity: u64
}

impl Log {
    pub fn new(path: &str, name: &str, index_granularity: u64) -> Log {
        Log {
            path: path.to_owned(),
            name: name.to_owned(),
            index_granularity: index_granularity
        }
    }

    pub fn ensure_exists(&self) -> Result<(), DatabaseError> {
        match OpenOptions::new().create(true).write(true).open(self.get_path()) {
            Ok(_) => Ok(()),
            Err(err) => Err(DatabaseError::new_io_error(err))
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
            Ok(file) => Ok(IndexedLineReader::new(BufReader::new(file), self.index_granularity)),
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

    pub fn compute_index(&self) -> Result<LinesIndex, DatabaseError> {
        self.ensure_exists().and_then(|_| {
            self.open_line_reader().and_then(|mut reader| {
                match reader.compute_index() {
                    Ok(_) => Ok(reader.get_index().clone()),
                    Err(err) => Err(DatabaseError::new_io_error(err))
                }
            })
        })
    }

    pub fn get_path(&self) -> String {
        if self.path.is_empty() {
            format!("{}.log", self.name)
        } else {
            format!("{}/{}.log", self.path, self.name)
        }
    }

    pub fn get_index_granularity(&self) -> u64 {
        self.index_granularity
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    #[test]
    fn test_get_path() {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name, 100);
        assert_eq!(log.get_path(), format!("{}.log", collection_name));
        let log = Log::new("path/to/log", collection_name, 100);
        assert_eq!(log.get_path(), format!("path/to/log/{}.log", collection_name));
    }

    #[test]
    fn test_log() {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name, 100);

        assert!(log.open_writer().is_ok());
        assert!(log.open_reader().is_ok());

        let index = log.compute_index().expect("Unable to compute index");
        assert_eq!(index.lines_count(), 0);

        let mut file_writer = log.open_writer().expect("Unable to open file writer");
        assert!(file_writer.write_line("data").is_ok());

        let index = log.compute_index().expect("Unable to compute index");
        assert_eq!(index.lines_count(), 1);

        assert!(log.remove().is_ok());

        assert!(log.open_reader().is_err());
    }
}
