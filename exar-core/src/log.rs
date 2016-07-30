use super::*;

use indexed_line_reader::*;

use std::fs::*;
use std::io::{BufReader, BufWriter, BufRead};

/// Exar DB's log file abstraction.
///
/// It offers helper methods to manage a log file and its index.
/// It also allows to open readers and writers for the log file.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let log = Log::new("/path/to/logs", "test", 100);
///
/// let exists = log.ensure_exists().unwrap();
/// let writer = log.open_writer().unwrap();
/// let reader = log.open_reader().unwrap();
/// let index = log.compute_index().unwrap();
/// log.remove().unwrap();
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    path: String,
    name: String,
    index_granularity: u64
}

impl Log {
    /// Returns a new `Log` pointing to the given path/name and using the given index granularity.
    pub fn new(path: &str, name: &str, index_granularity: u64) -> Log {
        Log {
            path: path.to_owned(),
            name: name.to_owned(),
            index_granularity: index_granularity
        }
    }

    /// Ensure the underlying log file exists and creates it if it does not exist,
    /// it returns a `DatabaseError` if a failure occurs while creating the log file.
    pub fn ensure_exists(&self) -> Result<(), DatabaseError> {
        match OpenOptions::new().create(true).write(true).open(self.get_path()) {
            Ok(_) => Ok(()),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Returns a buffered reader for the underlying log file
    /// or a `DatabaseError` if a failure occurs.
    pub fn open_reader(&self) -> Result<BufReader<File>, DatabaseError> {
        match OpenOptions::new().read(true).open(self.get_path()) {
            Ok(file) => Ok(BufReader::new(file)),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Returns an indexed line reader for the underlying log file
    /// or a `DatabaseError` if a failure occurs.
    pub fn open_line_reader(&self) -> Result<IndexedLineReader<BufReader<File>>, DatabaseError> {
        match OpenOptions::new().read(true).open(self.get_path()) {
            Ok(file) => Ok(IndexedLineReader::new(BufReader::new(file), self.index_granularity)),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Returns an indexed line reader for the underlying log file and restores the index
    /// using the given `LinesIndex` or a `DatabaseError` if a failure occurs.
    pub fn open_line_reader_with_index(&self, index: LinesIndex) -> Result<IndexedLineReader<BufReader<File>>, DatabaseError> {
        self.open_line_reader().and_then(|mut reader| {
            reader.restore_index(index);
            Ok(reader)
        })
    }

    /// Returns a buffered writer for the underlying log file or a `DatabaseError` if a failure occurs.
    pub fn open_writer(&self) -> Result<BufWriter<File>, DatabaseError> {
        match OpenOptions::new().create(true).write(true).append(true).open(self.get_path()) {
            Ok(file) => Ok(BufWriter::new(file)),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Removes the underlying log file and its index or a `DatabaseError` if a failure occurs.
    pub fn remove(&self) -> Result<(), DatabaseError> {
        match remove_file(self.get_path()) {
            Ok(()) => match remove_file(self.get_index_path()) {
                Ok(()) => Ok(()),
                Err(_) => Ok(())
            },
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Computes and returns the `LinesIndex` for the underlying log file
    /// or a `DatabaseError` if a failure occurs.
    pub fn compute_index(&self) -> Result<LinesIndex, DatabaseError> {
        self.ensure_exists().and_then(|_| {
            self.open_line_reader().and_then(|mut reader| {
                match reader.compute_index() {
                    Ok(_) => Ok(reader.get_index().clone()),
                    Err(err) => Err(DatabaseError::from_io_error(err))
                }
            })
        })
    }

    /// Returns a buffered reader for the log index file or a `DatabaseError` if a failure occurs.
    pub fn open_index_reader(&self) -> Result<BufReader<File>, DatabaseError> {
        match OpenOptions::new().read(true).open(self.get_index_path()) {
            Ok(file) => Ok(BufReader::new(file)),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Returns a buffered writer for the log index file or a `DatabaseError` if a failure occurs.
    pub fn open_index_writer(&self) -> Result<BufWriter<File>, DatabaseError> {
        match OpenOptions::new().create(true).write(true).truncate(true).open(self.get_index_path()) {
            Ok(file) => Ok(BufWriter::new(file)),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Restores and returns the log `LinesIndex` from the log index file
    /// or a `DatabaseError` if a failure occurs.
    ///
    /// If the log index file does not exist it will be computed and persisted.
    pub fn restore_index(&self) -> Result<LinesIndex, DatabaseError> {
        match self.open_index_reader() {
            Ok(reader) => {
                let mut index = LinesIndex::new(self.index_granularity);
                for line in reader.lines() {
                    match line {
                        Ok(line) => {
                            let parts: Vec<_> = line.split(" ").collect();
                            let line_count: u64 = parts[0].parse().unwrap();
                            let byte_count: u64 = parts[1].parse().unwrap();
                            index.insert(line_count, byte_count);
                        },
                        Err(err) => return Err(DatabaseError::from_io_error(err))
                    }
                }
                self.open_line_reader().and_then(|mut reader| {
                    reader.restore_index(index);
                    match reader.compute_index() {
                        Ok(_) => Ok(reader.get_index().clone()),
                        Err(err) => Err(DatabaseError::from_io_error(err))
                    }
                })
            },
            Err(_) => self.compute_index().and_then(|index| {
                self.persist_index(&index).and_then(|_| {
                    Ok(index)
                })
            })
        }
    }

    /// Persists the given `LinesIndex` to a log index file
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn persist_index(&self, index: &LinesIndex) -> Result<(), DatabaseError> {
        self.open_index_writer().and_then(|mut writer| {
            for (line_count, byte_count) in index.get_ref() {
                match writer.write_line(&format!("{} {}", line_count, byte_count)) {
                    Ok(_) => (),
                    Err(err) => return Err(DatabaseError::from_io_error(err))
                };
            }
            Ok(())
        })
    }

    /// Returns the path to the log file.
    pub fn get_path(&self) -> String {
        if self.path.is_empty() {
            format!("{}.log", self.name)
        } else {
            format!("{}/{}.log", self.path, self.name)
        }
    }

    /// Returns the path to the log index file.
    pub fn get_index_path(&self) -> String {
        if self.path.is_empty() {
            format!("{}.index.log", self.name)
        } else {
            format!("{}/{}.index.log", self.path, self.name)
        }
    }

    /// Returns the lines index granularity for the log file.
    pub fn get_index_granularity(&self) -> u64 {
        self.index_granularity
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    use indexed_line_reader::*;

    #[test]
    fn test_get_path() {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name, 100);
        assert_eq!(log.get_path(), format!("{}.log", collection_name));
        let log = Log::new("path/to/log", collection_name, 100);
        assert_eq!(log.get_path(), format!("path/to/log/{}.log", collection_name));
    }

    #[test]
    fn test_log_and_index_management() {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name, 10);

        assert!(log.ensure_exists().is_ok());
        assert!(log.open_writer().is_ok());
        assert!(log.open_reader().is_ok());

        let index = log.compute_index().expect("Unable to compute index");
        assert_eq!(index.line_count(), 0);

        let mut writer = log.open_writer().expect("Unable to open writer");
        for _ in 0..100 {
            assert!(writer.write_line("data").is_ok());
        }

        let index = log.compute_index().expect("Unable to compute index");
        assert_eq!(index.line_count(), 100);

        let reader = log.open_line_reader().expect("Unable to open reader");
        assert_eq!(*reader.get_index(), LinesIndex::new(10));

        let reader = log.open_line_reader_with_index(index.clone()).expect("Unable to open reader");
        assert_eq!(*reader.get_index(), index);

        assert!(log.open_index_reader().is_err());

        let restored_index = log.restore_index().expect("Unable to compute, persist and restore index");
        assert_eq!(restored_index, index);

        assert!(log.open_index_reader().is_ok());

        let restored_index = log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(restored_index, index);

        assert!(log.persist_index(&LinesIndex::new(10)).is_ok());

        let restored_index = log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(restored_index, index);

        assert!(log.remove().is_ok());

        assert!(log.open_reader().is_err());
    }
}
