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
/// let log = Log::new("/path/to/logs", "test", 100).expect("Unable to create log");
///
/// log.ensure_exists().unwrap();
///
/// let writer              = log.open_writer().unwrap();
/// let reader              = log.open_reader().unwrap();
/// let line_reader         = log.open_line_reader().unwrap();
/// let indexed_line_reader = log.open_indexed_line_reader().unwrap();
///
/// log.remove().unwrap();
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    path: String,
    name: String,
    index: LinesIndex,
    index_granularity: u64
}

impl Log {
    /// Returns a new `Log` pointing to the given path/name and using the given index granularity.
    pub fn new(path: &str, name: &str, index_granularity: u64) ->  Result<Log, DatabaseError> {
        let mut log = Log {
            path: path.to_owned(),
            name: name.to_owned(),
            index: LinesIndex::new(index_granularity),
            index_granularity: index_granularity
        };
        log.restore_index()?;
        Ok(log)
    }

    /// Ensure the underlying log file exists and creates it if it does not exist,
    /// it returns a `DatabaseError` if a failure occurs while creating the log file.
    pub fn ensure_exists(&self) -> Result<(), DatabaseError> {
        match OpenOptions::new().create(true).write(true).open(self.get_path()) {
            Ok(_)    => Ok(()),
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
    pub fn open_indexed_line_reader(&self) -> Result<IndexedLineReader<BufReader<File>>, DatabaseError> {
        let mut reader = self.open_line_reader()?;
        reader.restore_index(self.index.clone());
        Ok(reader)
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
                Ok(()) | Err(_) => Ok(())
            },
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
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

    fn compute_index(&self) -> Result<LinesIndex, DatabaseError> {
        self.ensure_exists()?;
        let mut reader = self.open_line_reader()?;
        match reader.compute_index() {
            Ok(_)    => Ok(reader.get_index().clone()),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    fn restore_index(&mut self) -> Result<(), DatabaseError> {
        match self.open_index_reader() {
            Ok(reader) => {
                let mut index = LinesIndex::new(self.index_granularity);
                for line in reader.lines() {
                    match line {
                        Ok(line) => {
                            let parts: Vec<_> = line.split(' ').collect();
                            let line_count: u64 = parts[0].parse().unwrap();
                            let byte_count: u64 = parts[1].parse().unwrap();
                            index.insert(line_count, byte_count);
                        },
                        Err(err) => return Err(DatabaseError::from_io_error(err))
                    }
                }
                let mut reader = self.open_line_reader()?;
                reader.restore_index(index);
                match reader.compute_index() {
                    Ok(_)    => {
                        self.index = reader.get_index().clone();
                        Ok(())
                    },
                    Err(err) => Err(DatabaseError::from_io_error(err))
                }
            },
            Err(_) => {
                self.index = self.compute_index()?;
                self.persist_index()
            }
        }
    }

    fn persist_index(&self) -> Result<(), DatabaseError> {
        let mut writer = self.open_index_writer()?;
        for (line_count, byte_count) in self.index.get_ref() {
            match writer.write_line(&format!("{} {}", line_count, byte_count)) {
                Ok(_) => (),
                Err(err) => return Err(DatabaseError::from_io_error(err))
            };
        }
        Ok(())
    }

    /// Adds a line to the lines index or returns a `DatabaseError` if a failure occurs.
    pub fn index_line(&mut self, offset: u64, bytes: u64) -> Result<(), DatabaseError> {
        self.index.insert(offset, bytes);
        self.persist_index()
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

    /// Returns the lines index for the log file.
    pub fn get_index(&self) -> &LinesIndex {
        &self.index
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
        let log = Log::new("", collection_name, 100).expect("Unable to create log");
        assert_eq!(log.get_path(), format!("{}.log", collection_name));
    }

    #[test]
    fn test_constructor_failure() {
        assert!(Log::new("", &invalid_collection_name(), 10).is_err());
    }

    #[test]
    fn test_log_and_index_management() {
        let ref collection_name = random_collection_name();

        let mut log = Log::new("", collection_name, 10).expect("Unable to create log");

        assert!(log.ensure_exists().is_ok());
        assert!(log.open_writer().is_ok());
        assert!(log.open_reader().is_ok());

        let index = log.compute_index().expect("Unable to compute index");
        assert_eq!(index.line_count(), 0);

        let mut writer = log.open_writer().expect("Unable to open writer");
        for _ in 0..100 {
            assert!(writer.write_line("data").is_ok());
        }
        drop(writer);

        let index = log.compute_index().expect("Unable to compute index");
        assert_eq!(index.line_count(), 100);

        log.index = index.clone();

        let reader = log.open_line_reader().expect("Unable to open reader");
        assert_eq!(*reader.get_index(), LinesIndex::new(10));

        let reader = log.open_indexed_line_reader().expect("Unable to open indexed reader");
        assert_eq!(*reader.get_index(), index);

        assert!(log.remove().is_ok());
        assert!(log.open_index_reader().is_err());

        let mut writer = log.open_writer().expect("Unable to open writer");
        for _ in 0..100 {
            assert!(writer.write_line("data").is_ok());
        }
        drop(writer);

        log.restore_index().expect("Unable to compute, persist and restore index");
        assert_eq!(*log.get_index(), index);

        assert!(log.open_index_reader().is_ok());

        log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(*log.get_index(), index);

        assert!(log.persist_index().is_ok());

        log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(*log.get_index(), index);

        assert!(log.remove().is_ok());

        assert!(log.open_reader().is_err());
    }
}
