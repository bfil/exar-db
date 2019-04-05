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
/// let log = Log::new("test", &DataConfig::default()).expect("Unable to create log");
///
/// log.ensure_exists().unwrap();
///
/// let writer              = log.open_writer().unwrap();
/// let reader              = log.open_reader().unwrap();
/// let line_reader         = log.open_line_reader().unwrap();
/// let indexed_line_reader = log.open_line_reader_with_index().unwrap();
///
/// log.remove().unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Log {
    path: String,
    name: String,
    index: LinesIndex,
    index_granularity: u64
}

impl Log {
    /// Returns a new `Log` pointing to the given path/name and using the given index granularity.
    pub fn new(name: &str, config: &DataConfig) ->  DatabaseResult<Log> {
        let mut log = Log {
            path: config.path.to_owned(),
            name: name.to_owned(),
            index: LinesIndex::new(config.index_granularity),
            index_granularity: config.index_granularity
        };
        log.restore_index()?;
        Ok(log)
    }

    fn open_file(&self, open_options: &mut OpenOptions) -> DatabaseResult<File> {
        open_options.open(self.get_path()).map_err(DatabaseError::from_io_error)
    }

    /// Ensure the underlying log file exists and creates it if it does not exist,
    /// it returns a `DatabaseError` if a failure occurs while creating the log file.
    pub fn ensure_exists(&self) -> DatabaseResult<()> {
        self.open_file(OpenOptions::new().create(true).write(true)).map(|_| ())
    }

    /// Returns a buffered reader for the underlying log file
    /// or a `DatabaseError` if a failure occurs.
    pub fn open_reader(&self) -> DatabaseResult<BufReader<File>> {
        self.open_file(OpenOptions::new().read(true)).map(|file| BufReader::new(file))
    }

    /// Returns an indexed line reader for the underlying log file
    /// or a `DatabaseError` if a failure occurs.
    pub fn open_line_reader(&self) -> DatabaseResult<IndexedLineReader<BufReader<File>>> {
        self.open_file(OpenOptions::new().read(true)).map(|file| IndexedLineReader::new(BufReader::new(file), self.index_granularity))
    }

    /// Returns an indexed line reader for the underlying log file and restores the index
    /// using the given `LinesIndex` or a `DatabaseError` if a failure occurs.
    pub fn open_line_reader_with_index(&self) -> DatabaseResult<IndexedLineReader<BufReader<File>>> {
        let mut reader = self.open_line_reader()?;
        reader.restore_index(self.index.clone());
        Ok(reader)
    }

    /// Returns a buffered writer for the underlying log file or a `DatabaseError` if a failure occurs.
    pub fn open_writer(&self) -> DatabaseResult<BufWriter<File>> {
        self.open_file(OpenOptions::new().create(true).write(true).append(true)).map(|file| BufWriter::new(file))
    }

    /// Removes the underlying log file and its index or a `DatabaseError` if a failure occurs.
    pub fn remove(&self) -> DatabaseResult<()> {
        match remove_file(self.get_path()) {
            Ok(())   => match remove_file(self.get_index_path()) {
                            Ok(()) | Err(_) => Ok(())
                        },
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    fn open_index_file(&self, open_options: &mut OpenOptions) -> DatabaseResult<File> {
        open_options.open(self.get_index_path()).map_err(DatabaseError::from_io_error)
    }

    /// Returns a buffered reader for the log index file or a `DatabaseError` if a failure occurs.
    pub fn open_index_reader(&self) -> DatabaseResult<BufReader<File>> {
        self.open_index_file(OpenOptions::new().read(true)).map(|file| BufReader::new(file))
    }

    /// Returns a buffered writer for the log index file or a `DatabaseError` if a failure occurs.
    pub fn open_index_writer(&self) -> DatabaseResult<BufWriter<File>> {
        self.open_index_file(OpenOptions::new().create(true).write(true).truncate(true)).map(|file| BufWriter::new(file))
    }

    fn compute_index(&mut self, existing_index: Option<LinesIndex>) -> DatabaseResult<()> {
        self.ensure_exists()?;
        let mut reader = self.open_line_reader()?;
        existing_index.map(|index| reader.restore_index(index));
        match reader.compute_index() {
            Ok(_)    => Ok(self.index = reader.get_index().clone()),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    fn restore_index(&mut self) -> DatabaseResult<()> {
        match self.open_index_reader() {
            Ok(reader) => {
                let mut index = LinesIndex::new(self.index_granularity);
                let mut index_granularity_changed = false;
                for (i, line) in reader.lines().enumerate() {
                    match line {
                        Ok(line) => {
                            let parts: Vec<_> = line.split(' ').collect();
                            let line_count: u64 = parts[0].parse().unwrap();
                            let byte_count: u64 = parts[1].parse().unwrap();
                            if i == 0 && line_count != self.index_granularity {
                                index_granularity_changed = true;
                            }
                            if !index_granularity_changed {
                                index.insert(line_count, byte_count);
                            }
                        },
                        Err(err) => return Err(DatabaseError::from_io_error(err))
                    }
                }
                self.compute_index(Some(index))
            },
            Err(_) => {
                self.compute_index(None)?;
                self.persist_index()
            }
        }
    }

    fn persist_index(&self) -> DatabaseResult<()> {
        let mut writer = self.open_index_writer()?;
        for (line_count, byte_count) in self.index.get_ref() {
            match writer.write_line(&format!("{} {}", line_count, byte_count)) {
                Ok(_)    => (),
                Err(err) => return Err(DatabaseError::from_io_error(err))
            };
        }
        Ok(())
    }

    /// Adds a line to the lines index or returns a `DatabaseError` if a failure occurs.
    pub fn index_line(&mut self, offset: u64, bytes: u64) -> DatabaseResult<()> {
        self.index.insert(offset, bytes);
        self.persist_index()
    }

    /// Returns the name of the log file.
    pub fn get_name(&self) -> &str {
        &self.name
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

    pub fn line_count(&self) -> u64 {
        self.index.line_count()
    }

    pub fn byte_count(&self) -> u64 {
        self.index.byte_count()
    }

    /// Returns a cloned lines index for the log file.
    pub fn clone_index(&self) -> LinesIndex {
        self.index.clone()
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use indexed_line_reader::*;

    #[test]
    fn test_get_path_and_index_path() {
        let collection_name = random_collection_name();
        let path            = temp_dir();

        let log = Log {
            path: path.to_owned(),
            name: collection_name.to_owned(),
            index: LinesIndex::new(DEFAULT_INDEX_GRANULARITY),
            index_granularity: DEFAULT_INDEX_GRANULARITY
        };

        assert_eq!(log.get_path(), format!("{}/{}.log", path, collection_name));
        assert_eq!(log.get_index_path(), format!("{}/{}.index.log", path, collection_name));

        let log_with_empty_path = Log {
            path: "".to_owned(),
            name: collection_name.to_owned(),
            index: LinesIndex::new(DEFAULT_INDEX_GRANULARITY),
            index_granularity: DEFAULT_INDEX_GRANULARITY
        };

        assert_eq!(log_with_empty_path.get_path(), format!("{}.log", collection_name));
        assert_eq!(log_with_empty_path.get_index_path(), format!("{}.index.log", collection_name));
    }

    #[test]
    fn test_constructor_failure() {
        assert!(Log::new(&invalid_collection_name(), &temp_data_config(DEFAULT_INDEX_GRANULARITY)).is_err());
    }

    #[test]
    fn test_log_and_index_management() {
        let collection_name = random_collection_name();
        let path            = temp_dir();
        let data_config = DataConfig { path: path.to_owned(), index_granularity: 10 };

        let mut log = Log::new(&collection_name, &data_config).expect("Unable to create log");

        assert!(log.ensure_exists().is_ok());
        assert!(log.open_writer().is_ok());
        assert!(log.open_reader().is_ok());

        log.compute_index(None).expect("Unable to compute index");

        let expected_index = LinesIndex::new(10);

        assert_eq!(log.index, expected_index);

        let mut writer = log.open_writer().expect("Unable to open writer");
        for _ in 0..100 {
            assert!(writer.write_line("data").is_ok());
        }
        drop(writer);

        log.compute_index(None).expect("Unable to compute index");

        let mut expected_index = LinesIndex::new(10);
        expected_index.insert( 10, 50);
        expected_index.insert( 20, 100);
        expected_index.insert( 30, 150);
        expected_index.insert( 40, 200);
        expected_index.insert( 50, 250);
        expected_index.insert( 60, 300);
        expected_index.insert( 70, 350);
        expected_index.insert( 80, 400);
        expected_index.insert( 90, 450);
        expected_index.insert(100, 500);

        assert_eq!(log.index, expected_index);

        let reader = log.open_line_reader().expect("Unable to open reader");
        assert_eq!(*reader.get_index(), LinesIndex::new(10));

        let reader = log.open_line_reader_with_index().expect("Unable to open indexed reader");
        assert_eq!(*reader.get_index(), expected_index);

        assert!(log.remove().is_ok());
        assert!(log.open_index_reader().is_err());

        let mut writer = log.open_writer().expect("Unable to open writer");
        for _ in 0..100 {
            assert!(writer.write_line("data").is_ok());
        }
        drop(writer);

        log.restore_index().expect("Unable to compute, persist and restore index");
        assert_eq!(log.index, expected_index);

        assert!(log.open_index_reader().is_ok());

        log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(log.index, expected_index);

        assert!(log.persist_index().is_ok());

        log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(log.index, expected_index);

        let data_config = DataConfig { path: path.to_owned(), index_granularity: 100 };
        let mut log = Log::new(&collection_name, &data_config).expect("Unable to create log");

        log.restore_index().expect("Unable to restore persisted index");

        let mut expected_index = LinesIndex::new(100);
        expected_index.insert(100, 500);

        assert_eq!(log.index, expected_index);

        assert!(log.remove().is_ok());

        assert!(log.open_reader().is_err());
    }
}
