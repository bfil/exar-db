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

    pub fn count_lines_and_bytes(&self) -> Result<(usize, usize), DatabaseError> {
        match self.open_line_reader() {
            Ok(mut reader) => {
                let mut lines_count = 0;
                let mut bytes_count = 0;
                for line in (&mut reader).lines() {
                    match line {
                        Ok(line) => {
                            lines_count += 1;
                            bytes_count += line.as_bytes().len() + 1;
                        },
                        Err(err) => return Err(DatabaseError::new_io_error(err))
                    }
                }
                Ok((lines_count, bytes_count))
            },
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
    use exar_testkit::*;

    #[test]
    fn test_get_path() {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name);
        assert_eq!(log.get_path(), format!("{}.log", collection_name));
        let log = Log::new("path/to/log", collection_name);
        assert_eq!(log.get_path(), format!("path/to/log/{}.log", collection_name));
    }

    #[test]
    fn test_log() {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name);

        assert!(log.open_writer().is_ok());
        assert!(log.open_reader().is_ok());

        assert_eq!(log.count_lines_and_bytes().unwrap(), (0, 0));

        let mut file_writer = log.open_writer().expect("Unable to open file writer");

        assert!(file_writer.write_line("data").is_ok());
        assert_eq!(log.count_lines_and_bytes().unwrap(), (1, 5));

        assert!(log.remove().is_ok());

        assert!(log.open_reader().is_err());
    }
}
