use super::*;

use std::fs::File;
use std::io::BufWriter;

#[derive(Debug)]
pub struct Logger {
    writer: BufWriter<File>,
    offset: u64,
    bytes_written: u64
}

impl Logger {
    pub fn new(log: Log) -> Result<Logger, DatabaseError> {
        log.restore_index().and_then(|index| {
            log.open_writer().and_then(|writer| {
                Ok(Logger {
                    writer: writer,
                    offset: index.line_count() + 1,
                    bytes_written: index.byte_count()
                })
            })
        })
    }

    pub fn log(&mut self, event: Event) -> Result<u64, DatabaseError> {
        match event.validated() {
            Ok(event) => {
                let event_id = self.offset;
                let mut event = event.with_id(event_id);
                if event.timestamp == 0 {
                    event = event.with_current_timestamp();
                }
                let event_string = event.to_tab_separated_string();
                match self.writer.write_line(&event_string) {
                    Ok(bytes_written) => {
                        self.offset += 1;
                        self.bytes_written += bytes_written as u64;
                        Ok(event_id)
                    },
                    Err(err) => Err(DatabaseError::new_io_error(err))
                }
            },
            Err(err) => Err(DatabaseError::ValidationError(err))
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    use std::io::{BufRead, BufReader};

    fn create_log() -> Log {
        let ref collection_name = random_collection_name();
        Log::new("", collection_name, 100)
    }

    #[test]
    fn test_constructor() {
        let log = create_log();
        let event = Event::new("data", vec!["tag1", "tag2"]);

        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");

        assert_eq!(logger.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(logger.offset, 1);
        assert_eq!(logger.bytes_written, 0);

        assert_eq!(logger.log(event), Ok(1));

        let logger = Logger::new(log.clone()).expect("Unable to create logger");

        assert_eq!(logger.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(logger.offset, 2);
        assert_eq!(logger.bytes_written, 31);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        let ref collection_name = invalid_collection_name();
        let log = Log::new("", collection_name, 10);

        assert!(Logger::new(log.clone()).is_err());

        assert!(log.remove().is_err());
    }

    #[test]
    fn test_log() {
        let log = create_log();
        let event = Event::new("data", vec!["tag1", "tag2"]);

        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");

        assert_eq!(logger.log(event.clone()), Ok(1));
        assert_eq!(logger.offset, 2);
        assert_eq!(logger.bytes_written, 31);
        assert_eq!(logger.log(event.clone()), Ok(2));
        assert_eq!(logger.offset, 3);
        assert_eq!(logger.bytes_written, 62);

        let reader = log.open_reader().expect("Unable to open reader");

        let mut lines = BufReader::new(reader).lines();

        let line = lines.next().expect("Unable to read next line")
                               .expect("Unable to read next line");

        let event = Event::from_tab_separated_str(&line).expect("Unable to decode event");

        assert_eq!(event.id, 1);
        assert_eq!(event.data, "data");
        assert_eq!(event.tags, vec!["tag1", "tag2"]);
        assert!(event.timestamp > 0);

        let line = lines.next().expect("Unable to read next line")
                               .expect("Unable to read next line");

        let event = Event::from_tab_separated_str(&line).expect("Unable to decode event");

        assert_eq!(event.id, 2);
        assert_eq!(event.data, "data");
        assert_eq!(event.tags, vec!["tag1", "tag2"]);
        assert!(event.timestamp > 0);

        assert!(lines.next().is_none());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_event_validation_failure() {
        let log = create_log();
        let event = Event::new("data", vec![]);

        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");

        let expected_validation_error = ValidationError::new("event must contain at least one tag");
        assert_eq!(logger.log(event.clone()), Err(DatabaseError::ValidationError(expected_validation_error)));

        assert!(log.remove().is_ok());
    }
}
