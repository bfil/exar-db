use super::*;

use std::fs::File;
use std::io::BufWriter;

/// Exar DB's event logger.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let log       = Log::new("/path/to/logs", "test", 100).expect("Unable to create log");
/// let publisher = Publisher::new(&PublisherConfig::default());
/// let scanner   = Scanner::new(&log, &publisher, &ScannerConfig::default()).expect("Unable to create scanner");
/// let event     = Event::new("data", vec!["tag1", "tag2"]);
///
/// let mut logger = Logger::new(&log, &publisher, &scanner).unwrap();
/// let event_id   = logger.log(event).unwrap();
/// # }
/// ```
#[derive(Debug)]
pub struct Logger {
    writer: BufWriter<File>,
    log: Log,
    publisher: Publisher,
    scanner: Scanner,
    offset: u64,
    bytes_written: u64
}

impl Logger {
    /// Creates a new logger for the given `Log` or returns a `DatabaseError` if a failure occurs.
    pub fn new(log: &Log, publisher: &Publisher, scanner: &Scanner) -> Result<Logger, DatabaseError> {
        let index = log.get_index();
        Ok(Logger {
            writer: log.open_writer()?,
            log: log.clone(),
            publisher: publisher.clone(),
            scanner: scanner.clone(),
            offset: index.line_count() + 1,
            bytes_written: index.byte_count()
        })
    }

    /// Appends the given event to the log and returns the `id` for the event logged
    /// or a `DatabaseError` if a failure occurs.
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
                        self.publisher.publish(event)?;
                        self.offset += 1;
                        self.bytes_written += bytes_written as u64;
                        if self.offset % self.log.get_index_granularity() == 0 {
                            self.log.index_line(self.offset, self.bytes_written)?;
                            self.scanner.update_index(self.log.get_index())?;
                        }
                        Ok(event_id)
                    },
                    Err(err) => Err(DatabaseError::from_io_error(err))
                }
            },
            Err(err) => Err(DatabaseError::ValidationError(err))
        }
    }

    /// Returns the total number of bytes logged.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    use std::io::{BufRead, BufReader};

    fn setup() -> (Log, Publisher, Scanner, Event) {
        let log       = Log::new("", &random_collection_name(), 10).expect("Unable to create log");
        let publisher = Publisher::new(&PublisherConfig::default());
        let scanner   = Scanner::new(&log, &publisher, &ScannerConfig::default()).expect("Unable to create scanner");
        let event     = Event::new("data", vec!["tag1", "tag2"]);
        (log, publisher, scanner, event)
    }

    #[test]
    fn test_constructor() {
        let (log, publisher, scanner, event) = setup();

        assert!(log.remove().is_ok());

        let collection_name = random_collection_name();
        let log             = Log::new("", &collection_name, 10).expect("Unable to create log");
        let mut logger      = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");

        assert_eq!(logger.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(logger.offset, 1);
        assert_eq!(logger.bytes_written, 0);

        assert_eq!(logger.log(event).expect("Unable to log event"), 1);

        drop(logger);

        let log    = Log::new("", &collection_name, 10).expect("Unable to create log");
        let logger = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");

        assert_eq!(logger.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(logger.offset, 2);
        assert_eq!(logger.bytes_written, 31);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_log() {
        let (log, publisher, scanner, event) = setup();

        let mut logger = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");

        assert_eq!(logger.log(event.clone()).expect("Unable to log event"), 1);
        assert_eq!(logger.offset, 2);
        assert_eq!(logger.bytes_written, 31);
        assert_eq!(logger.log(event.clone()).expect("Unable to log event"), 2);
        assert_eq!(logger.offset, 3);
        assert_eq!(logger.bytes_written, 62);

        drop(logger);

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
    fn test_index_updates() {
        let (log, publisher, scanner, event) = setup();

        let mut logger = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");

        for i in 0..100 {
            assert_eq!(logger.log(event.clone()), Ok(i+1));
        }

        assert_eq!(logger.log.get_index().get_ref().len(), 10);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_event_validation_failure() {
        let (log, publisher, scanner, _) = setup();

        let event = Event::new("data", vec![]);

        let mut logger = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");

        let expected_validation_error = ValidationError::new("event must contain at least one tag");
        assert_eq!(logger.log(event.clone()), Err(DatabaseError::ValidationError(expected_validation_error)));

        assert!(log.remove().is_ok());
    }
}
