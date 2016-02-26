use super::*;

use std::fs::File;

#[derive(Debug)]
pub struct Logger {
    writer: BufWriter<File>,
    offset: usize
}

impl Logger {
    pub fn new(log: Log) -> Result<Logger, DatabaseError> {
        log.open_writer().and_then(|writer| {
            log.count_lines().and_then(|lines_count| {
                Ok(Logger {
                    writer: writer,
                    offset: lines_count + 1
                })
            })
        })
    }

    pub fn log(&mut self, event: Event) -> Result<usize, DatabaseError> {
        match event.validate() {
            Ok(event) => {
                let event_id = self.offset;
                let mut event = event.with_id(event_id);
                if event.timestamp == 0 {
                    event = event.with_current_timestamp();
                }
                match self.writer.write_line(&event.to_tab_separated_string()) {
                    Ok(()) => {
                        self.offset += 1;
                        Ok(event_id)
                    },
                    Err(err) => Err(DatabaseError::new_io_error(err))
                }
            },
            Err(err) => Err(DatabaseError::ValidationError(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::io::{BufRead, BufReader};

    fn create_log() -> Log {
        let ref collection_name = testkit::gen_collection_name();
        Log::new("", collection_name)
    }

    #[test]
    fn test_constructor() {
        let log = create_log();
        let event = Event::new("data", vec!["tag1", "tag2"]);

        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");

        assert_eq!(logger.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(logger.offset, 1);

        assert_eq!(logger.log(event), Ok(1));

        let logger = Logger::new(log.clone()).expect("Unable to create logger");

        assert_eq!(logger.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(logger.offset, 2);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        let ref collection_name = testkit::invalid_collection_name();
        let log = Log::new("", collection_name);

        assert!(Logger::new(log.clone()).is_err());

        assert!(log.remove().is_err());
    }

    #[test]
    fn test_append() {
        let log = create_log();
        let event = Event::new("data", vec!["tag1", "tag2"]);

        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");

        assert_eq!(logger.log(event.clone()), Ok(1));
        assert_eq!(logger.offset, 2);
        assert_eq!(logger.log(event.clone()), Ok(2));
        assert_eq!(logger.offset, 3);

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
}
