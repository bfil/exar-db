use super::*;

use std::fs::File;

#[derive(Debug)]
pub struct Appender {
    writer: BufWriter<File>,
    offset: usize
}

impl Appender {
    pub fn new(log: Log) -> Result<Appender, DatabaseError> {
        match log.open_writer() {
            Ok(file) => {
                match log.count_lines() {
                    Ok(lines_count) => {
                        Ok(Appender {
                            writer: BufWriter::new(file),
                            offset:lines_count + 1
                        })
                    },
                    Err(err) => Err(DatabaseError::new_io_error(err))
                }
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn append(&mut self, event: Event) -> Result<usize, DatabaseError> {
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

        let mut appender = Appender::new(log.clone()).expect("Unable to create appender");

        assert_eq!(appender.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(appender.offset, 1);

        assert_eq!(appender.append(event), Ok(1));

        let appender = Appender::new(log.clone()).expect("Unable to create appender");

        assert_eq!(appender.writer.get_ref().metadata().unwrap().is_file(), true);
        assert_eq!(appender.offset, 2);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        let ref collection_name = testkit::invalid_collection_name();
        let log = Log::new("", collection_name);

        assert!(Appender::new(log.clone()).is_err());

        assert!(log.remove().is_err());
    }

    #[test]
    fn test_append() {
        let log = create_log();
        let event = Event::new("data", vec!["tag1", "tag2"]);

        let mut appender = Appender::new(log.clone()).expect("Unable to create appender");

        assert_eq!(appender.append(event.clone()), Ok(1));
        assert_eq!(appender.offset, 2);
        assert_eq!(appender.append(event.clone()), Ok(2));
        assert_eq!(appender.offset, 3);

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
