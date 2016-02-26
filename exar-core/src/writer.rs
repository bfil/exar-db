use super::*;

use std::fs::File;
use std::io::Write;
use std::sync::Mutex;

#[derive(Debug)]
pub struct Writer {
    file: Mutex<File>,
    offset: Mutex<usize>
}

impl Writer {
    pub fn new(log: Log) -> Result<Writer, DatabaseError> {
        match log.open_writer() {
            Ok(file) => {
                match log.count_lines() {
                    Ok(lines_count) => {
                        Ok(Writer {
                            file: Mutex::new(file),
                            offset: Mutex::new(lines_count + 1)
                        })
                    },
                    Err(err) => Err(DatabaseError::new_io_error(err))
                }
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn store(&self, event: Event) -> Result<usize, DatabaseError> {
        let mut file = self.file.lock().unwrap();
        let mut offset = self.offset.lock().unwrap();
        match event.validate() {
            Ok(event) => {
                let event_id = *offset;
                let mut event = event.with_id(event_id);
                if event.timestamp == 0 {
                    event = event.with_current_timestamp();
                }
                let event_string = format!("{}\n", event.to_tab_separated_string());
                match file.write_all(event_string.as_bytes()) {
                    Ok(()) => {
                        *offset += 1;
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

        let writer = Writer::new(log.clone()).expect("Unable to create writer");

        assert_eq!(writer.file.lock().unwrap().metadata().unwrap().is_file(), true);
        assert_eq!(*writer.offset.lock().unwrap(), 1);

        assert_eq!(writer.store(event), Ok(1));

        let writer = Writer::new(log.clone()).expect("Unable to create writer");

        assert_eq!(writer.file.lock().unwrap().metadata().unwrap().is_file(), true);
        assert_eq!(*writer.offset.lock().unwrap(), 2);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        let ref collection_name = testkit::invalid_collection_name();
        let log = Log::new("", collection_name);

        assert!(Writer::new(log.clone()).is_err());

        assert!(log.remove().is_err());
    }

    #[test]
    fn test_store() {
        let log = create_log();
        let event = Event::new("data", vec!["tag1", "tag2"]);

        let writer = Writer::new(log.clone()).expect("Unable to create writer");

        assert_eq!(writer.store(event.clone()), Ok(1));
        assert_eq!(*writer.offset.lock().unwrap(), 2);
        assert_eq!(writer.store(event.clone()), Ok(2));
        assert_eq!(*writer.offset.lock().unwrap(), 3);

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
