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
