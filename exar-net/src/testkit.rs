pub use super::*;
pub use exar::*;

pub use exar_testkit::*;

use std::fs::*;
use std::io::{Error, Read, Write};

pub struct LogMessageStream {
    path: String,
    reader: File,
    writer: File
}

impl LogMessageStream {
    pub fn new(path: &str) -> Result<Self, Error> {
        let writer = OpenOptions::new().create(true).write(true).append(true).open(path)?;
        let reader = OpenOptions::new().read(true).open(path)?;
        Ok(LogMessageStream { path: path.to_owned(), reader, writer })
    }
}

impl Read for LogMessageStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.reader.read(buf)
    }
}

impl Write for LogMessageStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.writer.write(buf)
    }
    fn flush(&mut self) -> Result<(), Error> {
        self.writer.flush()
    }
}

impl TryClone for LogMessageStream {
    fn try_clone(&self) -> DatabaseResult<Self> {
        match LogMessageStream::new(&self.path) {
            Ok(cloned_stream) => Ok(cloned_stream),
            Err(err)          => Err(DatabaseError::from_io_error(err))
        }
    }
}