use exar::*;
use super::*;

use std::io::prelude::*;
use std::io::{BufReader, Lines, LineWriter};
use std::net::TcpStream;

/// A bidirectional TCP message stream.
///
/// It allows to send and receives `TcpMessage`s to and from the `TcpStream`.
#[derive(Debug)]
pub struct TcpMessageStream<T: Read + Write> {
    reader: BufReader<T>,
    writer: LineWriter<T>
}

impl<T: Read + Write + TryClone> TcpMessageStream<T> {
    /// Creates a `TcpMessageStream` from a given `TcpStream`,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn new(stream: T) -> DatabaseResult<TcpMessageStream<T>> {
        let cloned_stream = stream.try_clone()?;
        Ok(TcpMessageStream {
            reader: BufReader::new(cloned_stream),
            writer: LineWriter::new(stream)
        })
    }

    /// Receives and returns a `TcpMessage` from the TCP stream,
    /// or a `DatabaseError` if a failure occurs.
    pub fn read_message(&mut self) -> DatabaseResult<TcpMessage> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => Err(DatabaseError::ConnectionError),
            Ok(_) => {
                let trimmed_line = line.trim();
                match TcpMessage::from_tab_separated_str(trimmed_line) {
                    Ok(message) => Ok(message),
                    Err(err)    => Err(DatabaseError::ParseError(err))
                }
            },
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Sends a `TcpMessage` to the TCP stream,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn write_message(&mut self, message: TcpMessage) -> DatabaseResult<()> {
        self.writer.write_line(&message.to_tab_separated_string())
                   .map_err(DatabaseError::from_io_error)
                   .map(|_| ())
    }

    /// Returns an iterator over the messages received on the TCP stream.
    pub fn messages(self) -> TcpMessages<T> {
        TcpMessages::new(self.reader)
    }
}

pub trait TryClone where Self: Sized {
    fn try_clone(&self) -> DatabaseResult<Self>;
}

impl TryClone for TcpStream {
    fn try_clone(&self) -> DatabaseResult<Self> {
        self.try_clone().map_err(DatabaseError::from_io_error)
    }
}

impl<T: Read + Write + TryClone> TryClone for TcpMessageStream<T> {
    fn try_clone(&self) -> DatabaseResult<Self> {
        let cloned_stream = self.writer.get_ref().try_clone()?;
        TcpMessageStream::new(cloned_stream)
    }
}

/// An iterator over the messages received on a stream.
pub struct TcpMessages<T: Read> {
    lines: Lines<BufReader<T>>
}

impl<T: Read> TcpMessages<T> {
    pub fn new(reader: BufReader<T>) -> TcpMessages<T> {
        TcpMessages {
            lines: reader.lines()
        }
    }
}

impl<T: Read> Iterator for TcpMessages<T> {
    type Item = DatabaseResult<TcpMessage>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.lines.next() {
            Some(Ok(ref message)) => match TcpMessage::from_tab_separated_str(message) {
                                         Ok(message) => Some(Ok(message)),
                                         Err(err)    => Some(Err(DatabaseError::ParseError(err)))
                                     },
            Some(Err(err))        => Some(Err(DatabaseError::from_io_error(err))),
            None                  => None
        }
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_tcp_message_stream() {
        let log_file_name = temp_log_file_path();
        let log_stream    = LogMessageStream::new(&log_file_name).expect("Unable to create log stream");
        let mut stream    = TcpMessageStream::new(log_stream).expect("Unable to create message stream");

        let message = TcpMessage::Connect("collection".to_string(), None, None);

        assert!(stream.write_message(message.clone()).is_ok());
        assert_eq!(stream.read_message(), Ok(message.clone()));

        let mut messages = stream.try_clone().expect("Unable to clone message stream").messages();

        assert!(stream.write_message(message.clone()).is_ok());

        assert_eq!(messages.next(), Some(Ok(message.clone())));
        assert_eq!(messages.next(), Some(Ok(message)));

        assert_eq!(messages.next(), None);
    }
}
