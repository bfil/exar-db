use exar::*;
use super::*;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Lines};
use std::net::TcpStream;

/// A bidiectional TCP message stream.
///
/// It allows to send and receives `TcpMessage`s to and from the `TcpStream`.
#[derive(Debug)]
pub struct TcpMessageStream<T: Read + Write> {
    reader: BufReader<T>,
    writer: BufWriter<T>
}

impl<T: Read + Write + TryClone> TcpMessageStream<T> {
    /// Creates a `TcpMessageStream` from a given `TcpStream`,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn new(stream: T) -> Result<TcpMessageStream<T>, DatabaseError> {
        stream.try_clone().and_then(|cloned_stream| {
            Ok(TcpMessageStream {
                reader: BufReader::new(cloned_stream),
                writer: BufWriter::new(stream)
            })
        })
    }

    /// Receives and returns a `TcpMessage` from the TCP stream,
    /// or a `DatabaseError` if a failure occurs.
    pub fn recv_message(&mut self) -> Result<TcpMessage, DatabaseError> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(_) => {
                let trimmed_line = line.trim();
                match TcpMessage::from_tab_separated_str(trimmed_line) {
                    Ok(message) => Ok(message),
                    Err(err) => Err(DatabaseError::ParseError(err))
                }
            },
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Sends a `TcpMessage` to the TCP stream,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn send_message(&mut self, message: TcpMessage) -> Result<(), DatabaseError> {
        match self.writer.write_line(&message.to_tab_separated_string()) {
            Ok(_) => Ok(()),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Returns an iterator over the messages received on the TCP stream.
    pub fn messages(self) -> TcpMessages<T> {
        TcpMessages::new(self)
    }
}

pub trait TryClone where Self: Sized {
    fn try_clone(&self) -> Result<Self, DatabaseError>;
}

impl TryClone for TcpStream {
    fn try_clone(&self) -> Result<Self, DatabaseError> {
        match self.try_clone() {
            Ok(cloned_stream) => Ok(cloned_stream),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }
}

impl<T: Read + Write + TryClone> TryClone for TcpMessageStream<T> {
    fn try_clone(&self) -> Result<Self, DatabaseError> {
        self.writer.get_ref().try_clone().and_then(|cloned_stream| {
            TcpMessageStream::new(cloned_stream)
        })
    }
}

/// An iterator over the messages received on a stream.
pub struct TcpMessages<T: Read + Write> {
    lines: Lines<BufReader<T>>
}

impl<T: Read + Write> TcpMessages<T> {
    pub fn new(stream: TcpMessageStream<T>) -> TcpMessages<T> {
        TcpMessages {
            lines: stream.reader.lines()
        }
    }
}

impl<T: Read + Write> Iterator for TcpMessages<T> {
    type Item = Result<TcpMessage, DatabaseError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.lines.next() {
            Some(Ok(ref message)) => match TcpMessage::from_tab_separated_str(message) {
                Ok(message) => Some(Ok(message)),
                Err(err) => Some(Err(DatabaseError::ParseError(err)))
            },
            Some(Err(err)) => Some(Err(DatabaseError::from_io_error(err))),
            None => None
        }
    }
}

#[cfg(test)]
mod tests {
    use exar::*;
    use super::super::*;

    use std::fs::*;
    use std::io::{Error, Read, Write};

    struct LogStream {
        path: String,
        reader: File,
        writer: File
    }

    impl LogStream {
        pub fn new(path: &str) -> Result<Self, Error> {
            OpenOptions::new().create(true).write(true).append(true).open(path).and_then(|writer| {
                OpenOptions::new().read(true).open(path).and_then(|reader| {
                    Ok(LogStream {
                        path: path.to_owned(),
                        reader: reader,
                        writer: writer
                    })
                })
            })
        }
    }

    impl Read for LogStream {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
            self.reader.read(buf)
        }
    }

    impl Write for LogStream {
        fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
            self.writer.write(buf)
        }
        fn flush(&mut self) -> Result<(), Error> {
            self.writer.flush()
        }
    }

    impl TryClone for LogStream {
        fn try_clone(&self) -> Result<Self, DatabaseError> {
            match LogStream::new(&self.path) {
                Ok(cloned_stream) => Ok(cloned_stream),
                Err(err) => Err(DatabaseError::from_io_error(err))
            }
        }
    }

    #[test]
    fn test_tcp_message_stream() {

        let log_stream = LogStream::new("message-stream.log").expect("Unable to create log stream");
        let mut stream = TcpMessageStream::new(log_stream).expect("Unable to create message stream");

        let message = TcpMessage::Connect("collection".to_string(), None, None);

        assert!(stream.send_message(message.clone()).is_ok());
        assert_eq!(stream.recv_message(), Ok(message.clone()));

        let mut messages = stream.try_clone().expect("Unable to clone message stream").messages();

        assert!(stream.send_message(message.clone()).is_ok());

        assert_eq!(messages.next(), Some(Ok(message.clone())));
        assert_eq!(messages.next(), Some(Ok(message)));

        assert_eq!(messages.next(), None);

        assert!(remove_file("message-stream.log").is_ok());
    }
}
