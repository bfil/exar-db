use exar::*;
use super::*;

use std::io::prelude::*;
use std::io::{BufReader, Lines};
use std::net::TcpStream;

#[derive(Debug)]
pub struct TcpMessageStream<T: Read + Write> {
    reader: BufReader<T>,
    writer: BufWriter<T>
}

impl<T: Read + Write + TryClone> TcpMessageStream<T> {
    pub fn new(stream: T) -> Result<TcpMessageStream<T>, DatabaseError> {
        stream.try_clone().and_then(|cloned_stream| {
            Ok(TcpMessageStream {
                reader: BufReader::new(cloned_stream),
                writer: BufWriter::new(stream)
            })
        })
    }

    pub fn recv_message(&mut self) -> Result<TcpMessage, DatabaseError> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(_) => {
                let trimmed_line = line.trim();
                match TcpMessage::from_tab_separated_str(&trimmed_line) {
                    Ok(message) => Ok(message),
                    Err(err) => Err(DatabaseError::ParseError(err))
                }
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn send_message(&mut self, message: TcpMessage) -> Result<(), DatabaseError> {
        match self.writer.write_line(&message.to_tab_separated_string()) {
            Ok(_) => Ok(()),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

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
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }
}

impl TryClone for TcpMessageStream<TcpStream> {
    fn try_clone(&self) -> Result<Self, DatabaseError> {
        match self.writer.get_ref().try_clone() {
            Ok(cloned_stream) => TcpMessageStream::new(cloned_stream),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }
}

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
            Some(Err(err)) => Some(Err(DatabaseError::new_io_error(err))),
            None => None
        }
    }
}
