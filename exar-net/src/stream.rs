use exar::*;
use super::*;

use std::io::prelude::*;
use std::io::{BufReader, Lines};
use std::net::TcpStream;

#[derive(Debug)]
pub struct Stream<T: Read + Write> {
    reader: BufReader<T>,
    writer: BufWriter<T>
}

impl<T: Read + Write + TryClone> Stream<T> {

    pub fn new(stream: T) -> Result<Stream<T>, DatabaseError> {
        stream.try_clone().and_then(|cloned_stream| {
            Ok(Stream {
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

    pub fn messages(self) -> Messages<T> {
        Messages::new(self)
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

impl TryClone for Stream<TcpStream> {
    fn try_clone(&self) -> Result<Self, DatabaseError> {
        match self.writer.get_ref().try_clone() {
            Ok(cloned_stream) => Stream::new(cloned_stream),
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }
}

pub struct Messages<T: Read + Write> {
    lines: Lines<BufReader<T>>
}

impl<T: Read + Write> Messages<T> {
    pub fn new(stream: Stream<T>) -> Messages<T> {
        Messages {
            lines: stream.reader.lines()
        }
    }
}

impl<T: Read + Write> Iterator for Messages<T> {
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
