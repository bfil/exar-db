use exar::*;
use super::*;

use std::io::prelude::*;
use std::io::{BufReader, Lines};
use std::net::TcpStream;

#[derive(Debug)]
pub struct Stream {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>
}

impl Stream {

    pub fn new(stream: TcpStream) -> Result<Stream, DatabaseError> {
        match stream.try_clone() {
            Ok(cloned_stream) => {
                Ok(Stream {
                    reader: BufReader::new(cloned_stream),
                    writer: BufWriter::new(stream)
                })
            },
            Err(err) => Err(DatabaseError::IoError(err))
        }
    }

    pub fn receive_message(&mut self) -> Result<TcpMessage, DatabaseError> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(_) => {
                let trimmed_line = line.trim();
                match TcpMessage::from_tab_separated_string(&trimmed_line) {
                    Ok(message) => Ok(message),
                    Err(err) => Err(DatabaseError::ParseError(err))
                }
            },
            Err(err) => Err(DatabaseError::IoError(err))
        }
    }

    pub fn send_message(&mut self, message: TcpMessage) -> Result<(), DatabaseError> {
        match self.writer.write_line(&message.to_tab_separated_string()) {
            Ok(_) => Ok(()),
            Err(err) => Err(DatabaseError::IoError(err))
        }
    }

    pub fn messages(self) -> Messages {
        Messages::new(self)
    }

    pub fn try_clone(&self) -> Result<Self, DatabaseError> {
        match self.writer.get_ref().try_clone() {
            Ok(cloned_stream) => Stream::new(cloned_stream),
            Err(err) => Err(DatabaseError::IoError(err))
        }
    }
}

pub struct Messages {
    lines: Lines<BufReader<TcpStream>>
}

impl Messages {
    pub fn new(stream: Stream) -> Messages {
        Messages {
            lines: stream.reader.lines()
        }
    }
}

impl Iterator for Messages {
    type Item = Result<TcpMessage, DatabaseError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.lines.next() {
            Some(Ok(ref message)) => match TcpMessage::from_tab_separated_string(message) {
                Ok(message) => Some(Ok(message)),
                Err(err) => Some(Err(DatabaseError::ParseError(err)))
            },
            Some(Err(err)) => Some(Err(DatabaseError::IoError(err))),
            None => None
        }
    }
}
