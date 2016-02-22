use super::*;

use std::error::Error;
use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::io::{Error as IoError, ErrorKind};

#[derive(Debug)]
pub enum DatabaseError {
    AuthenticationError,
    ConnectionError,
    EventStreamClosed,
    IoError(IoError),
    ParseError(ParseError),
    SubscriptionError,
    ValidationError(ValidationError)
}

impl ToTabSeparatedString for DatabaseError {
    fn to_tab_separated_string(&self) -> String {
        match *self {
            DatabaseError::AuthenticationError => tab_separated!("AuthenticationError"),
            DatabaseError::ConnectionError => tab_separated!("ConnectionError"),
            DatabaseError::EventStreamClosed => tab_separated!("EventStreamClosed"),
            DatabaseError::IoError(ref error) => tab_separated!("IoError", error.kind().to_tab_separated_string(), error),
            DatabaseError::ParseError(ref error) => match error {
                &ParseError::ParseError(ref description) => tab_separated!("ParseError", "ParseError", description),
                &ParseError::MissingField(index) => tab_separated!("ParseError", "MissingField", index)
            },
            DatabaseError::SubscriptionError => tab_separated!("SubscriptionError"),
            DatabaseError::ValidationError(ref error) => tab_separated!("ValidationError", error.description)
        }
    }
}

impl FromTabSeparatedString for DatabaseError {
    fn from_tab_separated_string(s: &str) -> Result<DatabaseError, ParseError> {
        let mut parser = TabSeparatedParser::new(2, s);
        let message_type: String = try!(parser.parse_next());
        match &message_type[..] {
            "AuthenticationError" => Ok(DatabaseError::AuthenticationError),
            "ConnectionError" => Ok(DatabaseError::ConnectionError),
            "EventStreamClosed" => Ok(DatabaseError::EventStreamClosed),
            "IoError" => {
                let message_data: String = try!(parser.parse_next());
                let mut parser = TabSeparatedParser::new(2, &message_data);
                let error_kind: String = try!(parser.parse_next());
                let error_kind = try!(ErrorKind::from_tab_separated_string(&error_kind));
                let error_description: String = try!(parser.parse_next());
                let io_error = IoError::new(error_kind, error_description);
                Ok(DatabaseError::IoError(io_error))
            },
            "ParseError" => {
                let message_data: String = try!(parser.parse_next());
                let mut parser = TabSeparatedParser::new(2, &message_data);
                let error_type: String = try!(parser.parse_next());
                match &error_type.to_uppercase()[..] {
                    "ParseError" => {
                        let error = ParseError::ParseError(try!(parser.parse_next()));
                        Ok(DatabaseError::ParseError(error))
                    },
                    "MissingField" => {
                        let error = ParseError::MissingField(try!(parser.parse_next()));
                        Ok(DatabaseError::ParseError(error))
                    },
                    x => Err(ParseError::ParseError(format!("Unknown parse error: {}", x)))
                }
            },
            "SubscriptionError" => Ok(DatabaseError::SubscriptionError),
            "ValidationError" => {
                let description: String = try!(parser.parse_next());
                Ok(DatabaseError::ValidationError(ValidationError::new(&description)))
            },
            x => Err(ParseError::ParseError(format!("Unknown database error: {}", x)))
        }
    }
}

impl ToTabSeparatedString for ErrorKind {
    fn to_tab_separated_string(&self) -> String {
        match *self {
            ErrorKind::NotFound => "NotFound",
            ErrorKind::PermissionDenied => "PermissionDenied",
            ErrorKind::ConnectionRefused => "ConnectionRefused",
            ErrorKind::ConnectionReset => "ConnectionReset",
            ErrorKind::ConnectionAborted => "ConnectionAborted",
            ErrorKind::NotConnected => "NotConnected",
            ErrorKind::AddrInUse => "AddrInUse",
            ErrorKind::AddrNotAvailable => "AddrNotAvailable",
            ErrorKind::BrokenPipe => "BrokenPipe",
            ErrorKind::AlreadyExists => "AlreadyExists",
            ErrorKind::WouldBlock => "WouldBlock",
            ErrorKind::InvalidInput => "InvalidInput",
            ErrorKind::InvalidData => "InvalidData",
            ErrorKind::TimedOut => "TimedOut",
            ErrorKind::WriteZero => "WriteZero",
            ErrorKind::Interrupted => "Interrupted",
            ErrorKind::Other => "Other",
            ErrorKind::UnexpectedEof => "UnexpectedEof",
            _ => "Unknown"
        }.to_owned()
    }
}

impl FromTabSeparatedString for ErrorKind {
    fn from_tab_separated_string(s: &str) -> Result<ErrorKind, ParseError> {
        match &s[..] {
            "NotFound" => Ok(ErrorKind::NotFound),
            "PermissionDenied" => Ok(ErrorKind::PermissionDenied),
            "ConnectionRefused" => Ok(ErrorKind::ConnectionRefused),
            "ConnectionReset" => Ok(ErrorKind::ConnectionReset),
            "ConnectionAborted" => Ok(ErrorKind::ConnectionAborted),
            "NotConnected" => Ok(ErrorKind::NotConnected),
            "AddrInUse" => Ok(ErrorKind::AddrInUse),
            "AddrNotAvailable" => Ok(ErrorKind::AddrNotAvailable),
            "BrokenPipe" => Ok(ErrorKind::BrokenPipe),
            "AlreadyExists" => Ok(ErrorKind::AlreadyExists),
            "WouldBlock" => Ok(ErrorKind::WouldBlock),
            "InvalidInput" => Ok(ErrorKind::InvalidInput),
            "InvalidData" => Ok(ErrorKind::InvalidData),
            "TimedOut" => Ok(ErrorKind::TimedOut),
            "WriteZero" => Ok(ErrorKind::WriteZero),
            "Interrupted" => Ok(ErrorKind::Interrupted),
            "Other" => Ok(ErrorKind::Other),
            "UnexpectedEof" => Ok(ErrorKind::UnexpectedEof),
            x => Err(ParseError::ParseError(format!("Unknown error kind: {}", x)))
        }
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        match *self {
            DatabaseError::AuthenticationError => write!(f, "authentication failure"),
            DatabaseError::ConnectionError => write!(f, "connection failure"),
            DatabaseError::EventStreamClosed => write!(f, "event stream is closed"),
            DatabaseError::IoError(ref error) => write!(f, "{}", error),
            DatabaseError::ParseError(ref error) => write!(f, "{}", error),
            DatabaseError::SubscriptionError => write!(f, "subscription failure"),
            DatabaseError::ValidationError(ref error) => write!(f, "{}", error)
        }
    }
}
