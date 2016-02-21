use exar::*;

use std::error::Error;
use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::io::{Error as IoError, ErrorKind};
use std::str::FromStr;

/// Connect         database        [admin]         [secret]
/// Connected
///
/// Publish         tag1 tag2       timestamp       event_data
/// Published       1
///
/// Subscribe       live            0               100             [tag1 tag2]
/// Subscribed
///
/// Event           event_id        tag1 tag2       timestamp       event_data
/// EndOfStream
///
/// Error           type            [subtype]       description

#[derive(Debug)]
pub enum TcpMessage {
    Connect(String, Option<String>, Option<String>),
    Connected,
    Publish(Event),
    Published(usize),
    Subscribe(bool, usize, Option<usize>, Option<String>),
    Subscribed,
    Event(Event),
    EndOfStream,
    Error(DatabaseError)
}

impl ToString for TcpMessage {
    fn to_string(&self) -> String {
        match *self {
            TcpMessage::Connect(ref collection_name, ref username, ref password) => {
                match (username, password) {
                    (&Some(ref username), &Some(ref password)) => {
                        format!("Connect\t{}\t{}\t{}", collection_name, username, password)
                    },
                    _ => format!("Connect\t{}", collection_name)
                }
            },
            TcpMessage::Connected => format!("Connected"),
            TcpMessage::Publish(Event { ref id, ref data, ref tags, ref timestamp }) => {
                format!("Publish\t{}\t{}\t{}", tags.join(" "), timestamp, data)
            },
            TcpMessage::Published(ref event_id) => format!("Published\t{}", event_id),
            TcpMessage::Subscribe(ref live, ref offset, ref limit, ref tag) => {
                match (limit, tag) {
                    (&Some(ref limit), &Some(ref tag)) => {
                        format!("Subscribe\t{}\t{}\t{}\t{}", live, offset, limit, tag)
                    },
                    (&Some(ref limit), &None) => {
                        format!("Subscribe\t{}\t{}\t{}", live, offset, limit)
                    },
                    (&None, &Some(ref tag)) => {
                        format!("Subscribe\t{}\t{}\t{}\t{}", live, offset, 0, tag)
                    },
                    _ => format!("Subscribe\t{}\t{}", live, offset)
                }
            },
            TcpMessage::Subscribed => format!("Subscribed"),
            TcpMessage::Event(Event { ref id, ref data, ref tags, ref timestamp }) => {
                format!("Event\t{}\t{}\t{}\t{}", id, tags.join(" "), timestamp, data)
            },
            TcpMessage::EndOfStream => format!("EndOfStream"),
            TcpMessage::Error(ref error) => format!("Error\t{}", db_error_to_string(error))
        }
    }
}

impl FromStr for TcpMessage {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, ParseError> {
        let mut parser = TabSeparatedParser::new(2, s);
        let message_type: String = try!(parser.parse_next());
        match &message_type[..] {
            "Connect" => {
                let message_data: String = try!(parser.parse_next());
                let mut parser = TabSeparatedParser::new(3, &message_data);
                let collection_name = try!(parser.parse_next());
                let username = parser.parse_next().ok();
                let password = parser.parse_next().ok();
                Ok(TcpMessage::Connect(collection_name, username, password))
            },
            "Connected" => Ok(TcpMessage::Connected),
            "Publish" => {
                let message_data: String = try!(parser.parse_next());
                let mut parser = TabSeparatedParser::new(3, &message_data);
                let tags: String = try!(parser.parse_next());
                let timestamp = try!(parser.parse_next());
                let data: String = try!(parser.parse_next());
                let tags: Vec<_> = tags.split(" ").collect();
                Ok(TcpMessage::Publish(Event::new(&data, tags).with_timestamp(timestamp)))
            },
            "Published" => {
                let event_id = try!(parser.parse_next());
                Ok(TcpMessage::Published(event_id))
            },
            "Subscribe" => {
                let message_data: String = try!(parser.parse_next());
                let mut parser = TabSeparatedParser::new(4, &message_data);
                let live = try!(parser.parse_next());
                let offset = try!(parser.parse_next());
                let mut limit = parser.parse_next().ok();
                if limit.unwrap_or(0) <= 0 { limit = None }
                let tag = parser.parse_next().ok();
                Ok(TcpMessage::Subscribe(live, offset, limit, tag))
            },
            "Subscribed" => Ok(TcpMessage::Subscribed),
            "Event" => {
                let message_data: String = try!(parser.parse_next());
                message_data.parse().and_then(|event| Ok(TcpMessage::Event(event)))
            },
            "EndOfStream" => Ok(TcpMessage::EndOfStream),
            "Error" => {
                let message_data: String = try!(parser.parse_next());
                string_to_db_error(&message_data).and_then(|error| Ok(TcpMessage::Error(error)))
            },
            x => Err(ParseError::ParseError(format!("Unknown TCP message: {}", x)))
        }
    }
}

#[derive(Debug)]
pub struct UnexpectedTcpMessage;

impl Display for UnexpectedTcpMessage {
    fn fmt(&self, fmt: &mut Formatter) -> DisplayResult {
        write!(fmt, "Unexpected TCP Message")
    }
}

impl Error for UnexpectedTcpMessage {
    fn description(&self) -> &str {
        "Unexpected TCP Message"
    }
}

fn db_error_to_string(error: &DatabaseError) -> String {
    match *error {
        DatabaseError::AuthenticationError => format!("AuthenticationError"),
        DatabaseError::ConnectionError => format!("ConnectionError"),
        DatabaseError::EventStreamClosed => format!("EventStreamClosed"),
        DatabaseError::IoError(ref error) => format!("IoError\t{}\t{}", error_kind_to_string(error.kind()), error),
        DatabaseError::ParseError(ref error) => match error {
            &ParseError::ParseError(ref description) => format!("ParseError\tParseError\t{}", description),
            &ParseError::MissingField(index) => format!("ParseError\tMissingField\t{}", index)
        },
        DatabaseError::SubscriptionError => format!("SubscriptionError"),
        DatabaseError::ValidationError(ref error) => format!("ValidationError\t{}", error.description)
    }
}

fn string_to_db_error(s: &str) -> Result<DatabaseError, ParseError> {
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
            let error_kind = try!(string_to_error_kind(&error_kind));
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

fn error_kind_to_string(kind: ErrorKind) -> String {
    match kind {
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

fn string_to_error_kind(s: &str) -> Result<ErrorKind, ParseError> {
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
