use super::*;

use std::error::Error;
use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::io::ErrorKind;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseError {
    AuthenticationError,
    ConnectionError,
    EventStreamClosed,
    IoError(ErrorKind, String),
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
            DatabaseError::IoError(ref error_kind, ref description) => {
                tab_separated!("IoError", error_kind.to_tab_separated_string(), description)
            },
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
                Ok(DatabaseError::IoError(error_kind, error_description))
            },
            "ParseError" => {
                let message_data: String = try!(parser.parse_next());
                let mut parser = TabSeparatedParser::new(2, &message_data);
                let error_type: String = try!(parser.parse_next());
                match &error_type[..] {
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
            DatabaseError::IoError(_, ref error) => write!(f, "{}", error),
            DatabaseError::ParseError(ref error) => write!(f, "{}", error),
            DatabaseError::SubscriptionError => write!(f, "subscription failure"),
            DatabaseError::ValidationError(ref error) => write!(f, "{}", error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::io::ErrorKind;

    #[test]
    fn test_database_error_tab_separator_encoding() {
        let authentication_error = DatabaseError::AuthenticationError;
        let connection_error = DatabaseError::ConnectionError;
        let event_stream_closed = DatabaseError::EventStreamClosed;
        let io_error = DatabaseError::IoError(ErrorKind::Other, "error".to_owned());
        let parse_error = DatabaseError::ParseError(ParseError::ParseError("error".to_owned()));
        let missig_field = DatabaseError::ParseError(ParseError::MissingField(1));
        let subscription_error = DatabaseError::SubscriptionError;
        let validation_error = DatabaseError::ValidationError(ValidationError { description: "error".to_owned() });

        assert_encoded_eq!(authentication_error, "AuthenticationError");
        assert_encoded_eq!(connection_error, "ConnectionError");
        assert_encoded_eq!(event_stream_closed, "EventStreamClosed");
        assert_encoded_eq!(io_error, "IoError\tOther\terror");
        assert_encoded_eq!(parse_error, "ParseError\tParseError\terror");
        assert_encoded_eq!(missig_field, "ParseError\tMissingField\t1");
        assert_encoded_eq!(subscription_error, "SubscriptionError");
        assert_encoded_eq!(validation_error, "ValidationError\terror");
    }

    #[test]
    fn test_database_error_tab_separator_decoding() {
        let authentication_error = DatabaseError::AuthenticationError;
        let connection_error = DatabaseError::ConnectionError;
        let event_stream_closed = DatabaseError::EventStreamClosed;
        let io_error = DatabaseError::IoError(ErrorKind::Other, "error".to_owned());
        let parse_error = DatabaseError::ParseError(ParseError::ParseError("error".to_owned()));
        let missig_field = DatabaseError::ParseError(ParseError::MissingField(1));
        let subscription_error = DatabaseError::SubscriptionError;
        let validation_error = DatabaseError::ValidationError(ValidationError { description: "error".to_owned() });

        assert_decoded_eq!("AuthenticationError", Ok(authentication_error));
        assert_decoded_eq!("ConnectionError", Ok(connection_error));
        assert_decoded_eq!("EventStreamClosed", Ok(event_stream_closed));
        assert_decoded_eq!("IoError\tOther\terror", Ok(io_error));
        assert_decoded_eq!("ParseError\tParseError\terror", Ok(parse_error));
        assert_decoded_eq!("ParseError\tMissingField\t1", Ok(missig_field));
        assert_decoded_eq!("SubscriptionError", Ok(subscription_error));
        assert_decoded_eq!("ValidationError\terror", Ok(validation_error));
    }
}
