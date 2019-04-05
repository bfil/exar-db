use super::*;

use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::io::{Error as IoError, ErrorKind};

/// A list specifying categories of database error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseError {
    /// The credentials used to connect to the database are either missing or invalid.
    AuthenticationError,
    /// The connection to the database failed.
    ConnectionError,
    /// The event stream has been closed unexpectedly.
    EventStreamError(EventStreamError),
    /// An I/O error occurred.
    IoError(ErrorKind, String),
    /// The parsing of an event from the log file failed.
    ParseError(ParseError),
    /// The attempted subscription failed.
    SubscriptionError,
    /// The validation of the event failed.
    ValidationError(ValidationError),
    /// An unexpected error occurred.
    UnexpectedError
}

impl DatabaseError {
    /// Returns a `DatabaseError` from the given `std::io::Error`.
    pub fn from_io_error(err: IoError) -> DatabaseError {
        DatabaseError::IoError(err.kind(), format!("{}", err))
    }
}

impl ToTabSeparatedString for DatabaseError {
    fn to_tab_separated_string(&self) -> String {
        match *self {
            DatabaseError::AuthenticationError => tab_separated!("AuthenticationError"),
            DatabaseError::ConnectionError => tab_separated!("ConnectionError"),
            DatabaseError::EventStreamError(ref error) => {
                tab_separated!("EventStreamError", match *error {
                    EventStreamError::Empty  => "Empty",
                    EventStreamError::Closed => "Closed"
                })
            },
            DatabaseError::IoError(ref error_kind, ref description) => {
                tab_separated!("IoError", error_kind.to_tab_separated_string(), description)
            },
            DatabaseError::ParseError(ref error) => match *error {
                ParseError::ParseError(ref description) => tab_separated!("ParseError", "ParseError", description),
                ParseError::MissingField(index)         => tab_separated!("ParseError", "MissingField", index)
            },
            DatabaseError::SubscriptionError          => tab_separated!("SubscriptionError"),
            DatabaseError::ValidationError(ref error) => tab_separated!("ValidationError", error.description),
            DatabaseError::UnexpectedError            => tab_separated!("UnexpectedError")
        }
    }
}

impl FromTabSeparatedStr for DatabaseError {
    fn from_tab_separated_str(s: &str) -> Result<DatabaseError, ParseError> {
        let mut parser = TabSeparatedParser::new(2, s);
        let message_type: String = parser.parse_next()?;
        match &message_type[..] {
            "AuthenticationError" => Ok(DatabaseError::AuthenticationError),
            "ConnectionError" => Ok(DatabaseError::ConnectionError),
            "EventStreamError" => {
                let error: String = parser.parse_next()?;
                match &error[..] {
                    "Empty"  => Ok(DatabaseError::EventStreamError(EventStreamError::Empty)),
                    "Closed" => Ok(DatabaseError::EventStreamError(EventStreamError::Closed)),
                    x        => Err(ParseError::ParseError(format!("unknown event stream error: {}", x)))
                }
            },
            "IoError" => {
                let message_data: String      = parser.parse_next()?;
                let mut parser                = TabSeparatedParser::new(2, &message_data);
                let error_kind: String        = parser.parse_next()?;
                let error_kind                = ErrorKind::from_tab_separated_str(&error_kind)?;
                let error_description: String = parser.parse_next()?;
                Ok(DatabaseError::IoError(error_kind, error_description))
            },
            "ParseError" => {
                let message_data: String = parser.parse_next()?;
                let mut parser           = TabSeparatedParser::new(2, &message_data);
                let error_type: String   = parser.parse_next()?;
                match &error_type[..] {
                    "ParseError" => {
                        let error = ParseError::ParseError(parser.parse_next()?);
                        Ok(DatabaseError::ParseError(error))
                    },
                    "MissingField" => {
                        let error = ParseError::MissingField(parser.parse_next()?);
                        Ok(DatabaseError::ParseError(error))
                    },
                    x => Err(ParseError::ParseError(format!("unknown parse error: {}", x)))
                }
            },
            "SubscriptionError" => Ok(DatabaseError::SubscriptionError),
            "ValidationError" => {
                let description: String = parser.parse_next()?;
                Ok(DatabaseError::ValidationError(ValidationError::new(&description)))
            },
            "UnexpectedError" => Ok(DatabaseError::UnexpectedError),
            x => Err(ParseError::ParseError(format!("unknown database error: {}", x)))
        }
    }
}

impl ToTabSeparatedString for ErrorKind {
    fn to_tab_separated_string(&self) -> String {
        match *self {
            ErrorKind::NotFound          => "NotFound",
            ErrorKind::PermissionDenied  => "PermissionDenied",
            ErrorKind::ConnectionRefused => "ConnectionRefused",
            ErrorKind::ConnectionReset   => "ConnectionReset",
            ErrorKind::ConnectionAborted => "ConnectionAborted",
            ErrorKind::NotConnected      => "NotConnected",
            ErrorKind::AddrInUse         => "AddrInUse",
            ErrorKind::AddrNotAvailable  => "AddrNotAvailable",
            ErrorKind::BrokenPipe        => "BrokenPipe",
            ErrorKind::AlreadyExists     => "AlreadyExists",
            ErrorKind::WouldBlock        => "WouldBlock",
            ErrorKind::InvalidInput      => "InvalidInput",
            ErrorKind::InvalidData       => "InvalidData",
            ErrorKind::TimedOut          => "TimedOut",
            ErrorKind::WriteZero         => "WriteZero",
            ErrorKind::Interrupted       => "Interrupted",
            ErrorKind::Other             => "Other",
            ErrorKind::UnexpectedEof     => "UnexpectedEof",
            _                            => "Unknown"
        }.to_owned()
    }
}

impl FromTabSeparatedStr for ErrorKind {
    fn from_tab_separated_str(s: &str) -> Result<ErrorKind, ParseError> {
        match &s[..] {
            "NotFound"          => Ok(ErrorKind::NotFound),
            "PermissionDenied"  => Ok(ErrorKind::PermissionDenied),
            "ConnectionRefused" => Ok(ErrorKind::ConnectionRefused),
            "ConnectionReset"   => Ok(ErrorKind::ConnectionReset),
            "ConnectionAborted" => Ok(ErrorKind::ConnectionAborted),
            "NotConnected"      => Ok(ErrorKind::NotConnected),
            "AddrInUse"         => Ok(ErrorKind::AddrInUse),
            "AddrNotAvailable"  => Ok(ErrorKind::AddrNotAvailable),
            "BrokenPipe"        => Ok(ErrorKind::BrokenPipe),
            "AlreadyExists"     => Ok(ErrorKind::AlreadyExists),
            "WouldBlock"        => Ok(ErrorKind::WouldBlock),
            "InvalidInput"      => Ok(ErrorKind::InvalidInput),
            "InvalidData"       => Ok(ErrorKind::InvalidData),
            "TimedOut"          => Ok(ErrorKind::TimedOut),
            "WriteZero"         => Ok(ErrorKind::WriteZero),
            "Interrupted"       => Ok(ErrorKind::Interrupted),
            "Other"             => Ok(ErrorKind::Other),
            "UnexpectedEof"     => Ok(ErrorKind::UnexpectedEof),
            x                   => Err(ParseError::ParseError(format!("unknown error kind: {}", x)))
        }
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        match *self {
            DatabaseError::AuthenticationError                        => write!(f, "authentication failure"),
            DatabaseError::ConnectionError                            => write!(f, "connection failure"),
            DatabaseError::EventStreamError(EventStreamError::Closed) => write!(f, "event stream is closed"),
            DatabaseError::EventStreamError(EventStreamError::Empty)  => write!(f, "event stream is empty"),
            DatabaseError::IoError(_, ref error)                      => write!(f, "{}", error),
            DatabaseError::ParseError(ref error)                      => write!(f, "{}", error),
            DatabaseError::SubscriptionError                          => write!(f, "subscription failure"),
            DatabaseError::ValidationError(ref error)                 => write!(f, "{}", error),
            DatabaseError::UnexpectedError                            => write!(f, "unexpected error")
        }
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::io::ErrorKind;

    #[test]
    fn test_database_error_tab_separator_encoding() {
        let authentication_error = DatabaseError::AuthenticationError;
        let connection_error     = DatabaseError::ConnectionError;
        let event_stream_closed  = DatabaseError::EventStreamError(EventStreamError::Closed);
        let event_stream_empty   = DatabaseError::EventStreamError(EventStreamError::Empty);
        let io_error             = DatabaseError::IoError(ErrorKind::Other, "error".to_owned());
        let parse_error          = DatabaseError::ParseError(ParseError::ParseError("error".to_owned()));
        let missing_field        = DatabaseError::ParseError(ParseError::MissingField(1));
        let subscription_error   = DatabaseError::SubscriptionError;
        let validation_error     = DatabaseError::ValidationError(ValidationError { description: "error".to_owned() });
        let unexpected_error     = DatabaseError::UnexpectedError;

        assert_encoded_eq!(authentication_error, "AuthenticationError");
        assert_encoded_eq!(connection_error, "ConnectionError");
        assert_encoded_eq!(event_stream_closed, "EventStreamError\tClosed");
        assert_encoded_eq!(event_stream_empty, "EventStreamError\tEmpty");
        assert_encoded_eq!(io_error, "IoError\tOther\terror");
        assert_encoded_eq!(parse_error, "ParseError\tParseError\terror");
        assert_encoded_eq!(missing_field, "ParseError\tMissingField\t1");
        assert_encoded_eq!(subscription_error, "SubscriptionError");
        assert_encoded_eq!(validation_error, "ValidationError\terror");
        assert_encoded_eq!(unexpected_error, "UnexpectedError");
    }

    #[test]
    fn test_database_error_tab_separator_decoding() {
        let authentication_error = DatabaseError::AuthenticationError;
        let connection_error     = DatabaseError::ConnectionError;
        let event_stream_closed  = DatabaseError::EventStreamError(EventStreamError::Closed);
        let event_stream_empty   = DatabaseError::EventStreamError(EventStreamError::Empty);
        let io_error             = DatabaseError::IoError(ErrorKind::Other, "error".to_owned());
        let parse_error          = DatabaseError::ParseError(ParseError::ParseError("error".to_owned()));
        let missing_field        = DatabaseError::ParseError(ParseError::MissingField(1));
        let subscription_error   = DatabaseError::SubscriptionError;
        let validation_error     = DatabaseError::ValidationError(ValidationError { description: "error".to_owned() });
        let unexpected_error     = DatabaseError::UnexpectedError;

        assert_decoded_eq!("AuthenticationError", authentication_error);
        assert_decoded_eq!("ConnectionError", connection_error);
        assert_decoded_eq!("EventStreamError\tClosed", event_stream_closed);
        assert_decoded_eq!("EventStreamError\tEmpty", event_stream_empty);
        assert_decoded_eq!("IoError\tOther\terror", io_error);
        assert_decoded_eq!("ParseError\tParseError\terror", parse_error);
        assert_decoded_eq!("ParseError\tMissingField\t1", missing_field);
        assert_decoded_eq!("SubscriptionError", subscription_error);
        assert_decoded_eq!("ValidationError\terror", validation_error);
        assert_decoded_eq!("UnexpectedError", unexpected_error);
    }
}
