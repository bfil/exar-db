use exar::*;

use std::error::Error;
use std::fmt::{Display, Formatter, Result as DisplayResult};

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

impl ToTabSeparatedString for TcpMessage {
    fn to_tab_separated_string(&self) -> String {
        match *self {
            TcpMessage::Connect(ref collection_name, ref username, ref password) => {
                match (username, password) {
                    (&Some(ref username), &Some(ref password)) => {
                        tab_separated!("Connect", collection_name, username, password)
                    },
                    _ => tab_separated!("Connect", collection_name)
                }
            },
            TcpMessage::Connected => tab_separated!("Connected"),
            TcpMessage::Publish(Event { ref data, ref tags, ref timestamp, .. }) => {
                tab_separated!("Publish", tags.join(" "), timestamp, data)
            },
            TcpMessage::Published(ref event_id) => tab_separated!("Published", event_id),
            TcpMessage::Subscribe(ref live, ref offset, ref limit, ref tag) => {
                match (limit, tag) {
                    (&Some(ref limit), &Some(ref tag)) => tab_separated!("Subscribe", live, offset, limit, tag),
                    (&Some(ref limit), &None) => tab_separated!("Subscribe", live, offset, limit),
                    (&None, &Some(ref tag)) => tab_separated!("Subscribe", live, offset, 0, tag),
                    _ => tab_separated!("Subscribe", live, offset)
                }
            },
            TcpMessage::Subscribed => tab_separated!("Subscribed"),
            TcpMessage::Event(ref event) => tab_separated!("Event", event.to_tab_separated_string()),
            TcpMessage::EndOfStream => tab_separated!("EndOfStream"),
            TcpMessage::Error(ref error) => tab_separated!("Error", error.to_tab_separated_string())
        }
    }
}

impl FromTabSeparatedString for TcpMessage {
    fn from_tab_separated_string(s: &str) -> Result<Self, ParseError> {
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
                Event::from_tab_separated_string(&message_data).and_then(|event| Ok(TcpMessage::Event(event)))
            },
            "EndOfStream" => Ok(TcpMessage::EndOfStream),
            "Error" => {
                let message_data: String = try!(parser.parse_next());
                DatabaseError::from_tab_separated_string(&message_data).and_then(|error| Ok(TcpMessage::Error(error)))
            },
            x => Err(ParseError::ParseError(format!("Unknown TCP message: {}", x)))
        }
    }
}

impl Display for TcpMessage {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        match *self {
            TcpMessage::Connect(ref collection_name, ref username, ref password) => {
                match (username, password) {
                    (&Some(ref username), &Some(ref password)) => {
                        write!(f, "Connect({}, {}, {})", collection_name, username, password)
                    },
                    _ => write!(f, "Connect({})", collection_name)
                }
            },
            TcpMessage::Connected => write!(f, "Connected"),
            TcpMessage::Publish(ref event) => write!(f, "Publish({})", event),
            TcpMessage::Published(ref event_id) => write!(f, "Published({})", event_id),
            TcpMessage::Subscribe(ref live, ref offset, ref limit, ref tag) => {
                match (limit, tag) {
                    (&Some(ref limit), &Some(ref tag)) => write!(f, "Subscribe({}, {}, {}, {})", live, offset, limit, tag),
                    (&Some(ref limit), &None) => write!(f, "Subscribe({}, {}, {})", live, offset, limit),
                    (&None, &Some(ref tag)) => write!(f, "Subscribe({}, {}, {}, {})", live, offset, 0, tag),
                    _ => write!(f, "Subscribe({}, {})", live, offset)
                }
            },
            TcpMessage::Subscribed => write!(f, "Subscribed"),
            TcpMessage::Event(ref event) => write!(f, "Event({})", event),
            TcpMessage::EndOfStream => write!(f, "EndOfStream"),
            TcpMessage::Error(ref error) => write!(f, "Error({})", error)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnexpectedTcpMessage;

impl Display for UnexpectedTcpMessage {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        write!(f, "unexpected TCP message")
    }
}

impl Error for UnexpectedTcpMessage {
    fn description(&self) -> &str {
        "unexpected TCP message"
    }
}
