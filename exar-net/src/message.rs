use exar::*;

use std::fmt::{Display, Formatter, Result as DisplayResult};

/// A list specifying categories of TCP message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpMessage {
    /// Message used to authenticate to Exar DB.
    Authenticate(String, String),
    /// Message used to acknowledge a successful authentication.
    Authenticated,
    /// Message used to select an Exar DB collection.
    Select(String),
    /// Message used to acknowledge a successful collection selection.
    Selected,
    /// Messaged used to drop an Exar DB collection.
    Drop(String),
    /// Message used to acknowledge a successful collection drop.
    Dropped,
    /// Message used to publish an event into a collection.
    Publish(Event),
    /// Message used to acknowledge a successfully published event.
    Published(u64),
    /// Message used to subscribe to an event stream.
    Subscribe(bool, u64, Option<u64>, Option<String>),
    /// Message used to acknowledge a successful subscription.
    Subscribed,
    /// Message used to unsubscribe from an event stream.
    Unsubscribe,
    /// Message containing an event.
    Event(Event),
    /// Message signaling the end of an event stream.
    EndOfEventStream,
    /// Message containing an error.
    Error(DatabaseError)
}

impl ToTabSeparatedString for TcpMessage {
    fn to_tab_separated_string(&self) -> String {
        match *self {
            TcpMessage::Authenticate(ref username, ref password) => tab_separated!("Authenticate", username, password),
            TcpMessage::Authenticated                            => tab_separated!("Authenticated"),
            TcpMessage::Select(ref collection_name)              => tab_separated!("Select", collection_name),
            TcpMessage::Selected                                 => tab_separated!("Selected"),
            TcpMessage::Drop(ref collection_name)                => tab_separated!("Drop", collection_name),
            TcpMessage::Dropped                                  => tab_separated!("Dropped"),
            TcpMessage::Publish(Event { ref data, ref tags, ref timestamp, .. }) => {
                tab_separated!("Publish", tags.join(" "), timestamp, data)
            },
            TcpMessage::Published(ref event_id)        => tab_separated!("Published", event_id),
            TcpMessage::Subscribe(ref live, ref offset, ref limit, ref tag) => {
                match (limit, tag) {
                    (&Some(ref limit), &Some(ref tag)) => tab_separated!("Subscribe", live, offset, limit, tag),
                    (&Some(ref limit), &None)          => tab_separated!("Subscribe", live, offset, limit),
                    (&None, &Some(ref tag))            => tab_separated!("Subscribe", live, offset, 0, tag),
                    _                                  => tab_separated!("Subscribe", live, offset)
                }
            },
            TcpMessage::Subscribed       => tab_separated!("Subscribed"),
            TcpMessage::Unsubscribe      => tab_separated!("Unsubscribe"),
            TcpMessage::Event(ref event) => tab_separated!("Event", event.to_tab_separated_string()),
            TcpMessage::EndOfEventStream => tab_separated!("EndOfEventStream"),
            TcpMessage::Error(ref error) => tab_separated!("Error", error.to_tab_separated_string())
        }
    }
}

impl FromTabSeparatedStr for TcpMessage {
    fn from_tab_separated_str(s: &str) -> Result<Self, ParseError> {
        let mut parser = TabSeparatedParser::new(2, s);
        let message_type: String = parser.parse_next()?;
        match &message_type[..] {
            "Authenticate" => {
                let message_data: String = parser.parse_next()?;
                let mut parser           = TabSeparatedParser::new(2, &message_data);
                let username             = parser.parse_next()?;
                let password             = parser.parse_next()?;
                Ok(TcpMessage::Authenticate(username, password))
            },
            "Authenticated" => Ok(TcpMessage::Authenticated),
            "Select" => {
                let collection_name = parser.parse_next()?;
                Ok(TcpMessage::Select(collection_name))
            },
            "Selected" => Ok(TcpMessage::Selected),
            "Drop"     => {
                let collection_name = parser.parse_next()?;
                Ok(TcpMessage::Drop(collection_name))
            },
            "Dropped"  => Ok(TcpMessage::Dropped),
            "Publish" => {
                let message_data: String = parser.parse_next()?;
                let mut parser           = TabSeparatedParser::new(3, &message_data);
                let tags: String         = parser.parse_next()?;
                let timestamp            = parser.parse_next()?;
                let data: String         = parser.parse_next()?;
                let tags: Vec<_>         = tags.split(' ').collect();
                Ok(TcpMessage::Publish(Event::new(&data, tags).with_timestamp(timestamp)))
            },
            "Published" => {
                let event_id = parser.parse_next()?;
                Ok(TcpMessage::Published(event_id))
            },
            "Subscribe" => {
                let message_data: String = parser.parse_next()?;
                let mut parser           = TabSeparatedParser::new(4, &message_data);
                let live                 = parser.parse_next()?;
                let offset               = parser.parse_next()?;
                let mut limit            = parser.parse_next().ok();
                if limit.unwrap_or(0) == 0 {
                    limit = None
                }
                let tag                  = parser.parse_next().ok();
                Ok(TcpMessage::Subscribe(live, offset, limit, tag))
            },
            "Subscribed"  => Ok(TcpMessage::Subscribed),
            "Unsubscribe" => Ok(TcpMessage::Unsubscribe),
            "Event" => {
                let message_data: String = parser.parse_next()?;
                Event::from_tab_separated_str(&message_data).and_then(|event| Ok(TcpMessage::Event(event)))
            },
            "EndOfEventStream" => Ok(TcpMessage::EndOfEventStream),
            "Error" => {
                let message_data: String = parser.parse_next()?;
                DatabaseError::from_tab_separated_str(&message_data).and_then(|error| Ok(TcpMessage::Error(error)))
            },
            x => Err(ParseError::ParseError(format!("unknown TCP message: {}", x)))
        }
    }
}

impl Display for TcpMessage {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        match *self {
            TcpMessage::Authenticate(ref username, ref password) => write!(f, "Authenticate({}, {})", username, password),
            TcpMessage::Authenticated                            => write!(f, "Authenticated"),
            TcpMessage::Select(ref collection_name)              => write!(f, "Select({})", collection_name),
            TcpMessage::Selected                                 => write!(f, "Selected"),
            TcpMessage::Drop(ref collection_name)                => write!(f, "Drop({})", collection_name),
            TcpMessage::Dropped                                  => write!(f, "Dropped"),
            TcpMessage::Publish(ref event)                       => write!(f, "Publish({})", event),
            TcpMessage::Published(ref event_id)                  => write!(f, "Published({})", event_id),
            TcpMessage::Subscribe(ref live, ref offset, ref limit, ref tag) => {
                match (limit, tag) {
                    (&Some(ref limit), &Some(ref tag))           => write!(f, "Subscribe({}, {}, {}, {})", live, offset, limit, tag),
                    (&Some(ref limit), &None)                    => write!(f, "Subscribe({}, {}, {})", live, offset, limit),
                    (&None, &Some(ref tag))                      => write!(f, "Subscribe({}, {}, {}, {})", live, offset, 0, tag),
                    _                                            => write!(f, "Subscribe({}, {})", live, offset)
                }
            },
            TcpMessage::Subscribed                               => write!(f, "Subscribed"),
            TcpMessage::Unsubscribe                              => write!(f, "Unsubscribe"),
            TcpMessage::Event(ref event)                         => write!(f, "Event({})", event),
            TcpMessage::EndOfEventStream                         => write!(f, "EndOfEventStream"),
            TcpMessage::Error(ref error)                         => write!(f, "Error({})", error)
        }
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_authenticate() {
        let message = TcpMessage::Authenticate("username".to_owned(), "password".to_owned());
        let string = "Authenticate\tusername\tpassword";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Authenticate(username, password)");
    }

    #[test]
    fn test_authenticated() {
        let message = TcpMessage::Authenticated;
        let string = "Authenticated";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Authenticated");
    }

    #[test]
    fn test_select() {
        let message = TcpMessage::Select("collection".to_owned());
        let string = "Select\tcollection";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Select(collection)");
    }

    #[test]
    fn test_selected() {
        let message = TcpMessage::Selected;
        let string = "Selected";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Selected");
    }

    #[test]
    fn test_drop() {
        let message = TcpMessage::Drop("collection".to_owned());
        let string = "Drop\tcollection";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Drop(collection)");
    }

    #[test]
    fn test_dropped() {
        let message = TcpMessage::Dropped;
        let string = "Dropped";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Dropped");
    }

    #[test]
    fn test_publish() {
        let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);
        let message = TcpMessage::Publish(event.clone());
        let string = "Publish\ttag1 tag2\t1234567890\tdata";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), format!("Publish({})", event));
    }

    #[test]
    fn test_published() {
        let message = TcpMessage::Published(1);
        let string = "Published\t1";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Published(1)");
    }

    #[test]
    fn test_subscribe() {
        let message = TcpMessage::Subscribe(true, 0, Some(100), Some("tag1".to_owned()));
        let string = "Subscribe\ttrue\t0\t100\ttag1";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Subscribe(true, 0, 100, tag1)");

        let message = TcpMessage::Subscribe(true, 0, Some(100), None);
        let string = "Subscribe\ttrue\t0\t100";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Subscribe(true, 0, 100)");

        let message = TcpMessage::Subscribe(true, 0, None, Some("tag1".to_owned()));
        let string = "Subscribe\ttrue\t0\t0\ttag1";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Subscribe(true, 0, 0, tag1)");

        let message = TcpMessage::Subscribe(true, 0, None, None);
        let string = "Subscribe\ttrue\t0";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Subscribe(true, 0)");
    }

    #[test]
    fn test_subscribed() {
        let message = TcpMessage::Subscribed;
        let string = "Subscribed";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Subscribed");
    }

    #[test]
    fn test_unsubscribe() {
        let message = TcpMessage::Unsubscribe;
        let string = "Unsubscribe";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Unsubscribe");
    }

    #[test]
    fn test_event() {
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1).with_timestamp(1234567890);
        let message = TcpMessage::Event(event.clone());
        let string = "Event\t1\t1234567890\ttag1 tag2\tdata";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), format!("Event({})", event));
    }

    #[test]
    fn test_end_of_event_stream() {
        let message = TcpMessage::EndOfEventStream;
        let string = "EndOfEventStream";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "EndOfEventStream");
    }

    #[test]
    fn test_error() {
        let message = TcpMessage::Error(DatabaseError::AuthenticationError);
        let string = "Error\tAuthenticationError";
        assert_encoded_eq!(message, string);
        assert_decoded_eq!(string, message.clone());
        assert_eq!(format!("{}", message), "Error(authentication failure)");
    }
}
