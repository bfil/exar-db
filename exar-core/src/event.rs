use super::*;

use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::sync::mpsc::{Receiver, TryRecvError};

use time;

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub id: usize,
    pub data: String,
    pub tags: Vec<String>,
    pub timestamp: u64
}

impl Event {
    pub fn new(data: &str, tags: Vec<&str>) -> Event {
        Event {
            id: 0,
            data: data.to_owned(),
            tags: tags.iter().map(|x| x.to_string()).collect(),
            timestamp: 0
        }
    }

    pub fn with_id(mut self, id: usize) -> Self {
        self.id = id;
        self
    }

    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn with_current_timestamp(mut self) -> Self {
        self.timestamp = get_current_timestamp_in_ms();
        self
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        write!(f, "Event({}, {}, [{}], {})", self.id, self.timestamp, self.tags.join(", "), self.data)
    }
}

impl ToTabSeparatedString for Event {
    fn to_tab_separated_string(&self) -> String {
        tab_separated!(self.id, self.timestamp, self.tags.join(" "), self.data)
    }
}

impl FromTabSeparatedStr for Event {
    fn from_tab_separated_str(s: &str) -> Result<Event, ParseError> {
        let mut parser = TabSeparatedParser::new(4, s);
        let id = try!(parser.parse_next());
        let timestamp = try!(parser.parse_next());
        let tags: String = try!(parser.parse_next());
        let data: String = try!(parser.parse_next());
        let tags: Vec<_> = tags.split(" ").map(|x| x.to_owned()).collect();
        Ok(Event {
            id: id,
            tags: tags,
            data: data,
            timestamp: timestamp
        })
    }
}

impl Validation for Event {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.tags.is_empty() {
            return Err(ValidationError::new("event must contain at least one tag"));
        } else if self.tags.iter().any(|t| t.is_empty()) {
            return Err(ValidationError::new("event must not contain empty tags"));
        }
        Ok(())
    }
}

pub struct EventStream {
    recv: Receiver<EventStreamMessage>
}

impl EventStream {
    pub fn new(recv: Receiver<EventStreamMessage>) -> EventStream {
        EventStream {
            recv: recv
        }
    }
    pub fn recv(&self) -> Result<Event, EventStreamError> {
        match self.recv.recv() {
            Ok(EventStreamMessage::Event(event)) => Ok(event),
            Ok(EventStreamMessage::End) => Err(EventStreamError::Closed),
            Err(_) => Err(EventStreamError::Closed)
        }
    }
    pub fn try_recv(&self) -> Result<Event, EventStreamError> {
        match self.recv.try_recv() {
            Ok(EventStreamMessage::Event(event)) => Ok(event),
            Ok(EventStreamMessage::End) => Err(EventStreamError::Closed),
            Err(err) => match err {
                TryRecvError::Empty => Err(EventStreamError::Empty),
                TryRecvError::Disconnected => Err(EventStreamError::Closed)
            }
        }
    }
}

impl Iterator for EventStream {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStreamMessage {
    Event(Event),
    End
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStreamError {
    Empty,
    Closed
}

fn get_current_timestamp_in_ms() -> u64 {
    let timespec = time::get_time();
    timespec.sec as u64 * 1000 + timespec.nsec as u64 / 1000 / 1000
}

#[cfg(test)]
mod tests {
    use super::get_current_timestamp_in_ms;
    use super::super::*;

    use std::sync::mpsc::channel;

    #[test]
    fn test_event() {
        let event = Event::new("data", vec!["tag1", "tag2"]);
        assert_eq!(event.id, 0);
        assert_eq!(event.data, "data".to_owned());
        assert_eq!(event.tags, vec!["tag1".to_owned(), "tag2".to_owned()]);
        assert!(event.timestamp <= get_current_timestamp_in_ms());

        let event = event.with_id(1);
        assert_eq!(event.id, 1);

        let event = event.with_timestamp(1234567890);
        assert_eq!(event.timestamp, 1234567890);

        let event = event.with_current_timestamp();
        assert!(event.timestamp != 1234567890);
        assert!(event.timestamp <= get_current_timestamp_in_ms());
    }

    #[test]
    fn test_event_encoding() {
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1).with_timestamp(1234567890);
        assert_encoded_eq!(event, "1\t1234567890\ttag1 tag2\tdata");
    }

    #[test]
    fn test_event_decoding() {
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1).with_timestamp(1234567890);
        assert_decoded_eq!("1\t1234567890\ttag1 tag2\tdata", event);
    }

    #[test]
    fn test_event_validation() {
        let event = Event::new("data", vec![]);
        assert_eq!(event.validate(), Err(ValidationError::new("event must contain at least one tag")));

        let event = Event::new("data", vec![""]);
        assert_eq!(event.validate(), Err(ValidationError::new("event must not contain empty tags")));

        let event = Event::new("data", vec!["tag1", "tag2"]);
        assert_eq!(event.clone().validate(), Ok(()));
        assert_eq!(event.clone().validated(), Ok(event));
    }

    #[test]
    fn test_event_stream() {
        let event = Event::new("data", vec![""]);

        let (send, recv) = channel();

        let mut event_stream = EventStream::new(recv);

        assert!(send.send(EventStreamMessage::Event(event.clone())).is_ok());

        assert_eq!(event_stream.next(), Some(event));
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Empty));

        drop(send);

        assert_eq!(event_stream.next(), None);
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Closed));
        assert_eq!(event_stream.recv(), Err(EventStreamError::Closed));

    }
}
