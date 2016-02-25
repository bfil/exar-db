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
    pub timestamp: usize
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

    pub fn with_timestamp(mut self, timestamp: usize) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn with_current_timestamp(mut self) -> Self {
        self.timestamp = self.get_current_time();
        self
    }

    pub fn without_empty_tags(mut self) -> Self {
        self.tags = self.tags.iter().filter(|t| !t.is_empty()).map(|x| x.to_string()).collect();
        self
    }

    fn get_current_time(&self) -> usize {
        let timespec = time::get_time();
        timespec.sec as usize * 1000 + timespec.nsec as usize / 1000 / 1000
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
    fn validate(self) -> Result<Self, ValidationError> {
        let event = self.without_empty_tags();
        if event.tags.is_empty() {
            Err(ValidationError::new("event must contain at least one tag"))
        } else {
            Ok(event)
        }
    }
}

pub struct EventStream {
    recv: Receiver<Event>
}

impl EventStream {
    pub fn new(recv: Receiver<Event>) -> EventStream {
        EventStream {
            recv: recv
        }
    }
    pub fn recv(&self) -> Result<Event, EventStreamError> {
        match self.recv.recv() {
            Ok(event) => Ok(event),
            Err(_) => Err(EventStreamError::Closed)
        }
    }
    pub fn try_recv(&self) -> Result<Event, EventStreamError> {
        match self.recv.try_recv() {
            Ok(event) => Ok(event),
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
pub enum EventStreamError {
    Empty,
    Closed
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::sync::mpsc::channel;

    #[test]
    fn test_event() {
        let event = Event::new("data", vec!["tag1", "tag2"]);
        assert_eq!(event.id, 0);
        assert_eq!(event.data, "data".to_owned());
        assert_eq!(event.tags, vec!["tag1".to_owned(), "tag2".to_owned()]);
        assert!(event.timestamp <= event.get_current_time());

        let event = event.with_id(1);
        assert_eq!(event.id, 1);

        let event = event.with_timestamp(1234567890);
        assert_eq!(event.timestamp, 1234567890);

        let event = event.with_current_timestamp();
        assert!(event.timestamp != 1234567890);
        assert!(event.timestamp <= event.get_current_time());

        let event = Event::new("data", vec!["tag1", "tag2", ""]);
        assert_eq!(event.tags, vec!["tag1".to_owned(), "tag2".to_owned(), "".to_owned()]);

        let event = event.without_empty_tags();
        assert_eq!(event.tags, vec!["tag1".to_owned(), "tag2".to_owned()]);
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
        let event = Event::new("data", vec![""]);
        assert_eq!(event.validate(), Err(ValidationError::new("event must contain at least one tag")));

        let event = Event::new("data", vec!["tag1", "tag2"]);
        assert_eq!(event.clone().validate(), Ok(event));
    }

    #[test]
    fn test_event_stream() {
        let event = Event::new("data", vec![""]);

        let (send, recv) = channel();

        let mut event_stream = EventStream::new(recv);

        assert!(send.send(event.clone()).is_ok());

        assert_eq!(event_stream.next(), Some(event));
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Empty));

        drop(send);

        assert_eq!(event_stream.next(), None);
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Closed));
        assert_eq!(event_stream.recv(), Err(EventStreamError::Closed));

    }
}
