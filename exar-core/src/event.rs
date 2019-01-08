use super::*;

use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::sync::mpsc::{Receiver, TryRecvError};

use time;

/// Exar DB's event.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let event = Event::new("data", vec!["tag1", "tag2"]);
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The event `id` (or sequence number).
    pub id: u64,
    /// The event data/payload.
    pub data: String,
    /// The event tags.
    pub tags: Vec<String>,
    /// The event timestamp.
    pub timestamp: u64
}

impl Event {
    /// Returns a new `Event` with the given data and tags.
    pub fn new(data: &str, tags: Vec<&str>) -> Event {
        Event {
            id: 0,
            data: data.to_owned(),
            tags: tags.iter().map(|x| x.to_string()).collect(),
            timestamp: 0
        }
    }

    /// Returns a modified version of the event by setting its `id` to the given value.
    pub fn with_id(mut self, id: u64) -> Self {
        self.id = id;
        self
    }

    /// Returns a modified version of the event by setting its timestamp to the given value.
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Returns a modified version of the event by setting its timestamp to the current time.
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
        let tags: Vec<_> = tags.split(' ').map(|x| x.to_owned()).collect();
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

/// Exar DB's event stream.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::channel;
///
/// let (sender, receiver) = channel();
/// let mut event_stream = EventStream::new(receiver);
///
/// let event = Event::new("data", vec!["tag1", "tag2"]);
/// let event_stream_message = EventStreamMessage::Event(event);
/// sender.send(event_stream_message);
/// # }
/// ```
pub struct EventStream {
    event_stream_receiver: Receiver<EventStreamMessage>
}

impl EventStream {
    /// Returns a new `EventStream` from the given `Receiver<EventStreamMessage>`.
    pub fn new(receiver: Receiver<EventStreamMessage>) -> EventStream {
        EventStream {
            event_stream_receiver: receiver
        }
    }
    /// Attempts to wait for an event on this event stream,
    /// returning an `EventStreamError` if the corresponding channel has hung up.
    ///
    /// This function will always block the current thread if there is no data available
    /// and it's possible for more data to be sent.
    pub fn recv(&self) -> Result<Event, EventStreamError> {
        match self.event_stream_receiver.recv() {
            Ok(EventStreamMessage::Event(event)) => Ok(event),
            Ok(EventStreamMessage::End) | Err(_) => Err(EventStreamError::Closed)
        }
    }
    /// Attempts to return a pending event on this event stream without blocking.
    ///
    /// This method will never block the caller in order to wait for the next event to become available.
    /// Instead, this will always return immediately with a possible option of pending data on the channel.
    pub fn try_recv(&self) -> Result<Event, EventStreamError> {
        match self.event_stream_receiver.try_recv() {
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

/// Exar DB's event stream message.
///
/// It can either be a message containing an event
/// or a message indicating the end of the event stream.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let event = Event::new("data", vec!["tag1", "tag2"]);
/// let event_stream_message = EventStreamMessage::Event(event);
/// let event_stream_end = EventStreamMessage::End;
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStreamMessage {
    /// The message containing an `Event`.
    Event(Event),
    /// The message indicating the end of the `EventStream`.
    End
}

/// A list specifying categories of event stream error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStreamError {
    /// The event stream is empty.
    Empty,
    /// The event stream has been closed.
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

        let (sender, receiver) = channel();

        let mut event_stream = EventStream::new(receiver);

        assert!(sender.send(EventStreamMessage::Event(event.clone())).is_ok());

        assert_eq!(event_stream.next(), Some(event));
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Empty));

        drop(sender);

        assert_eq!(event_stream.next(), None);
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Closed));
        assert_eq!(event_stream.recv(), Err(EventStreamError::Closed));

    }
}
