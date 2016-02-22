use super::*;

use std::sync::mpsc::Receiver;

use time;

#[cfg_attr(feature = "rustc-serialize", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
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
        let timespec = time::get_time();
        self.timestamp = timespec.sec as usize * 1000 + timespec.nsec as usize / 1000 / 1000;
        self
    }

    pub fn without_empty_tags(mut self) -> Self {
        self.tags = self.tags.iter().filter(|t| !t.is_empty()).map(|x| x.to_string()).collect();
        self
    }
}

impl ToTabSeparatedString for Event {
    fn to_tab_separated_string(&self) -> String {
        tab_separated!(self.id, self.tags.join(" "), self.timestamp, self.data)
    }
}

impl FromTabSeparatedString for Event {
    fn from_tab_separated_string(s: &str) -> Result<Event, ParseError> {
        let mut parser = TabSeparatedParser::new(4, s);
        let id = try!(parser.parse_next());
        let tags: String = try!(parser.parse_next());
        let timestamp = try!(parser.parse_next());
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

impl Validation<Event> for Event {
    fn validate(self) -> Result<Self, ValidationError> {
        let event = self.without_empty_tags();
        if event.tags.is_empty() {
            Err(ValidationError::new("Events must contain at least one tag"))
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
}

impl Iterator for EventStream {
    type Item = Result<Event, DatabaseError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.recv.recv() {
            Ok(event) => Some(Ok(event)),
            Err(_) => Some(Err(DatabaseError::EventStreamClosed))
        }
    }
}
