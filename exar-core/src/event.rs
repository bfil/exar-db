use super::*;

use std::fmt::{Display, Formatter, Result as DisplayResult};
use std::str::FromStr;

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
/// let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]);
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The event `id` (or sequence number).
    pub id: u64,
    /// The event data/payload.
    pub data: String,
    /// The event tags.
    pub tags: Vec<Tag>,
    /// The event timestamp.
    pub timestamp: u64
}

impl Event {
    /// Returns a new `Event` with the given data and tags.
    pub fn new(data: &str, tags: Vec<Tag>) -> Event {
        Event {
            id: 0,
            data: data.to_owned(),
            tags: tags,
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
        let tags: Vec<String> = self.tags.iter().map(|t| t.to_string()).collect();
        write!(f, "Event({}, {}, [{}], {})", self.id, self.timestamp, tags.join(", "), self.data)
    }
}

impl ToTabSeparatedString for Event {
    fn to_tab_separated_string(&self) -> String {
        let tags: Vec<String> = self.tags.iter().map(|t| t.to_string()).collect();
        tab_separated!(self.id, self.timestamp, tags.join(" "), self.data)
    }
}

impl FromTabSeparatedStr for Event {
    fn from_tab_separated_str(s: &str) -> Result<Event, ParseError> {
        let mut parser     = TabSeparatedParser::new(4, s);
        let id             = parser.parse_next()?;
        let timestamp      = parser.parse_next()?;
        let tags: String   = parser.parse_next()?;
        let data: String   = parser.parse_next()?;
        let tags: Vec<Tag> = tags.split(' ').map(|x| x.parse()).collect::<Result<Vec<Tag>, ParseError>>()?;
        Ok(Event { id, tags, data, timestamp })
    }
}

impl Validation for Event {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.tags.is_empty() {
            return Err(ValidationError::new("event must contain at least one tag"));
        } else if self.tags.iter().any(|t| t.value.is_empty()) {
            return Err(ValidationError::new("event tag values must not be empty"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    pub name: Option<String>,
    pub value: String,
    pub version: Option<u64>
}

impl Tag {
    pub fn new(value: &str) -> Self {
        Tag {
            name: None,
            value: value.to_owned(),
            version: None
        }
    }

    pub fn named(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }
}

impl FromStr for Tag {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let name_and_value: Vec<&str> = s.split('=').collect();
        let (name, value) = match &name_and_value[..] {
            &[name, value] => (Some(name.to_owned()), value),
            &[value]       => (None, value),
            _              => return Err(ParseError::ParseError(format!("unable to parse tag: {}", s)))
        };
        let value_and_version: Vec<&str> = value.split(':').collect();
        let (value, version) = match &value_and_version[..] {
            &[value, version] => {
                                     let version: u64 = version.parse().map_err(|err| {
                                         ParseError::ParseError(format!("unable to parse tag version: {}, {}", version, err))
                                     })?;
                                     (value.to_owned(), Some(version))
                                 },
            &[value]          => (value.to_owned(), None),
            _                 => return Err(ParseError::ParseError(format!("unable to parse tag: {}", s)))
        };
        Ok(Tag { name, value, version })
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        match (&self.name, &self.version) {
            (None,       None)          => write!(f, "{}",       self.value),
            (None,       Some(version)) => write!(f, "{}:{}",    self.value, version),
            (Some(name), None)          => write!(f, "{}={}",    name, self.value, ),
            (Some(name), Some(version)) => write!(f, "{}={}:{}", name, self.value, version)
        }
    }
}

fn get_current_timestamp_in_ms() -> u64 {
    let timespec = time::get_time();
    timespec.sec as u64 * 1000 + timespec.nsec as u64 / 1000 / 1000
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_event() {
        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]);
        assert_eq!(event.id, 0);
        assert_eq!(event.data, "data".to_owned());
        assert_eq!(event.tags, vec![Tag::new("tag1"), Tag::new("tag2")]);
        assert!(event.timestamp <= super::get_current_timestamp_in_ms());

        let event = event.with_id(1);
        assert_eq!(event.id, 1);

        let event = event.with_timestamp(1234567890);
        assert_eq!(event.timestamp, 1234567890);

        let event = event.with_current_timestamp();
        assert_ne!(event.timestamp, 1234567890);
        assert!(event.timestamp <= super::get_current_timestamp_in_ms());
    }

    #[test]
    fn test_event_encoding() {
        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_id(1).with_timestamp(1234567890);
        assert_encoded_eq!(event, "1\t1234567890\ttag1 tag2\tdata");
    }

    #[test]
    fn test_event_decoding() {
        let expected_event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_id(1).with_timestamp(1234567890);
        assert_decoded_eq!("1\t1234567890\ttag1 tag2\tdata", expected_event);
    }

    #[test]
    fn test_event_validation() {
        let event = Event::new("data", vec![]);
        assert_eq!(event.validate(), Err(ValidationError::new("event must contain at least one tag")));

        let event = Event::new("data", vec![Tag::new("")]);
        assert_eq!(event.validate(), Err(ValidationError::new("event tag values must not be empty")));

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]);
        assert_eq!(event.clone().validate(), Ok(()));
        assert_eq!(event.clone().validated(), Ok(event));
    }

    #[test]
    fn test_tag() {
        let tag = Tag::new("tag");
        assert_eq!(tag.name, None);
        assert_eq!(tag.value, "tag".to_owned());
        assert_eq!(tag.version, None);

        let tag = tag.named("name");
        assert_eq!(tag.name, Some("name".to_owned()));

        let tag = tag.with_version(1);
        assert_eq!(tag.version, Some(1));
    }

    #[test]
    fn test_tag_encoding() {
        let tag = Tag::new("tag");
        assert_eq!(tag.to_string(), "tag".to_owned());

        let tag = tag.named("name");
        assert_eq!(tag.to_string(), "name=tag".to_owned());

        let tag = tag.with_version(1);
        assert_eq!(tag.to_string(), "name=tag:1".to_owned());

        let tag = Tag::new("tag").with_version(1);
        assert_eq!(tag.to_string(), "tag:1".to_owned());
    }

    #[test]
    fn test_tag_decoding() {
        let expected_tag = Tag::new("tag");
        assert_eq!("tag".parse(), Ok(expected_tag));

        let expected_tag = Tag::new("tag").named("name");
        assert_eq!("name=tag".parse(), Ok(expected_tag));

        let expected_tag = Tag::new("tag").named("name").with_version(1);
        assert_eq!("name=tag:1".parse(), Ok(expected_tag));

        let expected_tag = Tag::new("tag").with_version(1);
        assert_eq!("tag:1".parse(), Ok(expected_tag));
    }

    #[test]
    fn test_tag_decoding_failure() {
        assert_eq!("name==tag".parse::<Tag>(), Err(ParseError::ParseError("unable to parse tag: name==tag".to_owned())));
        assert_eq!("tag::version".parse::<Tag>(), Err(ParseError::ParseError("unable to parse tag: tag::version".to_owned())));
        assert_eq!("name=version:x".parse::<Tag>(), Err(ParseError::ParseError("unable to parse tag version: x, invalid digit found in string".to_owned())));
    }
}
