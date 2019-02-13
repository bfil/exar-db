use super::*;

/// Exar DB's subscription query.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let query = Query::new(true, 100, Some(20), Some("tag".to_owned()));
/// 
/// // or using the fluent API
/// let fluent_query = Query::live().offset(100).limit(20).by_tag("tag");
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    /// Indicates whether the query targets real-time events.
    pub live_stream: bool,
    /// Indicates the query target offset.
    pub offset: u64,
    /// Indicates the maximum number of events to be returned by the query, if specified.
    pub limit: Option<u64>,
    /// Indicates the query target event tag, if specified.
    pub tag: Option<String>
}

impl Query {
    /// Creates a new `Query` from the given parameters.
    pub fn new(live_stream: bool, offset: u64, limit: Option<u64>, tag: Option<String>) -> Query {
        Query { offset, limit, tag, live_stream }
    }

    /// Initializes a `Query` targeting the current events in the event log.
    pub fn current() -> Query {
        Query::new(false, 0, None, None)
    }

    /// Initializes a `Query` targeting the current and real-time events in the event log.
    pub fn live() -> Query {
        Query::new(true, 0, None, None)
    }

    /// Mutates and returns the query by updating its target offset.
    pub fn offset(mut self, offset: u64) -> Query {
        self.offset = offset;
        self
    }

    /// Mutates and returns the query by updating its limit.
    pub fn limit(mut self, limit: u64) -> Query {
        self.limit = Some(limit);
        self
    }

    /// Mutates and returns the query by updating its target event tag.
    pub fn by_tag(mut self, tag: &str) -> Query {
        self.tag = Some(tag.to_owned());
        self
    }

    /// Returns whether a given `Event` matches the query.
    pub fn matches(&self, event: &Event) -> bool {
        match self.tag {
            Some(ref tag) => event.tags.contains(tag),
            None          => true
        }
    }

    /// Returns the offsets interval the query targets.
    pub fn interval(&self) -> Interval<u64> {
        let start = self.offset;
        let end = if self.limit.is_none() || self.tag.is_some() {
            u64::max_value()
        } else {
            start + self.limit.unwrap()
        };
        Interval::new(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_constructors_and_modifiers() {
        let query = Query::new(true, 100, Some(20), Some("tag".to_owned()));
        assert_eq!(query.live_stream, true);
        assert_eq!(query.offset, 100);
        assert_eq!(query.limit, Some(20));
        assert_eq!(query.tag, Some("tag".to_owned()));

        let query = Query::current();
        assert_eq!(query.live_stream, false);
        assert_eq!(query.offset, 0);
        assert_eq!(query.limit, None);
        assert_eq!(query.tag, None);

        let query = Query::live();
        assert_eq!(query.live_stream, true);
        assert_eq!(query.offset, 0);
        assert_eq!(query.limit, None);
        assert_eq!(query.tag, None);

        let query = query.offset(100);
        assert_eq!(query.offset, 100);

        let query = query.limit(20);
        assert_eq!(query.limit, Some(20));

        let query = query.by_tag("tag");
        assert_eq!(query.tag, Some("tag".to_owned()));
    }

    #[test]
    fn test_event_matching() {
        let query = Query::current();

        assert!(query.matches(&Event::new("data", vec!["tag1"]).with_id(1)));

        let query = Query::current().by_tag("tag1");

        assert!(query.matches(&Event::new("data", vec!["tag1"]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec!["tag2"]).with_id(1)));
    }
}
