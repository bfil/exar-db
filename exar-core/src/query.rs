use super::*;

/// Exar DB's query.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let query = Query::new(true, 100, Some(20), Some(Tag::new("tag")));
/// 
/// // or using the fluent API
/// let fluent_query = Query::live().offset(100).limit(20).by_tag(Tag::new("tag"));
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
    pub tag: Option<Tag>
}

impl Query {
    /// Creates a new `Query` from the given parameters.
    pub fn new(live_stream: bool, offset: u64, limit: Option<u64>, tag: Option<Tag>) -> Query {
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
    pub fn by_tag(mut self, tag: Tag) -> Query {
        self.tag = Some(tag);
        self
    }

    /// Returns whether a given `Event` matches the query.
    pub fn matches(&self, event: &Event) -> bool {
        match &self.tag {
            Some(query_tag) => event.tags.iter().any(|event_tag| {
                query_tag.value == event_tag.value &&
                query_tag.name  == event_tag.name &&
                query_tag.version.map(|version| Some(version) == event_tag.version).unwrap_or(true)
            }),
            None => true
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
    use testkit::*;

    #[test]
    fn test_constructors_and_modifiers() {
        let query = Query::new(true, 100, Some(20), Some(Tag::new("tag")));
        assert_eq!(query.live_stream, true);
        assert_eq!(query.offset, 100);
        assert_eq!(query.limit, Some(20));
        assert_eq!(query.tag, Some(Tag::new("tag")));
        assert_eq!(query.interval(), Interval::new(100, u64::max_value()));

        let query = Query::current();
        assert_eq!(query.live_stream, false);
        assert_eq!(query.offset, 0);
        assert_eq!(query.limit, None);
        assert_eq!(query.tag, None);
        assert_eq!(query.interval(), Interval::new(0, u64::max_value()));

        let query = Query::live();
        assert_eq!(query.live_stream, true);
        assert_eq!(query.offset, 0);
        assert_eq!(query.limit, None);
        assert_eq!(query.tag, None);
        assert_eq!(query.interval(), Interval::new(0, u64::max_value()));

        let query = query.offset(100);
        assert_eq!(query.offset, 100);
        assert_eq!(query.interval(), Interval::new(100, u64::max_value()));

        let query = query.limit(20);
        assert_eq!(query.limit, Some(20));
        assert_eq!(query.interval(), Interval::new(100, 120));

        let query = query.by_tag(Tag::new("tag"));
        assert_eq!(query.tag, Some(Tag::new("tag")));
    }

    #[test]
    fn test_event_matching() {
        let query = Query::current();

        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1")]).with_id(1)));
        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").named("name")]).with_id(1)));
        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").with_version(1)]).with_id(1)));
        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").named("name").with_version(1)]).with_id(1)));

        let query = Query::current().by_tag(Tag::new("tag1"));

        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1")]).with_id(1)));
        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1").named("name")]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag2")]).with_id(1)));

        let query = Query::current().by_tag(Tag::new("tag1").named("name1"));

        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").named("name1")]).with_id(1)));
        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").named("name1").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1")]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag2")]).with_id(1)));

        let query = Query::current().by_tag(Tag::new("tag1").with_version(1));

        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1").with_version(2)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1").named("name1").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag2")]).with_id(1)));

        let query = Query::current().by_tag(Tag::new("tag1").named("name1").with_version(1));

        assert!(query.matches(&Event::new("data", vec![Tag::new("tag1").named("name1").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1").named("name1").with_version(2)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1").named("name2").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag1").with_version(1)]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec![Tag::new("tag2")]).with_id(1)));
    }
}
