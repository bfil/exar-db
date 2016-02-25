use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    pub offset: usize,
    pub limit: Option<usize>,
    pub tag: Option<String>,
    pub live: bool,
    pub position: usize,
    pub count: usize
}

impl Query {
    pub fn new(live: bool, offset: usize, limit: Option<usize>, tag: Option<String>) -> Query {
        Query {
            offset: offset,
            limit: limit,
            tag: tag,
            live: live,
            position: offset,
            count: 0
        }
    }

    pub fn current() -> Query {
        Query::new(false, 0, None, None)
    }

    pub fn live() -> Query {
        Query::new(true, 0, None, None)
    }

    pub fn offset(mut self, offset: usize) -> Query {
        self.offset = offset;
        self.position = offset;
        self
    }

    pub fn limit(mut self, limit: usize) -> Query {
        self.limit = Some(limit);
        self
    }

    pub fn by_tag(mut self, tag: &str) -> Query {
        self.tag = Some(tag.to_owned());
        self
    }

    pub fn matches(&self, event: &Event) -> bool {
        match self.tag {
            Some(ref tag) => self.position < event.id && event.tags.contains(tag),
            None => self.position < event.id
        }
    }

    pub fn is_active(&self) -> bool {
        match self.limit {
            Some(limit) => self.count < limit,
            None => true
        }
    }

    pub fn is_live(&self) -> bool {
        self.live
    }

    pub fn update(&mut self, event_id: usize) {
        self.position = event_id;
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_constructors_and_modifiers() {
        let query = Query::new(true, 100, Some(20), Some("tag".to_owned()));
        assert_eq!(query.live, true);
        assert_eq!(query.offset, 100);
        assert_eq!(query.limit, Some(20));
        assert_eq!(query.tag, Some("tag".to_owned()));
        assert_eq!(query.is_live(), true);

        let query = Query::current();
        assert_eq!(query.live, false);
        assert_eq!(query.offset, 0);
        assert_eq!(query.limit, None);
        assert_eq!(query.tag, None);
        assert_eq!(query.is_live(), false);

        let query = Query::live();
        assert_eq!(query.live, true);
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
        let mut query = Query::current();

        assert!(query.matches(&Event::new("data", vec!["tag1"]).with_id(1)));

        query.update(1);

        assert!(!query.matches(&Event::new("data", vec!["tag1"]).with_id(1)));

        let mut query = Query::current().by_tag("tag1");

        assert!(query.matches(&Event::new("data", vec!["tag1"]).with_id(1)));
        assert!(!query.matches(&Event::new("data", vec!["tag2"]).with_id(1)));

        query.update(1);

        assert!(!query.matches(&Event::new("data", vec!["tag1"]).with_id(1)));
    }

    #[test]
    fn test_is_active() {
        let query = Query::current();

        assert!(query.is_active());

        let mut query = query.limit(3);

        assert!(query.is_active());

        query.update(1);
        query.update(2);
        query.update(3);

        assert_eq!(query.position, 3);
        assert_eq!(query.count, 3);
        assert!(!query.is_active());
    }
}
