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
