use super::*;

use std::sync::mpsc::Sender;

#[derive(Clone, Debug)]
pub struct Subscription {
    active: bool,
    pub send: Sender<Event>,
    pub query: Query
}

impl Subscription {
    pub fn new(send: Sender<Event>, query: Query) -> Subscription {
        Subscription {
            active: true,
            send: send,
            query: query
        }
    }

    pub fn emit(&mut self, event: Event) -> Result<(), DatabaseError> {
        let event_id = event.id;
        match self.send.send(event) {
            Ok(_) => Ok(self.query.update(event_id)),
            Err(_) => {
                self.active = false;
                Err(DatabaseError::EventStreamError(EventStreamError::Closed))
            }
        }
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn matches_event(&self, event: &Event) -> bool {
        self.is_active() && self.query.is_active() && self.query.matches(event)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::sync::mpsc::channel;

    #[test]
    fn test_subscription() {
        let (send, recv) = channel();
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1);

        let mut subscription = Subscription::new(send, Query::current());

        assert!(subscription.emit(event.clone()).is_ok());
        assert_eq!(recv.recv(), Ok(event.clone()));
        assert_eq!(subscription.query.position, 1);
        assert!(subscription.is_active());

        drop(recv);

        assert!(subscription.emit(event.clone()).is_err());
        assert_eq!(subscription.query.position, 1);
        assert!(!subscription.is_active());
    }
}
