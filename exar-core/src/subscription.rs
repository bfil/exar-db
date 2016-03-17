use super::*;

use std::sync::mpsc::Sender;

#[derive(Clone, Debug)]
pub struct Subscription {
    active: bool,
    pub send: Sender<EventStreamMessage>,
    pub query: Query
}

impl Subscription {
    pub fn new(send: Sender<EventStreamMessage>, query: Query) -> Subscription {
        Subscription {
            active: true,
            send: send,
            query: query
        }
    }

    pub fn send(&mut self, event: Event) -> Result<(), DatabaseError> {
        let event_id = event.id;
        match self.send.send(EventStreamMessage::Event(event)) {
            Ok(_) => {
                self.query.update(event_id);
                if !self.is_active() || !self.query.is_active() {
                    self.active = false;
                    match self.send.send(EventStreamMessage::End) {
                        Ok(_) => Ok(()),
                        Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
                    }
                } else {
                    Ok(())
                }
            },
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

        assert!(subscription.send(event.clone()).is_ok());
        assert_eq!(recv.recv(), Ok(EventStreamMessage::Event(event.clone())));
        assert_eq!(subscription.query.position, 1);
        assert!(subscription.is_active());

        drop(recv);

        assert!(subscription.send(event.clone()).is_err());
        assert_eq!(subscription.query.position, 1);
        assert!(!subscription.is_active());
    }
}
