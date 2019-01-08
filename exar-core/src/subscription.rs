use super::*;

use std::sync::mpsc::Sender;

/// Exar DB's subscription.
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
/// let event = Event::new("data", vec!["tag1", "tag2"]);
///
/// let mut subscription = Subscription::new(sender, Query::current());
/// subscription.send(event).unwrap();
/// let event_stream_message = receiver.recv().unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Subscription {
    active: bool,
    /// The channel sender used to stream `EventStreamMessage`s back to the subscriber.
    pub event_stream_sender: Sender<EventStreamMessage>,
    /// The query associated to this subscription.
    pub query: Query
}

impl Subscription {
    /// Creates a new `Subscription` with the given channel sender and query.
    pub fn new(sender: Sender<EventStreamMessage>, query: Query) -> Subscription {
        Subscription {
            active: true,
            event_stream_sender: sender,
            query: query
        }
    }

    /// Sends an `Event` to the subscriber or returns a `DatabaseError` if a failure occurs.
    pub fn send(&mut self, event: Event) -> Result<(), DatabaseError> {
        let event_id = event.id;
        match self.event_stream_sender.send(EventStreamMessage::Event(event)) {
            Ok(_) => {
                self.query.update(event_id);
                if !self.is_active() || !self.query.is_active() {
                    self.active = false;
                    match self.event_stream_sender.send(EventStreamMessage::End) {
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

    /// Returns whether the subscription is still active.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Returns whether the subscription is interested in the given `Event`.
    pub fn matches_event(&self, event: &Event) -> bool {
        self.is_active() && self.query.is_active() && self.query.matches(event)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::sync::mpsc::channel;

    #[test]
    fn test_simple_subscription() {
        let (sender, receiver) = channel();
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1);

        let mut subscription = Subscription::new(sender, Query::current());

        assert!(subscription.send(event.clone()).is_ok());
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(event.clone())));
        assert_eq!(subscription.query.interval().start, 1);
        assert!(subscription.is_active());

        drop(receiver);

        assert_eq!(subscription.send(event.clone()), Err(DatabaseError::EventStreamError(EventStreamError::Closed)));
        assert_eq!(subscription.query.interval().start, 1);
        assert!(!subscription.is_active());
    }

    #[test]
    fn test_subscription_event_stream_end() {
        let (sender, receiver) = channel();
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1);

        let mut subscription = Subscription::new(sender, Query::current().limit(1));

        assert!(subscription.send(event.clone()).is_ok());
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(event.clone())));
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::End));
        assert_eq!(subscription.query.interval().start, 1);
        assert!(!subscription.is_active());

        drop(receiver);

        assert_eq!(subscription.send(event.clone()), Err(DatabaseError::EventStreamError(EventStreamError::Closed)));
        assert_eq!(subscription.query.interval().start, 1);
        assert!(!subscription.is_active());
    }
}
