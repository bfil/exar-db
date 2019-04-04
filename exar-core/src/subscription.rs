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
    sender: Sender<EventStreamMessage>,
    query: Query,
    offset: u64,
    count: u64
}

impl Subscription {
    /// Creates a new `Subscription` with the given channel sender and query.
    pub fn new(sender: Sender<EventStreamMessage>, query: Query) -> Self {
        let offset = query.offset;
        Subscription { active: true, sender, query, offset, count: 0 }
    }

    /// Sends an `Event` to the subscriber or returns a `DatabaseError` if a failure occurs.
    pub fn send(&mut self, event: Event) -> DatabaseResult<()> {
        let event_id = event.id;
        match self.sender.send(EventStreamMessage::Event(event)) {
            Ok(_) => {
                self.offset = event_id;
                self.count += 1;
                if !self.is_active() {
                    Ok(self.active = false)
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
        let query_within_limit = match self.query.limit {
            Some(limit) => self.count < limit,
            None        => true
        };
        self.active && query_within_limit
    }

    /// Returns whether the subscription is interested in live `Event`s.
    pub fn is_live(&self) -> bool {
        self.query.live_stream
    }

    /// Returns whether the subscription is interested in the given `Event`.
    pub fn matches_event(&self, event: &Event) -> bool {
        self.is_active() && event.id > self.offset && self.query.matches(event)
    }

    /// Returns the current offsets interval of the subscription.
    pub fn interval(&self) -> Interval<u64> {
        Interval::new(self.offset, self.query.interval().end)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self.sender.send(EventStreamMessage::End);
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionHandle {
    sender: Sender<EventStreamMessage>
}

impl SubscriptionHandle {
    /// Creates a new `SubscriptionHandle` with the given channel sender.
    pub fn new(sender: Sender<EventStreamMessage>) -> Self {
        SubscriptionHandle { sender }
    }

    pub fn unsubscribe(&self) -> DatabaseResult<()> {
        self.sender.send(EventStreamMessage::End)
                   .map_err(|_| DatabaseError::EventStreamError(EventStreamError::Closed))
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        let _ = self.unsubscribe();
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
        assert_eq!(subscription.interval().start, 1);
        assert!(subscription.is_active());

        drop(receiver);

        assert_eq!(subscription.send(event.clone()), Err(DatabaseError::EventStreamError(EventStreamError::Closed)));
        assert_eq!(subscription.interval().start, 1);
        assert!(!subscription.is_active());
    }

    #[test]
    fn test_subscription_event_stream_end() {
        let (sender, receiver) = channel();
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1);

        let mut subscription = Subscription::new(sender, Query::current().limit(1));

        assert!(subscription.send(event.clone()).is_ok());
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(event.clone())));
        assert_eq!(subscription.interval().start, 1);
        assert!(!subscription.is_active());

        drop(subscription);
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::End));
    }

    #[test]
    fn test_subscription_event_stream_receiver_drop() {
        let (sender, receiver) = channel();
        let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1);

        let mut subscription = Subscription::new(sender, Query::current().limit(1));

        assert!(subscription.send(event.clone()).is_ok());
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(event.clone())));
        assert_eq!(subscription.interval().start, 1);
        assert!(!subscription.is_active());

        drop(receiver);

        assert_eq!(subscription.send(event.clone()), Err(DatabaseError::EventStreamError(EventStreamError::Closed)));
        assert_eq!(subscription.interval().start, 1);
        assert!(!subscription.is_active());
    }
}
