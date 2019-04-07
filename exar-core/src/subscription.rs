use super::*;

use std::sync::mpsc::{Receiver, Sender, TryRecvError};

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
///
/// let event = Event::new("data", vec!["tag1", "tag2"]);
///
/// sender.send(EventStreamMessage::Event(event)).expect("Unable to send event stream message");
///
/// let subscription   = Subscription::new(sender, receiver);
/// let events: Vec<_> = subscription.event_stream().take(1).collect();
///
/// subscription.unsubscribe().expect("Unable to unsubscribe");
/// # }
/// ```
pub struct Subscription {
    event_stream: EventStream,
    unsubscribe_handle: UnsubscribeHandle
}

impl Subscription {
    /// Returns a new `Subscription` from the given `EventStreamMessage<Sender>` and `EventStreamMessage<Receiver>`.
    pub fn new(sender: Sender<EventStreamMessage>, receiver: Receiver<EventStreamMessage>) -> Self {
        let event_stream       = EventStream::new(receiver);
        let unsubscribe_handle = UnsubscribeHandle::new(sender);
        Subscription { event_stream, unsubscribe_handle }
    }

    /// Returns a reference to the underlying event stream
    pub fn event_stream(&self) -> &EventStream {
        &self.event_stream
    }

    /// Returns a reference to the unsubscribe handle
    pub fn unsubscribe_handle(&self) -> &UnsubscribeHandle {
        &self.unsubscribe_handle
    }

    /// Unsubscribes from the underlying event stream
    pub fn unsubscribe(&self) -> DatabaseResult<()> {
        self.unsubscribe_handle.unsubscribe()
    }

    /// Moves the subscription into a `(EventStream, UnsubscribeHandle)`
    pub fn into_event_stream_and_unsubscribe_handle(self) -> (EventStream, UnsubscribeHandle) {
        (self.event_stream, self.unsubscribe_handle)
    }
}

impl Iterator for Subscription {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        self.event_stream.recv().ok()
    }
}

impl Iterator for &Subscription {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        self.event_stream.recv().ok()
    }
}

/// Exar DB's event stream.
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
/// let mut event_stream   = EventStream::new(receiver);
///
/// let event                = Event::new("data", vec!["tag1", "tag2"]);
/// let event_stream_message = EventStreamMessage::Event(event);
///
/// sender.send(event_stream_message);
/// # }
/// ```
pub struct EventStream {
    receiver: Receiver<EventStreamMessage>
}

impl EventStream {
    /// Returns a new `EventStream` from the given `Receiver<EventStreamMessage>`.
    pub fn new(receiver: Receiver<EventStreamMessage>) -> EventStream {
        EventStream { receiver }
    }

    /// Attempts to wait for an event on this event stream,
    /// returning an `EventStreamError` if the corresponding channel has hung up.
    ///
    /// This function will always block the current thread if there is no data available
    /// and it's possible for more data to be sent.
    pub fn recv(&self) -> Result<Event, EventStreamError> {
        match self.receiver.recv() {
            Ok(EventStreamMessage::Event(event)) => Ok(event),
            Ok(EventStreamMessage::End) | Err(_) => Err(EventStreamError::Closed)
        }
    }

    /// Attempts to return a pending event on this event stream without blocking.
    ///
    /// This method will never block the caller in order to wait for the next event to become available.
    /// Instead, this will always return immediately with a possible option of pending data on the channel.
    pub fn try_recv(&self) -> Result<Event, EventStreamError> {
        match self.receiver.try_recv() {
            Ok(EventStreamMessage::Event(event)) => Ok(event),
            Ok(EventStreamMessage::End)          => Err(EventStreamError::Closed),
            Err(err)                             => match err {
                TryRecvError::Empty        => Err(EventStreamError::Empty),
                TryRecvError::Disconnected => Err(EventStreamError::Closed)
            }
        }
    }
}

impl Iterator for EventStream {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

impl Iterator for &EventStream {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

#[derive(Clone, Debug)]
pub struct UnsubscribeHandle {
    sender: Sender<EventStreamMessage>
}

impl UnsubscribeHandle {
    /// Creates a new `UnsubscribeHandle` with the given channel sender.
    pub fn new(sender: Sender<EventStreamMessage>) -> Self {
        UnsubscribeHandle { sender }
    }

    /// Unsubscribes from the underlying event stream
    pub fn unsubscribe(&self) -> DatabaseResult<()> {
        self.sender.send(EventStreamMessage::End)
                   .map_err(|_| DatabaseError::EventStreamError(EventStreamError::Closed))
    }
}

/// Exar DB's event emitter.
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
/// let event = Event::new("data", vec!["tag1", "tag2"]).with_id(1);
///
/// let mut event_emitter = EventEmitter::new(sender, Query::current());
/// event_emitter.emit(event).expect("Unable to emit event");
/// let event_stream_message = receiver.recv().unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct EventEmitter {
    active: bool,
    sender: Sender<EventStreamMessage>,
    query: Query,
    offset: u64,
    count: u64
}

impl EventEmitter {
    /// Creates a new `EventEmitter` with the given channel sender and query.
    pub fn new(sender: Sender<EventStreamMessage>, query: Query) -> Self {
        let offset = query.offset;
        EventEmitter { active: true, sender, query, offset, count: 0 }
    }

    /// Emits an `Event` to the subscription (if it should) and returns whether the event was emitted
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn emit(&mut self, event: Event) -> DatabaseResult<bool> {
        if self.should_emit(&event) {
            let event_id = event.id;
            match self.sender.send(EventStreamMessage::Event(event)) {
                Ok(_) => {
                    self.offset = event_id;
                    self.count += 1;
                    if !self.is_active() {
                        self.active = false
                    }
                    Ok(true)
                },
                Err(_) => {
                    self.active = false;
                    Err(DatabaseError::EventStreamError(EventStreamError::Closed))
                }
            }
        } else {
            Ok(false)
        }
    }

    fn should_emit(&self, event: &Event) -> bool {
        self.is_active() && event.id > self.offset && self.query.matches(event)
    }

    /// Returns whether the event emitter is still active.
    pub fn is_active(&self) -> bool {
        let query_within_limit = match self.query.limit {
            Some(limit) => self.count < limit,
            None        => true
        };
        self.active && query_within_limit
    }

    /// Returns whether the event emitter should emit live `Event`s.
    pub fn is_live(&self) -> bool {
        self.query.live_stream
    }

    /// Returns the current offsets interval of the event emitter.
    pub fn interval(&self) -> Interval<u64> {
        Interval::new(self.offset, self.query.interval().end)
    }
}

impl Drop for EventEmitter {
    fn drop(&mut self) {
        let _ = self.sender.send(EventStreamMessage::End);
    }
}

/// Exar DB's event stream message.
///
/// It can either be a message containing an event
/// or a message indicating the end of the event stream.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let event                = Event::new("data", vec!["tag1", "tag2"]);
/// let event_stream_message = EventStreamMessage::Event(event);
/// let event_stream_end     = EventStreamMessage::End;
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStreamMessage {
    /// The message containing an `Event`.
    Event(Event),
    /// The message indicating the end of the `EventStream`.
    End
}

/// A list specifying categories of event stream error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventStreamError {
    /// The event stream is empty.
    Empty,
    /// The event stream has been closed.
    Closed
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::sync::mpsc::channel;

    #[test]
    fn test_subscription() {
        let (sender, receiver) = channel();

        let event = Event::new("data", vec!["tag1", "tag2"]);

        sender.send(EventStreamMessage::Event(event.clone())).expect("Unable to send event stream message");

        let subscription   = Subscription::new(sender.clone(), receiver);
        let events: Vec<_> = subscription.event_stream().take(1).collect();

        assert_eq!(events, vec![event.clone()]);

        subscription.unsubscribe().expect("Unable to unsubscribe from the event stream");

        sender.send(EventStreamMessage::Event(event)).expect("Unable to send event stream message");

        let events: Vec<_> = subscription.event_stream().take(1).collect();

        assert_eq!(events, vec![]);
    }

    #[test]
    fn test_event_stream() {
        let event = Event::new("data", vec![""]);

        let (sender, receiver) = channel();

        let mut event_stream = EventStream::new(receiver);

        assert!(sender.send(EventStreamMessage::Event(event.clone())).is_ok());

        assert_eq!(event_stream.next(), Some(event));
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Empty));

        drop(sender);

        assert_eq!(event_stream.next(), None);
        assert_eq!(event_stream.try_recv(), Err(EventStreamError::Closed));
        assert_eq!(event_stream.recv(), Err(EventStreamError::Closed));
    }

    #[test]
    fn test_event_emitter() {
        let (sender, receiver) = channel();
        let first_event        = Event::new("data", vec!["tag1", "tag2"]).with_id(1);
        let second_event       = Event::new("data", vec!["tag1", "tag2"]).with_id(2);
        let mut event_emitter  = EventEmitter::new(sender, Query::current());

        assert_eq!(event_emitter.emit(first_event.clone()), Ok(true));
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(first_event)));
        assert_eq!(event_emitter.interval().start, 1);
        assert!(event_emitter.is_active());

        drop(receiver);

        assert_eq!(event_emitter.emit(second_event), Err(DatabaseError::EventStreamError(EventStreamError::Closed)));
        assert_eq!(event_emitter.interval().start, 1);
        assert!(!event_emitter.is_active());
    }

    #[test]
    fn test_event_emitter_end_of_event_stream() {
        let (sender, receiver) = channel();
        let first_event        = Event::new("data", vec!["tag1", "tag2"]).with_id(1);
        let second_event       = Event::new("data", vec!["tag1", "tag2"]).with_id(2);
        let mut event_emitter  = EventEmitter::new(sender, Query::current().limit(1));

        assert_eq!(event_emitter.emit(first_event.clone()), Ok(true));
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(first_event.clone())));
        assert_eq!(event_emitter.interval().start, 1);
        assert!(!event_emitter.is_active());
        assert_eq!(event_emitter.emit(second_event), Ok(false));

        drop(event_emitter);
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::End));
    }

    #[test]
    fn test_event_emitter_receiver_drop() {
        let (sender, receiver) = channel();
        let first_event        = Event::new("data", vec!["tag1", "tag2"]).with_id(1);
        let second_event       = Event::new("data", vec!["tag1", "tag2"]).with_id(2);
        let mut event_emitter  = EventEmitter::new(sender, Query::current());

        assert_eq!(event_emitter.emit(first_event.clone()), Ok(true));
        assert_eq!(receiver.recv(), Ok(EventStreamMessage::Event(first_event)));
        assert_eq!(event_emitter.interval().start, 1);
        assert!(event_emitter.is_active());

        drop(receiver);

        assert_eq!(event_emitter.emit(second_event), Err(DatabaseError::EventStreamError(EventStreamError::Closed)));
        assert_eq!(event_emitter.interval().start, 1);
        assert!(!event_emitter.is_active());
    }
}
