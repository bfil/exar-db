use super::*;

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender};

/// Exar DB's events' publisher.
///
/// It manages event emitters and continuously published
/// new events to the subscriptions depending on the event emitters' query parameters.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::channel;
///
/// let publisher = Publisher::new(&PublisherConfig::default()).expect("Unable to create publisher");
/// let event     = Event::new("data", vec!["tag1", "tag2"]);
///
/// let (sender, receiver) = channel();
/// let event_emitter      = EventEmitter::new(sender, Query::live());
/// publisher.sender().register_event_emitter(event_emitter).expect("Unable to register event emitter");
/// publisher.sender().publish(event).expect("Unable to publish event");
///
/// let event_stream_message = receiver.recv().expect("Unable to receive event stream message");
///
/// drop(publisher);
/// # }
/// ```
#[derive(Debug)]
pub struct Publisher {
    executor: SingleThreadedExecutor<PublisherSender, PublisherThread>
}

impl Publisher {
    pub fn new(config: &PublisherConfig) -> DatabaseResult<Self> {
        Ok(Publisher {
            executor: SingleThreadedExecutor::new(
                |sender| PublisherSender::new(sender),
                |receiver| Ok(PublisherThread::new(receiver, config))
            )?
        })
    }

    pub fn sender(&self) -> &PublisherSender {
        self.executor.sender()
    }
}

#[derive(Clone, Debug)]
pub struct PublisherSender {
    sender: Sender<PublisherMessage>
}

impl PublisherSender {
    pub fn new(sender: Sender<PublisherMessage>) -> Self {
        PublisherSender { sender }
    }

    pub fn publish(&self, event: Event) -> DatabaseResult<()> {
        self.sender.send_message(PublisherMessage::PublishEvent(event))
    }

    pub fn register_event_emitter(&self, event_emitter: EventEmitter) -> DatabaseResult<()> {
        self.sender.send_message(PublisherMessage::RegisterEventEmitter(event_emitter))
    }
}

impl Stop for PublisherSender {
    fn stop(&self) -> DatabaseResult<()> {
        self.sender.send_message(PublisherMessage::Stop)
    }
}

#[derive(Debug)]
pub struct PublisherThread {
    receiver: Receiver<PublisherMessage>,
    buffer_size: usize,
    events_buffer: VecDeque<Event>,
    event_emitters: Vec<EventEmitter>
}

impl PublisherThread {
    fn new(receiver: Receiver<PublisherMessage>, config: &PublisherConfig) -> PublisherThread {
        PublisherThread {
            receiver,
            buffer_size: config.buffer_size,
            events_buffer: VecDeque::with_capacity(config.buffer_size),
            event_emitters: vec![]
        }
    }

    fn buffer_event(&mut self, event: &Event) {
        if self.events_buffer.len() == self.buffer_size {
            self.events_buffer.pop_front();
        }
        self.events_buffer.push_back(event.clone());
    }
}

impl Run for PublisherThread {
    fn run(mut self) -> Self {
        'main: loop {
            while let Ok(message) = self.receiver.recv() {
                match message {
                    PublisherMessage::RegisterEventEmitter(mut event_emitter) => {
                        let min_event_emitter_event_id = event_emitter.interval().start + 1;
                        match self.events_buffer.get(0) {
                            Some(first_buffered_event) if min_event_emitter_event_id < first_buffered_event.id => {
                                drop(event_emitter)
                            },
                            Some(first_buffered_event) => {
                                for event in self.events_buffer.iter().skip((min_event_emitter_event_id - first_buffered_event.id) as usize) {
                                    let _ = event_emitter.emit(event.clone());
                                }
                                if event_emitter.is_active() && event_emitter.is_live() {
                                    self.event_emitters.push(event_emitter)
                                }
                            },
                            None =>
                                if event_emitter.is_active() && event_emitter.is_live() {
                                    self.event_emitters.push(event_emitter)
                                }
                        }
                    },
                    PublisherMessage::PublishEvent(ref event) => {
                        self.buffer_event(event);
                        for event_emitter in self.event_emitters.iter_mut() {
                            let _ = event_emitter.emit(event.clone());
                        }
                        self.event_emitters.retain(|s| s.is_active())
                    },
                    PublisherMessage::Stop => break 'main
                }
            }
        };
        self
    }
}

#[derive(Clone, Debug)]
pub enum PublisherMessage {
    RegisterEventEmitter(EventEmitter),
    PublishEvent(Event),
    Stop
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::sync::mpsc::{channel, Sender};
    use std::thread;

    fn with_publisher_thread_running<F: FnOnce() + Sized>(thread: PublisherThread, sender: &Sender<PublisherMessage>, f: F) -> PublisherThread {
        let handle = thread::spawn(|| thread.run());
        f();
        assert!(sender.send(PublisherMessage::Stop).is_ok());
        handle.join().expect("Unable to join publisher thread")
    }

    #[test]
    fn test_publisher() {
        let publisher        = Publisher::new(&PublisherConfig::default()).expect("Unable to create publisher");
        let publisher_sender = publisher.sender();
        let first_event      = Event::new("data", vec!["tag1", "tag2"]).with_id(1);
        let second_event     = Event::new("data", vec!["tag1", "tag2"]).with_id(2);

        let (sender, receiver) = channel();
        let event_emitter      = EventEmitter::new(sender, Query::live());

        assert!(publisher_sender.publish(first_event.clone()).is_ok());

        publisher.sender().register_event_emitter(event_emitter).expect("Unable to register event emitter with the publisher");

        assert_event_received(&receiver, 1);

        assert!(publisher_sender.publish(second_event.clone()).is_ok());

        assert_event_received(&receiver, 2);
    }

    #[test]
    fn test_publisher_thread_events_buffering() {
        let publisher_config   = PublisherConfig { buffer_size: 1 };
        let (sender, receiver) = channel();
        let publisher_thread   = PublisherThread::new(receiver, &publisher_config);
        let first_event        = Event::new("data", vec!["tag1", "tag2"]).with_id(1);
        let second_event       = Event::new("data", vec!["tag1", "tag2"]).with_id(2);
        let third_event        = Event::new("data", vec!["tag1", "tag2"]).with_id(3);

        assert_eq!(publisher_thread.events_buffer, vec![]);
        assert_eq!(publisher_thread.event_emitters.len(), 0);

        let publisher_thread = with_publisher_thread_running(publisher_thread, &sender, || {
            assert!(sender.send(PublisherMessage::PublishEvent(first_event.clone())).is_ok());
        });
        assert_eq!(publisher_thread.events_buffer, vec![first_event]);

        let (event_emitter_sender, event_emitter_receiver) = channel();
        let event_emitter = EventEmitter::new(event_emitter_sender, Query::live());

        let publisher_thread = with_publisher_thread_running(publisher_thread, &sender, || {
            assert!(sender.send(PublisherMessage::RegisterEventEmitter(event_emitter)).is_ok());
            assert_event_received(&event_emitter_receiver, 1);
            assert!(sender.send(PublisherMessage::PublishEvent(second_event.clone())).is_ok());
            assert_event_received(&event_emitter_receiver, 2);
        });

        assert_eq!(publisher_thread.events_buffer, vec![second_event.clone()]);

        let (late_event_emitter_sender, late_event_emitter_receiver) = channel();
        let late_event_emitter = EventEmitter::new(late_event_emitter_sender, Query::live());

        let publisher_thread = with_publisher_thread_running(publisher_thread, &sender, || {
            assert!(sender.send(PublisherMessage::RegisterEventEmitter(late_event_emitter)).is_ok());
            assert_end_of_event_stream_received(&late_event_emitter_receiver);
        });

        assert_eq!(publisher_thread.event_emitters.len(), 1);

        let (another_event_emitter_sender, another_event_emitter_receiver) = channel();
        let another_event_emitter = EventEmitter::new(another_event_emitter_sender, Query::live().offset(1).limit(2));

        let publisher_thread = with_publisher_thread_running(publisher_thread, &sender, || {
            assert!(sender.send(PublisherMessage::RegisterEventEmitter(another_event_emitter)).is_ok());
            assert_event_received(&another_event_emitter_receiver, 2);
        });

        assert_eq!(publisher_thread.event_emitters.len(), 2);

        let publisher_thread = with_publisher_thread_running(publisher_thread, &sender, || {
            assert!(sender.send(PublisherMessage::PublishEvent(third_event.clone())).is_ok());
            assert_event_received(&event_emitter_receiver, 3);
            assert_event_received(&another_event_emitter_receiver, 3);
            assert_end_of_event_stream_received(&another_event_emitter_receiver);
        });

        assert_eq!(publisher_thread.events_buffer, vec![third_event]);
        assert_eq!(publisher_thread.event_emitters.len(), 1);
    }
}
