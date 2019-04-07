use super::*;

use std::collections::VecDeque;
use std::sync::mpsc::{channel, Receiver, Sender};

/// Exar DB's events' publisher.
///
/// It manages event emitters and continuously published
/// new events depending on the event emitters' query parameters.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// # }
/// ```
#[derive(Debug)]
pub struct Publisher {
    threads: ControllableThreads<PublisherSender, PublisherThread>
}

impl Publisher {
    pub fn new(config: &PublisherConfig) -> DatabaseResult<Self> {
        let (sender, receiver) = channel();
        let threads = ControllableThreads::new(PublisherSender::new(sender), vec![PublisherThread::new(receiver, config)]);
        Ok(Publisher { threads })
    }

    pub fn sender(&self) -> &PublisherSender {
        self.threads.sender()
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
struct PublisherThread {
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

impl Run<Self> for PublisherThread {
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