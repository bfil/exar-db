use super::*;

use std::collections::VecDeque;
use std::sync::mpsc::{channel, Receiver, Sender};

/// Exar DB's events' publisher.
///
/// It manages event stream subscriptions and continuously published
/// new events depending on the subscriptions query parameters.
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

    pub fn handle_subscription(&self, subscription: Subscription) -> DatabaseResult<()> {
        self.sender.send_message(PublisherMessage::HandleSubscription(subscription))
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
    subscriptions: Vec<Subscription>
}

impl PublisherThread {
    fn new(receiver: Receiver<PublisherMessage>, config: &PublisherConfig) -> PublisherThread {
        PublisherThread {
            receiver,
            buffer_size: config.buffer_size,
            events_buffer: VecDeque::with_capacity(config.buffer_size),
            subscriptions: vec![]
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
                    PublisherMessage::HandleSubscription(mut subscription) => {
                        let min_subscription_event_id = subscription.interval().start + 1;
                        match self.events_buffer.get(0) {
                            Some(first_buffered_event) if min_subscription_event_id < first_buffered_event.id => {
                                drop(subscription)
                            },
                            Some(first_buffered_event) => {
                                for event in self.events_buffer.iter().skip((min_subscription_event_id - first_buffered_event.id) as usize) {
                                    if subscription.matches_event(event) {
                                        let _ = subscription.send(event.clone());
                                    }
                                }
                                if subscription.is_active() && subscription.is_live() {
                                    self.subscriptions.push(subscription)
                                }
                            },
                            None =>
                                if subscription.is_active() && subscription.is_live() {
                                    self.subscriptions.push(subscription)
                                }
                        }
                    },
                    PublisherMessage::PublishEvent(ref event) => {
                        self.buffer_event(event);
                        for subscription in self.subscriptions.iter_mut().filter(|s| s.matches_event(event)) {
                            let _ = subscription.send(event.clone());
                        }
                        self.subscriptions.retain(|s| s.is_active())
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
    HandleSubscription(Subscription),
    PublishEvent(Event),
    Stop
}