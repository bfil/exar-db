use super::*;

use std::collections::VecDeque;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;

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
#[derive(Clone, Debug)]
pub struct Publisher {
    action_sender: Sender<PublisherAction>
}

impl Publisher {
    pub fn new(config: &PublisherConfig) -> Publisher {
        let (sender, receiver) = channel();
        PublisherThread::new(receiver, config).run();
        Publisher {
            action_sender: sender
        }
    }

    pub fn publish(&self, event: Event) -> Result<(), DatabaseError> {
        self.send_action(PublisherAction::PublishEvent(event))
    }

    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        self.send_action(PublisherAction::HandleSubscription(subscription))
    }

    fn send_action(&self, action: PublisherAction) -> Result<(), DatabaseError> {
        match self.action_sender.send(action) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    fn stop(&self) -> Result<(), DatabaseError> {
        self.send_action(PublisherAction::Stop)
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        match self.stop() {
            Ok(_) => (),
            Err(err) => error!("Unable to stop publisher thread: {}", err)
        }
    }
}

struct PublisherThread {
    action_receiver: Receiver<PublisherAction>,
    buffer_size: usize,
    events_buffer: VecDeque<Event>,
    subscriptions: Vec<Subscription>
}

impl PublisherThread {
    fn new(receiver: Receiver<PublisherAction>, config: &PublisherConfig) -> PublisherThread {
        PublisherThread {
            action_receiver: receiver,
            buffer_size: config.buffer_size,
            events_buffer: VecDeque::with_capacity(config.buffer_size),
            subscriptions: vec![]
        }
    }

    fn run(mut self) -> JoinHandle<Self> {
        thread::spawn(move || {
            'main: loop {
                while let Ok(action) = self.action_receiver.recv() {
                    match action {
                        PublisherAction::HandleSubscription(mut subscription) => {
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
                                None => self.subscriptions.push(subscription)
                            }
                        },
                        PublisherAction::PublishEvent(ref event) => {
                            self.buffer_event(event);
                            for subscription in self.subscriptions.iter_mut().filter(|s| s.matches_event(event)) {
                                let _ = subscription.send(event.clone());
                            }
                            self.subscriptions.retain(|s| s.is_active())
                        },
                        PublisherAction::Stop => break 'main
                    }
                }
            };
            self.subscriptions.truncate(0);
            self
        })
    }

    fn buffer_event(&mut self, event: &Event) {
        if self.events_buffer.len() == self.buffer_size {
            self.events_buffer.pop_front();
        }
        self.events_buffer.push_back(event.clone());
    }
}

#[derive(Clone, Debug)]
pub enum PublisherAction {
    HandleSubscription(Subscription),
    PublishEvent(Event),
    Stop
}