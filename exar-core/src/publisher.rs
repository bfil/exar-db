use super::*;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

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
    config: PublisherConfig,
    action_sender: Arc<Mutex<Option<Sender<PublisherAction>>>>
}

impl Publisher {
    pub fn new(config: &PublisherConfig) -> Result<Publisher, DatabaseError> {
        let mut publisher = Publisher {
            config: config.clone(),
            action_sender: Arc::new(Mutex::new(None))
        };
        publisher.start()?;
        Ok(publisher)
    }

    pub fn publish(&mut self, event: Event) -> Result<(), DatabaseError> {
        self.send_action(PublisherAction::PublishEvent(event))
    }

    pub fn handle_subscription(&mut self, subscription: Subscription) -> Result<(), DatabaseError> {
        self.send_action(PublisherAction::HandleSubscription(subscription))
    }

    fn send_action(&self, action: PublisherAction) -> Result<(), DatabaseError> {
        match *self.action_sender.lock().unwrap() {
            Some(ref action_sender) => match action_sender.send(action) {
                Ok(()) => Ok(()),
                Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
            },
            None => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    pub fn start(&mut self) -> Result<(), DatabaseError> {
        let mut action_sender = self.action_sender.lock().unwrap();
        match *action_sender {
            Some(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed)),
            None => {
                let (sender, receiver) = channel();
                *action_sender = Some(sender);
                let config = self.config.clone();
                thread::spawn(|| {
                    PublisherThread::new(receiver, config).run();
                });
                Ok(())
            }
        }
    }

    pub fn stop(&mut self) -> Result<(), DatabaseError> {
        self.send_action(PublisherAction::Stop).and_then(|result| {
            *self.action_sender.lock().unwrap() = None;
            Ok(result)
        })
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let _ =  self.stop();
    }
}

#[derive(Debug)]
struct PublisherThread {
    action_receiver: Receiver<PublisherAction>,
    buffer_size: usize,
    events_buffer: VecDeque<Event>,
    subscriptions: Vec<Subscription>
}

impl PublisherThread {
    fn new(receiver: Receiver<PublisherAction>, config: PublisherConfig) -> PublisherThread {
        PublisherThread {
            action_receiver: receiver,
            buffer_size: config.buffer_size,
            events_buffer: VecDeque::with_capacity(config.buffer_size),
            subscriptions: vec![]
        }
    }

    fn run(mut self) {
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