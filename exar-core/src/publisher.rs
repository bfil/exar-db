use super::*;

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
#[derive(Debug)]
pub struct Publisher {
    action_sender: Sender<PublisherAction>
}

impl Publisher {
    pub fn new() -> Publisher {
        let (sender, receiver) = channel();
        PublisherThread::new(receiver).run();
        Publisher {
            action_sender: sender
        }
    }

    pub fn publish(&self, event: Event) -> Result<(), DatabaseError> {
        match self.action_sender.send(PublisherAction::PublishEvent(event)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    /// Clones the channel sender responsible to send `PublisherAction`s to the publisher.
    pub fn clone_action_sender(&self) -> Sender<PublisherAction> {
        self.action_sender.clone()
    }

    fn stop(&self) -> Result<(), DatabaseError> {
        match self.action_sender.send(PublisherAction::Stop) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
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
    subscriptions: Vec<Subscription>
}

impl PublisherThread {
    fn new(receiver: Receiver<PublisherAction>) -> PublisherThread {
        PublisherThread {
            action_receiver: receiver,
            subscriptions: vec![]
        }
    }

    fn run(mut self) -> JoinHandle<Self> {
        thread::spawn(move || {
            'main: loop {
                while let Ok(action) = self.action_receiver.recv() {
                    match action {
                        PublisherAction::AddSubscription(subscription) => {
                            self.subscriptions.push(subscription);
                        },
                        PublisherAction::PublishEvent(ref event) => {
                            for subscription in self.subscriptions.iter_mut().filter(|s| s.matches_event(event)) {
                                // TODO: if sending fails it will skip and publish later events, should probable fail and remove subscription
                                let _ = subscription.send(event.clone());
                            }
                        },
                        PublisherAction::Stop => break 'main
                    }
                }
            };
            self.subscriptions.truncate(0);
            self
        })
    }
}

#[derive(Clone, Debug)]
pub enum PublisherAction {
    AddSubscription(Subscription),
    PublishEvent(Event),
    Stop
}