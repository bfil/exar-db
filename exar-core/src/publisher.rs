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
    thread: ControllableThread<PublisherSender, PublisherThread>
}

impl Publisher {
    pub fn new(config: &PublisherConfig) -> Self {
        let (sender, receiver) = channel();
        Publisher {
            thread: ControllableThread::new(PublisherSender::new(sender), PublisherThread::new(receiver, config))
        }
    }

    pub fn sender(&self) -> &PublisherSender {
        self.thread.sender()
    }

    pub fn start(&mut self) -> DatabaseResult<()> {
        self.thread.start()
    }

    pub fn stop(&mut self) -> DatabaseResult<()> {
        self.thread.stop()
    }
}

#[derive(Clone, Debug)]
pub struct PublisherSender {
    sender: Sender<PublisherAction>
}

impl PublisherSender {
    pub fn new(sender: Sender<PublisherAction>) -> Self {
        PublisherSender { sender }
    }

    pub fn publish(&mut self, event: Event) -> DatabaseResult<()> {
        self.sender.send_action(PublisherAction::PublishEvent(event))
    }

    pub fn handle_subscription(&mut self, subscription: Subscription) -> DatabaseResult<()> {
        self.sender.send_action(PublisherAction::HandleSubscription(subscription))
    }
}

impl Stop for PublisherSender {
    fn stop(&mut self) -> DatabaseResult<()> {
        self.sender.send_action(PublisherAction::Stop)
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
    fn new(receiver: Receiver<PublisherAction>, config: &PublisherConfig) -> PublisherThread {
        PublisherThread {
            action_receiver: receiver,
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
        self
    }
}

#[derive(Clone, Debug)]
pub enum PublisherAction {
    HandleSubscription(Subscription),
    PublishEvent(Event),
    Stop
}