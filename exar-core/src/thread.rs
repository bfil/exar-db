use super::*;

use rand;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;
use std::thread;
use std::thread::JoinHandle;

#[derive(Debug)]
pub struct ControllableThreads<S: Stop, T: Run<T> + Send + 'static> {
    sender: S,
    threads: Vec<T>,
    join_handles: Vec<JoinHandle<T>>
}

impl<S: Stop, T: Run<T> + Send + 'static> ControllableThreads<S, T> {
    pub fn new(sender: S, threads: Vec<T>) -> DatabaseResult<Self> {
        let mut threads = ControllableThreads { sender, threads, join_handles: vec![] };
        match threads.start() {
            Ok(_)    => Ok(threads),
            Err(err) => {
                            error!("Unable to start controllable threads: {}", err);
                            Err(DatabaseError::InternalError)
                        }
        }
    }

    pub fn sender(&self) -> &S {
        &self.sender
    }

    fn start(&mut self) -> DatabaseResult<()> {
        if self.threads.is_empty() {
            Err(DatabaseError::InternalError)
        } else {
            for thread in self.threads.drain(..) {
                let join_handle = thread::spawn(|| thread.run());
                self.join_handles.push(join_handle)
            }
            Ok(())
        }
    }

    fn stop(&mut self) -> DatabaseResult<()> {
        if self.join_handles.is_empty() {
            Err(DatabaseError::InternalError)
        } else {
            self.sender.stop()?;
            for handle in self.join_handles.drain(..) {
                let thread = handle.join().map_err(|_| DatabaseError::InternalError)?;
                self.threads.push(thread);
            }
            Ok(())
        }
    }
}

impl<S: Stop, T: Run<T> + Send + 'static> Drop for ControllableThreads<S, T> {
    fn drop(&mut self) {
        match self.stop() {
            Ok(_)    => (),
            Err(err) => error!("Unable to stop controllable threads: {}", err)
        };
    }
}

pub trait Run<T> {
    fn run(self) -> T;
}

pub trait Stop {
    fn stop(&self) -> DatabaseResult<()>;
}

pub trait SendMessage<T> {
    fn send_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T> SendMessage<T> for Sender<T> {
    fn send_message(&self, message: T) -> DatabaseResult<()> {
        self.send(message).map_err(|_| DatabaseError::InternalError)
    }
}

#[derive(Clone, Debug)]
pub struct MultiSender<T> {
    senders: Vec<Sender<T>>,
    routing_strategy: Arc<Mutex<RoutingStrategy>>
}

impl<T> MultiSender<T> {
    pub fn new(senders: Vec<Sender<T>>, routing_strategy: RoutingStrategy) -> Self {
        MultiSender { senders, routing_strategy: Arc::new(Mutex::new(routing_strategy)) }
    }

    fn update_routing_strategy(&self, routing_strategy: RoutingStrategy) {
        *self.routing_strategy.lock().unwrap() = routing_strategy;
    }
}

pub trait BroadcastMessage<T> {
    fn broadcast_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T: Clone> BroadcastMessage<T> for MultiSender<T> {
    fn broadcast_message(&self, message: T) -> DatabaseResult<()> {
        for sender in &*self.senders {
            sender.send_message(message.clone())?;
        }
        Ok(())
    }
}

pub trait RouteMessage<T> {
    fn route_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T: Clone> RouteMessage<T> for MultiSender<T> {
    fn route_message(&self, message: T) -> DatabaseResult<()> {
        let routing_strategy = self.routing_strategy.lock().unwrap().clone();
        (match routing_strategy {
            RoutingStrategy::Random => match rand::thread_rng().choose(&self.senders) {
                Some(sender) => {
                    sender.send_message(message)?;
                    Ok(())
                },
                None => Err(DatabaseError::SubscriptionError)
            },
            RoutingStrategy::RoundRobin(index) => {
                match self.senders.get(index) {
                    Some(sender) => {
                        sender.send_message(message)?;
                        let new_index = if index + 1 < self.senders.len() { index + 1 } else { 0 };
                        self.update_routing_strategy(RoutingStrategy::RoundRobin(new_index));
                        Ok(())
                    },
                    None => Err(DatabaseError::SubscriptionError)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::sync::mpsc::{channel, Sender, Receiver};

    struct TestThread {
        receiver: Receiver<String>,
        messages: Vec<String>
    }

    impl Run<TestThread> for TestThread {
        fn run(mut self) -> Self {
            'main: loop {
                while let Ok(message) = self.receiver.recv() {
                    match message.as_ref() {
                        "stop"  => break 'main,
                        message => self.messages.push(message.to_owned())
                    }
                }
            };
            self
        }
    }

    impl Stop for Sender<String> {
        fn stop(&self) -> DatabaseResult<()> {
            self.send("stop".to_owned()).map_err(|_| DatabaseError::InternalError)
        }
    }

    #[test]
    fn test_controllable_threads() {
        let (sender, receiver)       = channel();
        let test_thread              = TestThread { receiver, messages: vec![] };
        let mut controllable_threads = ControllableThreads::new(sender, vec![test_thread]).expect("Unable to create controllable threads");

        assert_eq!(controllable_threads.threads.len(), 0);
        assert_eq!(controllable_threads.join_handles.len(), 1);

        assert!(controllable_threads.sender().send("a".to_owned()).is_ok());
        assert!(controllable_threads.sender().send("b".to_owned()).is_ok());

        assert!(controllable_threads.stop().is_ok());

        assert_eq!(controllable_threads.threads.len(), 1);
        assert_eq!(controllable_threads.join_handles.len(), 0);

        let test_thread = controllable_threads.threads.get(0).expect("Unable to get test thread");
        assert_eq!(test_thread.messages, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn test_controllable_threads_constructor_failure() {
        let (sender, _)                   = channel();
        let test_threads: Vec<TestThread> = vec![];
        assert!(ControllableThreads::new(sender, test_threads).is_err());
    }

    #[test]
    fn test_send_message() {
        let (sender, receiver) = channel();
        assert!(sender.send_message("test".to_owned()).is_ok());
        assert_eq!(receiver.recv(), Ok("test".to_owned()));
    }

    #[test]
    fn test_route_message_round_robin() {
        let (sender1, receiver1) = channel();
        let (sender2, receiver2) = channel();

        let multi_sender = MultiSender::new(vec![sender1, sender2], RoutingStrategy::RoundRobin(0));

        assert!(multi_sender.route_message("a".to_owned()).is_ok());
        assert!(multi_sender.route_message("b".to_owned()).is_ok());
        assert!(multi_sender.route_message("c".to_owned()).is_ok());
        assert!(multi_sender.route_message("d".to_owned()).is_ok());

        drop(multi_sender);

        let receiver1_messages: Vec<String> = receiver1.iter().collect();
        let receiver2_messages: Vec<String> = receiver2.iter().collect();

        assert_eq!(receiver1_messages, vec!["a".to_owned(), "c".to_owned()]);
        assert_eq!(receiver2_messages, vec!["b".to_owned(), "d".to_owned()]);
    }

    #[test]
    fn test_route_message_random() {
        let (sender1, receiver1) = channel();
        let (sender2, receiver2) = channel();

        let multi_sender = MultiSender::new(vec![sender1, sender2], RoutingStrategy::Random);

        assert!(multi_sender.route_message("a".to_owned()).is_ok());
        assert!(multi_sender.route_message("b".to_owned()).is_ok());
        assert!(multi_sender.route_message("c".to_owned()).is_ok());
        assert!(multi_sender.route_message("d".to_owned()).is_ok());

        drop(multi_sender);

        let receiver1_messages: Vec<String> = receiver1.iter().collect();
        let receiver2_messages: Vec<String> = receiver2.iter().collect();

        let mut all_messages = [&receiver1_messages[..], &receiver2_messages[..]].concat();
        all_messages.sort();

        assert_eq!(all_messages, vec!["a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()]);
    }

    #[test]
    fn test_broadcast_message() {
        let (sender1, receiver1) = channel();
        let (sender2, receiver2) = channel();

        let multi_sender = MultiSender::new(vec![sender1, sender2], RoutingStrategy::RoundRobin(0));

        assert!(multi_sender.broadcast_message("a".to_owned()).is_ok());
        assert!(multi_sender.broadcast_message("b".to_owned()).is_ok());

        drop(multi_sender);

        let receiver1_messages: Vec<String> = receiver1.iter().collect();
        let receiver2_messages: Vec<String> = receiver2.iter().collect();

        assert_eq!(receiver1_messages, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(receiver2_messages, vec!["a".to_owned(), "b".to_owned()]);
    }
}
