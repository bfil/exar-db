use super::*;

use rand;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;

/// A trait for sending messages.
pub trait SendMessage<T> {
    fn send_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T> SendMessage<T> for Sender<T> {
    fn send_message(&self, message: T) -> DatabaseResult<()> {
        self.send(message).map_err(|_| DatabaseError::InternalError)
    }
}

/// A message router.
///
/// It can be used to route messages to multiple channel senders.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::channel;
///
/// let (sender1, receiver1) = channel();
/// let (sender2, receiver2) = channel();
///
/// let router = Router::new(vec![sender1, sender2], RoutingStrategy::RoundRobin(0));
///
/// router.route_message("a".to_owned()).expect("Unable to route message");
/// router.route_message("b".to_owned()).expect("Unable to route message");
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Router<T> {
    senders: Vec<Sender<T>>,
    routing_strategy: Arc<Mutex<RoutingStrategy>>
}

impl<T> Router<T> {
    /// Creates a new instance of a router with the given `Sender`s and `RoutingStrategy`.
    pub fn new(senders: Vec<Sender<T>>, routing_strategy: RoutingStrategy) -> Self {
        Router { senders, routing_strategy: Arc::new(Mutex::new(routing_strategy)) }
    }

    fn update_routing_strategy(&self, routing_strategy: RoutingStrategy) {
        *self.routing_strategy.lock().unwrap() = routing_strategy;
    }
}

/// A trait for broadcasting messages.
pub trait BroadcastMessage<T> {
    fn broadcast_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T: Clone> BroadcastMessage<T> for Router<T> {
    fn broadcast_message(&self, message: T) -> DatabaseResult<()> {
        for sender in &*self.senders {
            sender.send_message(message.clone())?;
        }
        Ok(())
    }
}

/// A trait for routing messages.
pub trait RouteMessage<T> {
    fn route_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T: Clone> RouteMessage<T> for Router<T> {
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

    use std::sync::mpsc::channel;

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

        let router = Router::new(vec![sender1, sender2], RoutingStrategy::RoundRobin(0));

        assert!(router.route_message("a".to_owned()).is_ok());
        assert!(router.route_message("b".to_owned()).is_ok());
        assert!(router.route_message("c".to_owned()).is_ok());
        assert!(router.route_message("d".to_owned()).is_ok());

        drop(router);

        let receiver1_messages: Vec<String> = receiver1.iter().collect();
        let receiver2_messages: Vec<String> = receiver2.iter().collect();

        assert_eq!(receiver1_messages, vec!["a".to_owned(), "c".to_owned()]);
        assert_eq!(receiver2_messages, vec!["b".to_owned(), "d".to_owned()]);
    }

    #[test]
    fn test_route_message_random() {
        let (sender1, receiver1) = channel();
        let (sender2, receiver2) = channel();

        let router = Router::new(vec![sender1, sender2], RoutingStrategy::Random);

        assert!(router.route_message("a".to_owned()).is_ok());
        assert!(router.route_message("b".to_owned()).is_ok());
        assert!(router.route_message("c".to_owned()).is_ok());
        assert!(router.route_message("d".to_owned()).is_ok());

        drop(router);

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

        let router = Router::new(vec![sender1, sender2], RoutingStrategy::RoundRobin(0));

        assert!(router.broadcast_message("a".to_owned()).is_ok());
        assert!(router.broadcast_message("b".to_owned()).is_ok());

        drop(router);

        let receiver1_messages: Vec<String> = receiver1.iter().collect();
        let receiver2_messages: Vec<String> = receiver2.iter().collect();

        assert_eq!(receiver1_messages, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(receiver2_messages, vec!["a".to_owned(), "b".to_owned()]);
    }
}
