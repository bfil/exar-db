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
    pub fn new(sender: S, threads: Vec<T>) -> Self {
        let mut threads = ControllableThreads { sender, threads, join_handles: vec![] };
        match threads.start() {
            Ok(_)    => (),
            Err(err) => error!("Unable to start controllable threads: {}", err)
        };
        threads
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
            RoutingStrategy::RoundRobin(ref last_index) => {
                let new_index = if last_index + 1 < self.senders.len() { last_index + 1 } else { 0 };
                match self.senders.get(new_index) {
                    Some(sender) => {
                        sender.send_message(message)?;
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

    #[test]
    fn test_controllable_threads_message_passing() {

    }

    #[test]
    fn test_controllable_threads_stop() {

    }

    #[test]
    fn test_route_message() {

    }
}
