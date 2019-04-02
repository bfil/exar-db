use super::*;

use rand;
use rand::Rng;
use std::sync::mpsc::Sender;
use std::thread;
use std::thread::JoinHandle;

#[derive(Debug)]
pub struct ControllableThread<S: Stop, T: Run<T> + Send + 'static> {
    sender: S,
    thread: Option<T>,
    join_handle: Option<JoinHandle<T>>
}

impl<S: Stop, T: Run<T> + Send + 'static> ControllableThread<S, T> {
    pub fn new(sender: S, thread: T) -> Self {
        ControllableThread { sender, thread: Some(thread), join_handle: None }
    }

    pub fn sender(&self) -> &S {
        &self.sender
    }

    pub fn start(&mut self) -> DatabaseResult<()> {
        match self.thread.take() {
            None         => Err(DatabaseError::UnexpectedError),
            Some(thread) => Ok(self.join_handle = Some(thread::spawn(|| thread.run())))
        }
    }

    pub fn stop(&mut self) -> DatabaseResult<()> {
        match self.join_handle.take() {
            None         => Err(DatabaseError::UnexpectedError),
            Some(handle) => {
                self.sender.stop()?;
                Ok(self.thread = Some(handle.join().map_err(|_| DatabaseError::UnexpectedError)?))
            }
        }
    }
}

impl<S: Stop, T: Run<T> + Send + 'static> Drop for ControllableThread<S, T> {
    fn drop(&mut self) {
        match self.stop() {
            Ok(_)    => (),
            Err(err) => warn!("Unable to stop controllable thread: {}", err)
        };
    }
}

#[derive(Debug)]
pub struct ControllableThreads<S: Stop, T: Run<T> + Send + 'static> {
    sender: S,
    threads: Vec<T>,
    join_handles: Vec<JoinHandle<T>>
}

impl<S: Stop, T: Run<T> + Send + 'static> ControllableThreads<S, T> {
    pub fn new(sender: S, threads: Vec<T>) -> Self {
        ControllableThreads { sender, threads, join_handles: vec![] }
    }

    pub fn sender(&self) -> &S {
        &self.sender
    }

    pub fn start(&mut self) -> DatabaseResult<()> {
        if self.threads.is_empty() {
            Err(DatabaseError::UnexpectedError)
        } else {
            for thread in self.threads.drain(..) {
                let join_handle = thread::spawn(|| thread.run());
                self.join_handles.push(join_handle)
            }
            Ok(())
        }
    }

    pub fn stop(&mut self) -> DatabaseResult<()> {
        if self.join_handles.is_empty() {
            Err(DatabaseError::UnexpectedError)
        } else {
            self.sender.stop()?;
            for handle in self.join_handles.drain(..) {
                let thread = handle.join().map_err(|_| DatabaseError::UnexpectedError)?;
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
            Err(err) => warn!("Unable to stop controllable threads: {}", err)
        };
    }
}

pub trait Run<T> {
    fn run(self) -> T;
}

pub trait Stop {
    fn stop(&mut self) -> DatabaseResult<()>;
}

pub trait SendMessage<T> {
    fn send_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T> SendMessage<T> for Sender<T> {
    fn send_message(&self, message: T) -> DatabaseResult<()> {
        self.send(message).map_err(|_| DatabaseError::UnexpectedError)
    }
}

pub trait BroadcastMessage<T> {
    fn broadcast_message(&self, message: T) -> DatabaseResult<()>;
}

impl<T: Clone> BroadcastMessage<T> for Vec<Sender<T>> {
    fn broadcast_message(&self, message: T) -> DatabaseResult<()> {
        for sender in self {
            sender.send_message(message.clone())?;
        }
        Ok(())
    }
}

pub trait RouteMessage<T> {
    fn route_message(&self, message: T, routing_strategy: &RoutingStrategy) -> DatabaseResult<RoutingStrategy>;
}

impl<T: Clone> RouteMessage<T> for Vec<Sender<T>> {
    fn route_message(&self, message: T, routing_strategy: &RoutingStrategy) -> DatabaseResult<RoutingStrategy> {
        (match routing_strategy {
            RoutingStrategy::Random => match rand::thread_rng().choose(self) {
                Some(sender) => {
                    sender.send_message(message)?;
                    Ok(RoutingStrategy::Random)
                },
                None => Err(DatabaseError::SubscriptionError)
            },
            RoutingStrategy::RoundRobin(ref last_index) => {
                let new_index = if last_index + 1 < self.len() { last_index + 1 } else { 0 };
                match self.get(new_index) {
                    Some(sender) => {
                        sender.send_message(message)?;
                        Ok(RoutingStrategy::RoundRobin(new_index))
                    },
                    None => Err(DatabaseError::SubscriptionError)
                }
            }
        })
    }
}