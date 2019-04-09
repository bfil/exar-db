use super::*;

use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::thread::JoinHandle;

/// A single-threaded executor.
///
/// It runs a background thread that can be interacted with via messaging to run custom tasks.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::{Sender, Receiver};
///
/// struct TestThread {
///     receiver: Receiver<String>
/// }
///
/// impl Run for TestThread {
///     fn run(mut self) -> Self {
///         'main: loop {
///             while let Ok(message) = self.receiver.recv() {
///                 match message.as_ref() {
///                     "stop"  => break 'main,
///                     message => println!("Received: {}", message)
///                 }
///             }
///         };
///         self
///     }
/// }
///
/// struct TestSender {
///     sender: Sender<String>
/// }
///
/// impl TestSender {
///     fn send(&self, message: String) -> DatabaseResult<()> {
///         self.sender.send_message(message)
///     }
/// }
///
/// impl Stop for TestSender {
///     fn stop(&self) -> DatabaseResult<()> {
///         self.sender.send_message("stop".to_owned())
///     }
/// }
///
/// let mut executor = SingleThreadedExecutor::new(
///     |sender|   TestSender { sender },
///     |receiver| Ok(TestThread { receiver })
/// ).expect("Unable to create executor");
///
/// executor.sender().send("a".to_owned()).expect("Unable to send message");
/// executor.sender().send("b".to_owned()).expect("Unable to send message");
///
/// drop(executor);
/// # }
/// ```
#[derive(Debug)]
pub struct SingleThreadedExecutor<S: Stop, T: Run + Send + 'static> {
    sender: S,
    thread: Option<T>,
    join_handle: Option<JoinHandle<T>>
}

impl<S: Stop, T: Run + Send + 'static> SingleThreadedExecutor<S, T> {
    /// Creates a new single-threaded executor.
    pub fn new<M, FS, FR>(fs: FS, fr: FR) -> DatabaseResult<Self>
        where FS: Fn(Sender<M>) -> S, FR: Fn(Receiver<M>) -> DatabaseResult<T> {
        let (sender, receiver) = channel();
        let sender = fs(sender);
        let thread = fr(receiver)?;
        let mut executor = SingleThreadedExecutor {
            sender, thread: Some(thread), join_handle: None
        };
        match executor.start() {
            Ok(_)    => Ok(executor),
            Err(err) => {
                            error!("Unable to start single-threaded executor: {}", err);
                            Err(DatabaseError::InternalError)
                        }
        }
    }

    /// Returns a reference to the sender.
    pub fn sender(&self) -> &S {
        &self.sender
    }

    fn start(&mut self) -> DatabaseResult<()> {
        match self.thread.take() {
            Some(thread) => Ok(self.join_handle = Some(thread::spawn(|| thread.run()))),
            None         => Err(DatabaseError::InternalError)
        }
    }

    fn stop(&mut self) -> DatabaseResult<()> {
        match self.join_handle.take() {
            Some(join_handle) => {
                                     self.sender.stop()?;
                                     let thread = join_handle.join().map_err(|_| DatabaseError::InternalError)?;
                                     Ok(self.thread = Some(thread))
                                 },
            None              => Err(DatabaseError::InternalError)
        }
    }
}

impl<S: Stop, T: Run + Send + 'static> Drop for SingleThreadedExecutor<S, T> {
    fn drop(&mut self) {
        match self.stop() {
            Ok(_)    => (),
            Err(err) => error!("Unable to stop single-threaded executor: {}", err)
        };
    }
}

/// A multi-threaded executor.
///
/// It runs a background thread that can be interacted with via messaging to run custom tasks.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::{Sender, Receiver};
///
/// struct TestThread {
///     receiver: Receiver<String>
/// }
///
/// impl Run for TestThread {
///     fn run(mut self) -> Self {
///         'main: loop {
///             while let Ok(message) = self.receiver.recv() {
///                 match message.as_ref() {
///                     "stop"  => break 'main,
///                     message => println!("Received: {}", message)
///                 }
///             }
///         };
///         self
///     }
/// }
///
/// struct TestRouter {
///     router: Router<String>
/// }
///
/// impl TestRouter {
///     fn route(&self, message: String) -> DatabaseResult<()> {
///         self.router.route_message(message)
///     }
/// }
///
/// impl Stop for TestRouter {
///     fn stop(&self) -> DatabaseResult<()> {
///         self.router.broadcast_message("stop".to_owned())
///     }
/// }
///
/// let mut executor = MultiThreadedExecutor::new(2,
///     |senders|  TestRouter { router: Router::new(senders, RoutingStrategy::default()) },
///     |receiver| Ok(TestThread { receiver })
/// ).expect("Unable to create executor");
///
/// executor.sender().route("a".to_owned()).expect("Unable to send message");
/// executor.sender().route("b".to_owned()).expect("Unable to send message");
///
/// drop(executor);
/// # }
/// ```
#[derive(Debug)]
pub struct MultiThreadedExecutor<S: Stop, T: Run + Send + 'static> {
    sender: S,
    threads: Vec<T>,
    join_handles: Vec<JoinHandle<T>>
}

impl<S: Stop, T: Run + Send + 'static> MultiThreadedExecutor<S, T> {
    /// Creates a new multi-threaded executor.
    pub fn new<M, FS, FR>(number_of_threads: u8, fs: FS, fr: FR) -> DatabaseResult<Self>
        where FS: Fn(Vec<Sender<M>>) -> S, FR: Fn(Receiver<M>) -> DatabaseResult<T> {
        let mut senders = vec![];
        let mut threads = vec![];
        for _ in 0..number_of_threads {
            let (sender, receiver) = channel();
            senders.push(sender);
            let thread = fr(receiver)?;
            threads.push(thread);
        }
        let sender = fs(senders);
        let mut executor = MultiThreadedExecutor { sender, threads, join_handles: vec![] };
        match executor.start() {
            Ok(_)    => Ok(executor),
            Err(err) => {
                error!("Unable to start multi-threaded executor: {}", err);
                Err(DatabaseError::InternalError)
            }
        }
    }

    /// Returns a reference to the sender.
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

impl<S: Stop, T: Run + Send + 'static> Drop for MultiThreadedExecutor<S, T> {
    fn drop(&mut self) {
        match self.stop() {
            Ok(_)    => (),
            Err(err) => error!("Unable to stop multi-threaded executor: {}", err)
        };
    }
}

pub trait Run {
    fn run(self) -> Self;
}

pub trait Stop {
    fn stop(&self) -> DatabaseResult<()>;
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::sync::mpsc::{Sender, Receiver};

    struct TestThread {
        receiver: Receiver<String>,
        messages: Vec<String>
    }

    impl Run for TestThread {
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

    struct TestSender {
        sender: Sender<String>
    }

    impl TestSender {
        fn send(&self, message: String) -> DatabaseResult<()> {
            self.sender.send_message(message)
        }
    }

    impl Stop for TestSender {
        fn stop(&self) -> DatabaseResult<()> {
            self.sender.send_message("stop".to_owned())
        }
    }

    struct TestRouter {
        router: Router<String>
    }

    impl TestRouter {
        fn route(&self, message: String) -> DatabaseResult<()> {
            self.router.route_message(message)
        }
    }

    impl Stop for TestRouter {
        fn stop(&self) -> DatabaseResult<()> {
            self.router.broadcast_message("stop".to_owned())
        }
    }

    #[test]
    fn test_single_threaded_executor() {
        let mut executor = SingleThreadedExecutor::new(
            |sender|   TestSender { sender },
            |receiver| Ok(TestThread { receiver, messages: vec![] })
        ).expect("Unable to create executor");

        assert!(executor.thread.is_none());
        assert!(executor.join_handle.is_some());

        assert!(executor.sender().send("a".to_owned()).is_ok());
        assert!(executor.sender().send("b".to_owned()).is_ok());

        assert!(executor.stop().is_ok());

        assert!(executor.thread.is_some());
        assert!(executor.join_handle.is_none());

        let test_thread = executor.thread.take().expect("Unable to get test thread");
        assert_eq!(test_thread.messages, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn test_multi_threaded_executor() {
        let mut executor = MultiThreadedExecutor::new(2,
            |senders|  TestRouter { router: Router::new(senders, RoutingStrategy::default()) },
            |receiver| Ok(TestThread { receiver, messages: vec![] })
        ).expect("Unable to create executor");

        assert_eq!(executor.threads.len(), 0);
        assert_eq!(executor.join_handles.len(), 2);

        assert!(executor.sender().route("a".to_owned()).is_ok());
        assert!(executor.sender().route("b".to_owned()).is_ok());

        assert!(executor.stop().is_ok());

        assert_eq!(executor.threads.len(), 2);
        assert_eq!(executor.join_handles.len(), 0);

        let test_thread1 = executor.threads.get(0).expect("Unable to get test thread");
        let test_thread2 = executor.threads.get(1).expect("Unable to get test thread");
        assert_eq!(test_thread1.messages, vec!["a".to_owned()]);
        assert_eq!(test_thread2.messages, vec!["b".to_owned()]);
    }

    #[test]
    fn test_multi_threaded_executor_constructor_failure() {
        assert!(MultiThreadedExecutor::new(0, |senders| {
            TestRouter { router: Router::new(senders, RoutingStrategy::default()) }
        }, |receiver| {
            Ok(TestThread { receiver, messages: vec![] })
        }).is_err());
    }
}
