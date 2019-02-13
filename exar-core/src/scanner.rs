use super::*;

use indexed_line_reader::*;
use rand;
use rand::Rng;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;

/// Exar DB's log file scanner.
///
/// It manages event stream subscriptions and continuously scans
/// portions of the log file depending on the subscriptions query parameters.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::channel;
///
/// let log = Log::new("/path/to/logs", "test", 100);
/// let event = Event::new("data", vec!["tag1", "tag2"]);
///
/// let line_reader = log.open_line_reader().unwrap();
/// let (publisher_sender, _) = channel();
/// let mut scanner = Scanner::new(line_reader, publisher_sender);
///
/// let (sender, _) = channel();
/// let subscription = Subscription::new(sender, Query::live());
/// scanner.handle_subscription(subscription).unwrap();
///
/// drop(scanner);
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Scanner {
    action_senders: Vec<Sender<ScannerAction>>,
    routing_strategy: RoutingStrategy
}

impl Scanner {
    /// Creates a new log scanner using the given `IndexedLineReader` and a `Publisher`.
    pub fn new(log: &Log, publisher: &Publisher, config: &CollectionConfig) -> Result<Scanner, DatabaseError> {
        Ok(Scanner {
            action_senders: Scanner::run_scanner_threads(log, publisher, config)?,
            routing_strategy: config.routing_strategy.clone()
        })
    }

    fn run_scanner_threads(log: &Log, publisher: &Publisher, config: &CollectionConfig) -> Result<Vec<Sender<ScannerAction>>, DatabaseError> {
        let mut action_senders = vec![];
        for _ in 0..config.scanner.threads {
            let (sender, receiver) = channel();
            let line_reader = log.open_indexed_line_reader()?;
            ScannerThread::new(line_reader, receiver, publisher.clone()).run();
            action_senders.push(sender);
        }
        Ok(action_senders)
    }

    /// Subscribes to the collection of events using the given query and returns an event stream
    /// or a `DatabaseError` if a failure occurs.
    pub fn handle_subscription(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        let (sender, receiver) = channel();
        self.send_subscription(Subscription::new(sender, query)).and_then(|_| {
            Ok(EventStream::new(receiver))
        })
    }

    /// Adds the given line to the `LinesIndex` or returns a `DatabaseError` if a failure occurs.
    pub fn update_index(&self, index: &LinesIndex) -> Result<(), DatabaseError> {
        self.broadcast_action(ScannerAction::UpdateIndex(index.clone()))
    }

    fn send_subscription(&mut self, subscription: Subscription) -> Result<(), DatabaseError> {
        (match self.routing_strategy {
            RoutingStrategy::Random => match rand::thread_rng().choose(&self.action_senders) {
                Some(action_sender) =>
                    self.send_action(action_sender, ScannerAction::HandleSubscription(subscription)).and_then(|_| {
                        Ok(RoutingStrategy::Random)
                    }),
                None => Err(DatabaseError::SubscriptionError)
            },
            RoutingStrategy::RoundRobin(ref last_index) => {
                let new_index = if last_index + 1 < self.action_senders.len() { last_index + 1 } else { 0 };
                match self.action_senders.get(new_index) {
                    Some(action_sender) =>
                        self.send_action(action_sender, ScannerAction::HandleSubscription(subscription)).and_then(|_| {
                            Ok(RoutingStrategy::RoundRobin(new_index))
                        }),
                    None => Err(DatabaseError::SubscriptionError)
                }
            }
        }).and_then(|updated_strategy| {
            self.routing_strategy = updated_strategy;
            Ok(())
        })
    }

    fn send_action(&self, action_sender: &Sender<ScannerAction>, action: ScannerAction) -> Result<(), DatabaseError> {
        match action_sender.send(action) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    fn broadcast_action(&self, action: ScannerAction) -> Result<(), DatabaseError> {
        for action_sender in &self.action_senders {
            self.send_action(action_sender, action.clone())?;
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), DatabaseError> {
        self.broadcast_action(ScannerAction::Stop)
    }
}

impl Drop for Scanner {
    fn drop(&mut self) {
        match self.stop() {
            Ok(_) => (),
            Err(err) => error!("Unable to stop scanner thread: {}", err)
        }
    }
}

/// Exar DB's log file scanner thread.
///
/// It uses a channel receiver to receive actions to be performed between scans,
/// and it manages the thread that continuously scans portions of the log file
/// depending on the subscriptions query parameters.
#[derive(Debug)]
pub struct ScannerThread {
    reader: IndexedLineReader<BufReader<File>>,
    action_receiver: Receiver<ScannerAction>,
    publisher: Publisher,
    subscriptions: Vec<Subscription>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, receiver: Receiver<ScannerAction>, publisher: Publisher) -> ScannerThread {
        ScannerThread {
            reader: reader,
            action_receiver: receiver,
            publisher: publisher,
            subscriptions: vec![]
        }
    }

    fn run(mut self) -> JoinHandle<Self> {
        thread::spawn(move || {
            'main: loop {
                while let Ok(action) = self.action_receiver.recv() {
                    let mut actions = vec![ action ];
                    while let Ok(action) = self.action_receiver.try_recv() {
                        actions.push(action);
                    }
                    for action in actions {
                        match action {
                            ScannerAction::HandleSubscription(subscription) => {
                                self.subscriptions.push(subscription);
                            },
                            ScannerAction::UpdateIndex(index) => {
                                self.reader.restore_index(index);
                            },
                            ScannerAction::Stop => break 'main
                        }
                    }
                    if !self.subscriptions.is_empty() {
                        match self.scan() {
                            Ok(_) => self.send_subscriptions_to_publisher(),
                            Err(err) => error!("Unable to scan log: {}", err)
                        }
                    }
                }
            };
            self.subscriptions.truncate(0);
            self
        })
    }

    fn send_subscriptions_to_publisher(&mut self) {
        for subscription in self.subscriptions.drain(..) {
            let _ = self.publisher.handle_subscription(subscription.clone());
        }
    }

    fn subscriptions_intervals(&self) -> Vec<Interval<u64>> {
        self.subscriptions.iter().map(|s| s.interval()).collect()
    }

    fn scan(&mut self) -> Result<(), DatabaseError> {
        for interval in self.subscriptions_intervals().merged() {
            match self.reader.seek(SeekFrom::Start(interval.start)) {
                Ok(_) => {
                    for line in (&mut self.reader).lines() {
                        match line {
                            Ok(line) => match Event::from_tab_separated_str(&line) {
                                Ok(ref event) => {
                                    for subscription in self.subscriptions.iter_mut().filter(|s| s.matches_event(event)) {
                                        let _ = subscription.send(event.clone());
                                    }
                                    if interval.end == event.id || self.subscriptions.iter().all(|s| !s.is_active()) {
                                        break;
                                    }
                                },
                                Err(err) => warn!("Unable to deserialize log line: {}", err)
                            },
                            Err(err) => warn!("Unable to read log line: {}", err)
                        }
                    }
                },
                Err(err) => return Err(DatabaseError::from_io_error(err))
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum ScannerAction {
    HandleSubscription(Subscription),
    UpdateIndex(LinesIndex),
    Stop
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    use indexed_line_reader::*;

    use std::fs::*;
    use std::io::BufReader;
    use std::sync::mpsc::{channel, TryRecvError};
    use std::thread;
    use std::time::Duration;

    fn create_log() -> Log {
        let ref collection_name = random_collection_name();
        let log = Log::new("", collection_name, 100);
        assert!(log.open_writer().is_ok());
        log
    }

    fn create_log_and_line_reader() -> (Log, IndexedLineReader<BufReader<File>>) {
        let log = create_log();
        let line_reader = log.open_line_reader().expect("Unable to open line reader");
        (log, line_reader)
    }

    #[test]
    fn test_scanner_constructor() {
        let (log, line_reader) = create_log_and_line_reader();
        let (publisher_sender, _) = channel();

        let _ = Scanner::new(line_reader, publisher_sender);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_message_passing() {
        let (log, line_reader) = create_log_and_line_reader();
        let (publisher_sender, _) = channel();

        let mut scanner = Scanner::new(line_reader, publisher_sender);

        assert!(scanner.stop().is_ok());

        let (sender, _) = channel();
        let subscription = Subscription::new(sender, Query::live());

        let (sender, receiver) = channel();
        scanner.action_sender = sender;

        assert!(scanner.handle_subscription(subscription.clone()).is_ok());

        match receiver.recv() {
            Ok(ScannerAction::HandleSubscription(s)) => {
                assert_eq!(s.is_live(), true);
            },
            _ => panic!("Expected to receive an HandleSubscription message")
        }

        assert!(scanner.add_line_index(100, 1000).is_ok());

        match receiver.recv() {
            Ok(ScannerAction::AddLineIndex(pos, byte_count)) => {
                assert_eq!(pos, 100);
                assert_eq!(byte_count, 1000);
            },
            _ => panic!("Expected to receive an HandleSubscription message")
        }

        assert!(scanner.stop().is_ok());

        match receiver.recv() {
            Ok(ScannerAction::Stop) => (),
            _ => panic!("Expected to receive a Stop message")
        }

        drop(receiver);

        assert!(scanner.stop().is_err());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_stop() {
        let (log, line_reader) = create_log_and_line_reader();
        let (publisher_sender, _) = channel();

        let (sender, receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, receiver, publisher_sender);
        let handle = scanner_thread.run();

        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.subscriptions.len(), 0);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_new_indexed_line() {
        let (log, line_reader) = create_log_and_line_reader();
        let (publisher_sender, _) = channel();

        let (sender, receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, receiver, publisher_sender);
        let handle = scanner_thread.run();

        assert!(sender.send(ScannerAction::AddLineIndex(100, 1234)).is_ok());
        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.index.byte_count_at_pos(&100), Some(1234));
        assert_eq!(scanner_thread.reader.get_index().clone().byte_count_at_pos(&100), Some(1234));

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_subscriptions_management() {
        let log = create_log();
        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");
        let line_reader = log.open_line_reader().expect("Unable to open line reader");
        let event = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(logger.log(event).is_ok());

        let (thread_sender, thread_receiver) = channel();
        let (publisher_sender, publisher_receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, thread_receiver, publisher_sender);
        scanner_thread.run();

        let (sender, receiver) = channel();
        let live_subscription = Subscription::new(sender, Query::live());

        assert!(thread_sender.send(ScannerAction::HandleSubscription(live_subscription.clone())).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        if let Ok(PublisherAction::AddSubscription(s)) = publisher_receiver.try_recv() {
            assert_eq!(s.is_live(), true);
        } else {
            panic!("Unable to receive live subscription from the publisher receiver");
        }
        assert_eq!(receiver.try_recv().err(), Some(TryRecvError::Empty));

        let (sender, receiver) = channel();
        let current_subscription = Subscription::new(sender, Query::current());

        assert!(thread_sender.send(ScannerAction::HandleSubscription(current_subscription)).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        assert_eq!(receiver.try_recv().err(), Some(TryRecvError::Disconnected));

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_apply_round_robin_routing_strategy() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, &config)
            .expect("Unable to create collection");

        let (sender, _) = channel();
        let subscription = Subscription::new(sender, Query::current());

        collection.routing_strategy = RoutingStrategy::RoundRobin(0);

        let updated_strategy = collection.apply_routing_strategy(subscription.clone())
            .expect("Unable to apply routing strategy");

        assert_eq!(updated_strategy, RoutingStrategy::RoundRobin(1));

        collection.routing_strategy = updated_strategy;

        let updated_strategy = collection.apply_routing_strategy(subscription)
            .expect("Unable to apply routing strategy");

        assert_eq!(updated_strategy, RoutingStrategy::RoundRobin(0));

        assert!(collection.drop().is_ok());
    }
}
