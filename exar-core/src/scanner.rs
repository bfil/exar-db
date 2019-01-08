use super::*;

use indexed_line_reader::*;

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
    action_sender: Sender<ScannerAction>
}

impl Scanner {
    /// Creates a new log scanner using the given `IndexedLineReader` and a `Publisher`.
    pub fn new(reader: IndexedLineReader<BufReader<File>>, publisher_sender: Sender<PublisherAction>) -> Scanner {
        let (sender, receiver) = channel();
        ScannerThread::new(reader, receiver, publisher_sender).run();
        Scanner {
            action_sender: sender
        }
    }

    /// Handles the given `Subscription` or returns a `DatabaseError` if a failure occurs.
    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        match self.action_sender.send(ScannerAction::HandleSubscription(subscription)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    /// Adds the given line to the `LinesIndex` or returns a `DatabaseError` if a failure occurs.
    pub fn add_line_index(&self, line: u64, byte_count: u64) -> Result<(), DatabaseError> {
        match self.action_sender.send(ScannerAction::AddLineIndex(line, byte_count)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    /// Clones the channel sender responsible to send `ScannerAction`s to the scanner thread.
    pub fn clone_action_sender(&self) -> Sender<ScannerAction> {
        self.action_sender.clone()
    }

    fn stop(&self) -> Result<(), DatabaseError> {
        match self.action_sender.send(ScannerAction::Stop) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
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
    index: LinesIndex,
    reader: IndexedLineReader<BufReader<File>>,
    action_receiver: Receiver<ScannerAction>,
    publisher_sender: Sender<PublisherAction>,
    subscriptions: Vec<Subscription>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, receiver: Receiver<ScannerAction>, publisher_sender: Sender<PublisherAction>) -> ScannerThread {
        ScannerThread {
            index: reader.get_index().clone(),
            reader: reader,
            action_receiver: receiver,
            publisher_sender: publisher_sender,
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
                            ScannerAction::AddLineIndex(line, byte_count) => {
                                self.index.insert(line, byte_count);
                                self.reader.restore_index(self.index.clone());
                            },
                            ScannerAction::Stop => break 'main
                        }
                    }
                    if !self.subscriptions.is_empty() {
                        match self.scan() {
                            Ok(_) => self.handle_live_subscriptions(),
                            Err(err) => error!("Unable to scan log: {}", err)
                        }
                    }
                }
            };
            self.subscriptions.truncate(0);
            self
        })
    }

    fn handle_live_subscriptions(&mut self) {
        for subscription in self.subscriptions.drain(..) {
            if subscription.is_active() && subscription.query.live_stream && subscription.query.is_active() {
                let _ = self.publisher_sender.send(PublisherAction::AddSubscription(subscription.clone()));
            }
        }
    }

    fn subscriptions_intervals(&self) -> Vec<Interval<u64>> {
        self.subscriptions.iter().map(|s| s.query.interval()).collect()
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
                                        // TODO: if sending fails it will skip and publish later events, should probable fail and remove subscription
                                        let _ = subscription.send(event.clone());
                                    }
                                    if interval.end == event.id || self.subscriptions.iter().all(|s| !s.query.is_active()) {
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
    AddLineIndex(u64, u64),
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
                assert_eq!(s.query, subscription.query);
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
            assert_eq!(s.query.live_stream, true);
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
}
