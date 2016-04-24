use super::*;

use indexed_line_reader::*;

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Scanner {
    action_sender: Sender<ScannerAction>
}

impl Scanner {
    pub fn new(reader: IndexedLineReader<BufReader<File>>, sleep_duration: Duration) -> Scanner {
        let (sender, receiver) = channel();
        ScannerThread::new(reader, receiver).run(sleep_duration);
        Scanner {
            action_sender: sender
        }
    }

    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        match self.action_sender.send(ScannerAction::HandleSubscription(subscription)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    pub fn add_line_index(&self, line: u64, byte_count: u64) -> Result<(), DatabaseError> {
        match self.action_sender.send(ScannerAction::AddLineIndex(line, byte_count)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    pub fn set_tail_scanner_sender(&mut self, sender: Sender<ScannerAction>) -> Result<(), DatabaseError> {
        match self.action_sender.send(ScannerAction::SetTailScannerSender(sender)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

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
            Err(err) => println!("Unable to stop scanner thread: {}", err)
        }
    }
}

#[derive(Debug)]
pub struct ScannerThread {
    index: LinesIndex,
    reader: IndexedLineReader<BufReader<File>>,
    action_receiver: Receiver<ScannerAction>,
    tail_scanner_sender: Option<Sender<ScannerAction>>,
    subscriptions: Vec<Subscription>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, receiver: Receiver<ScannerAction>) -> ScannerThread {
        ScannerThread {
            index: reader.get_index().clone(),
            reader: reader,
            action_receiver: receiver,
            tail_scanner_sender: None,
            subscriptions: vec![]
        }
    }

    fn run(mut self, sleep_duration: Duration) -> JoinHandle<Self> {
        thread::spawn(move || {
            'main: loop {
                while let Ok(action) = self.action_receiver.try_recv() {
                    match action {
                        ScannerAction::HandleSubscription(subscription) => {
                            self.subscriptions.push(subscription);
                        },
                        ScannerAction::AddLineIndex(line, byte_count) => {
                            self.index.insert(line, byte_count);
                            self.reader.restore_index(self.index.clone());
                        },
                        ScannerAction::SetTailScannerSender(sender) => {
                            self.tail_scanner_sender = Some(sender);
                        },
                        ScannerAction::Stop => break 'main
                    }
                }
                if self.subscriptions.len() != 0 {
                    match self.scan() {
                        Ok(_) => self.retain_active_subscriptions(),
                        Err(err) => println!("Unable to scan log: {}", err)
                    }
                }
                thread::sleep(sleep_duration);
            };
            self.subscriptions.truncate(0);
            self
        })
    }

    fn retain_active_subscriptions(&mut self) {
        match self.tail_scanner_sender {
            Some(ref tail_scanner_sender) => {
                for subscription in self.subscriptions.iter().filter(|s| {
                    s.is_active() && s.query.live_stream && s.query.is_active()
                }) {
                    let _ = tail_scanner_sender.send(ScannerAction::HandleSubscription(subscription.clone()));
                }
                self.subscriptions.truncate(0);
            },
            None => self.subscriptions.retain(|s| {
                s.is_active() && s.query.is_active()
            })
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
                                        let _ = subscription.send(event.clone());
                                    }
                                    if interval.end == event.id || self.subscriptions.iter().all(|s| !s.query.is_active()) {
                                        break;
                                    }
                                },
                                Err(err) => println!("Unable to deserialize log line: {}", err)
                            },
                            Err(err) => println!("Unable to read log line: {}", err)
                        }
                    }
                },
                Err(err) => return Err(DatabaseError::new_io_error(err))
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum ScannerAction {
    HandleSubscription(Subscription),
    AddLineIndex(u64, u64),
    SetTailScannerSender(Sender<ScannerAction>),
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

    fn sleep_duration() -> Duration {
        Duration::from_millis(10)
    }

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

        let _ = Scanner::new(line_reader, sleep_duration());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_message_passing() {
        let (log, line_reader) = create_log_and_line_reader();

        let mut scanner = Scanner::new(line_reader, sleep_duration());

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

        let (tail_scanner_sender, tail_scanner_receiver) = channel();
        assert!(scanner.set_tail_scanner_sender(tail_scanner_sender).is_ok());

        match receiver.recv() {
            Ok(ScannerAction::SetTailScannerSender(sender)) => {
                assert!(sender.send(ScannerAction::HandleSubscription(subscription.clone())).is_ok())
            },
            _ => panic!("Expected to receive a SetTailScannerSender message")
        }

        match tail_scanner_receiver.recv() {
            Ok(ScannerAction::HandleSubscription(s)) => {
                assert_eq!(s.query, subscription.query);
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

        let (sender, receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, receiver);
        let handle = scanner_thread.run(sleep_duration());

        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.subscriptions.len(), 0);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_new_indexed_line() {
        let (log, line_reader) = create_log_and_line_reader();

        let (sender, receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, receiver);
        let handle = scanner_thread.run(sleep_duration());

        assert!(sender.send(ScannerAction::AddLineIndex(100, 1234)).is_ok());
        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.index.byte_count_at_pos(&100), Some(1234));
        assert_eq!(scanner_thread.reader.get_index().clone().byte_count_at_pos(&100), Some(1234));

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_set_tail_scanner_sender() {
        let (log, line_reader) = create_log_and_line_reader();

        let (sender, receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, receiver);
        let handle = scanner_thread.run(sleep_duration());

        let (tail_scanner_sender, _) = channel();

        assert!(sender.send(ScannerAction::SetTailScannerSender(tail_scanner_sender)).is_ok());
        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert!(scanner_thread.tail_scanner_sender.is_some());

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
        let scanner_thread = ScannerThread::new(line_reader, thread_receiver);
        scanner_thread.run(sleep_duration);

        let (sender, receiver) = channel();
        let live_subscription = Subscription::new(sender, Query::live());

        let (tail_scanner_sender, tail_scanner_receiver) = channel();
        assert!(thread_sender.send(ScannerAction::SetTailScannerSender(tail_scanner_sender)).is_ok());
        thread::sleep(sleep_duration * 2);

        assert!(thread_sender.send(ScannerAction::HandleSubscription(live_subscription.clone())).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        if let Ok(ScannerAction::HandleSubscription(s)) = tail_scanner_receiver.try_recv() {
            assert_eq!(s.query.live_stream, true);
        } else {
            panic!("Unable to receive live subscription from the tail scanner receiver");
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
    fn test_tail_scanner_thread_subscriptions_management() {
        let log = create_log();
        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");
        let line_reader = log.open_line_reader().expect("Unable to open line reader");
        let event = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(logger.log(event).is_ok());

        let (thread_sender, thread_receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, thread_receiver);
        scanner_thread.run(sleep_duration);

        let (sender, receiver) = channel();
        let live_subscription = Subscription::new(sender, Query::live());

        assert!(thread_sender.send(ScannerAction::HandleSubscription(live_subscription.clone())).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        assert_eq!(receiver.try_recv().err(), Some(TryRecvError::Empty));
    }

    #[test]
    fn test_scanner_thread_subscriptions_intervals_merging() {

        let (sender, _) = channel();
        let subscriptions = vec![
            Subscription::new(sender.clone(), Query::live().offset(0).limit(10)),
            Subscription::new(sender.clone(), Query::current().offset(30).limit(20)),
            Subscription::new(sender.clone(), Query::live().offset(40).limit(30))
        ];

        let intervals: Vec<_> = subscriptions.iter().map(|s| s.query.interval()).collect();

        assert_eq!(intervals.merged(), vec![Interval::new(0, 10), Interval::new(30, 70)]);
    }
}
