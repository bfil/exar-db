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
    send: Sender<ScannerAction>
}

impl Scanner {
    pub fn new(reader: IndexedLineReader<BufReader<File>>, sleep_duration: Duration) -> Scanner {
        let (send, recv) = channel();
        ScannerThread::new(reader, recv).run(sleep_duration);
        Scanner {
            send: send
        }
    }

    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        match self.send.send(ScannerAction::HandleSubscription(subscription)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    pub fn add_line_index(&self, line: usize, byte_count: usize) -> Result<(), DatabaseError> {
        match self.send.send(ScannerAction::AddLineIndex(line, byte_count)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    pub fn set_live_sender(&mut self, send: Sender<ScannerAction>) -> Result<(), DatabaseError> {
        match self.send.send(ScannerAction::SetLiveSender(send)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    pub fn clone_sender(&self) -> Sender<ScannerAction> {
        self.send.clone()
    }

    fn stop(&self) -> Result<(), DatabaseError> {
        match self.send.send(ScannerAction::Stop) {
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
    recv: Receiver<ScannerAction>,
    live_send: Option<Sender<ScannerAction>>,
    subscriptions: Vec<Subscription>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, recv: Receiver<ScannerAction>) -> ScannerThread {
        ScannerThread {
            index: LinesIndex::new(100000),
            reader: reader,
            recv: recv,
            live_send: None,
            subscriptions: vec![]
        }
    }

    fn run(mut self, sleep_duration: Duration) -> JoinHandle<Self> {
        thread::spawn(move || {
            'main: loop {
                while let Ok(action) = self.recv.try_recv() {
                    match action {
                        ScannerAction::HandleSubscription(subscription) => self.subscriptions.push(subscription),
                        ScannerAction::AddLineIndex(line, byte_count) => {
                            self.index.insert(line as u64, byte_count as u64);
                            self.reader.restore_index(self.index.clone());
                        },
                        ScannerAction::SetLiveSender(send) => {
                            self.live_send = Some(send);
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
        match self.live_send {
            Some(ref live_send) => {
                for subscription in self.subscriptions.iter().filter(|s| s.query.is_live()) {
                    let _ = live_send.send(ScannerAction::HandleSubscription(subscription.clone()));
                }
                self.subscriptions.truncate(0);
            },
            None => self.subscriptions.retain(|s| {
                s.is_active() && s.query.is_live() && s.query.is_active()
            })
        }
    }

    fn subscriptions_intervals(&self) -> Vec<Interval<u64>> {
        self.subscriptions.iter().map(|s| {
            let start = s.query.position as u64;
            let end = if s.query.is_live() || s.query.limit.is_none() {
                u64::max_value()
            } else {
                start + s.query.limit.unwrap() as u64
            };
            Interval::new(start, end)
        }).collect()
    }

    fn merged_intervals(&self) -> Vec<Interval<u64>> {
        let mut intervals = self.subscriptions_intervals();
        intervals.merge();
        intervals
    }

    fn scan(&mut self) -> Result<(), DatabaseError> {
        for interval in self.merged_intervals() {
            match self.reader.seek(SeekFrom::Start(interval.start)) {
                Ok(_) => {
                    for line in (&mut self.reader).lines() {
                        match line {
                            Ok(line) => match Event::from_tab_separated_str(&line) {
                                Ok(ref event) => {
                                    for subscription in self.subscriptions.iter_mut().filter(|s| s.matches_event(event)) {
                                        let _ = subscription.send(event.clone());
                                    }
                                    if interval.end as usize == event.id {
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
    AddLineIndex(usize, usize),
    SetLiveSender(Sender<ScannerAction>),
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

        let (send, _) = channel();
        let subscription = Subscription::new(send, Query::live());

        let (send, recv) = channel();
        scanner.send = send;

        assert!(scanner.handle_subscription(subscription.clone()).is_ok());

        match recv.recv() {
            Ok(ScannerAction::HandleSubscription(s)) => {
                assert_eq!(s.query, subscription.query);
            },
            _ => panic!("Expected to receive an HandleSubscription message")
        }

        assert!(scanner.add_line_index(100, 1000).is_ok());

        match recv.recv() {
            Ok(ScannerAction::AddLineIndex(pos, byte_count)) => {
                assert_eq!(pos, 100);
                assert_eq!(byte_count, 1000);
            },
            _ => panic!("Expected to receive an HandleSubscription message")
        }

        assert!(scanner.stop().is_ok());

        match recv.recv() {
            Ok(ScannerAction::Stop) => (),
            _ => panic!("Expected to receive a Stop message")
        }

        drop(recv);

        assert!(scanner.stop().is_err());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_stop() {
        let (log, line_reader) = create_log_and_line_reader();

        let (send, recv) = channel();
        let scanner_thread = ScannerThread::new(line_reader, recv);
        let handle = scanner_thread.run(sleep_duration());

        assert!(send.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.subscriptions.len(), 0);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_new_indexed_line() {
        let (log, line_reader) = create_log_and_line_reader();

        let (send, recv) = channel();
        let scanner_thread = ScannerThread::new(line_reader, recv);
        let handle = scanner_thread.run(sleep_duration());

        assert!(send.send(ScannerAction::AddLineIndex(100, 1234)).is_ok());
        assert!(send.send(ScannerAction::Stop).is_ok());

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

        let (thread_send, thread_recv) = channel();
        let scanner_thread = ScannerThread::new(line_reader, thread_recv);
        scanner_thread.run(sleep_duration);

        let (send, recv) = channel();
        let live_subscription = Subscription::new(send, Query::live());

        assert!(thread_send.send(ScannerAction::HandleSubscription(live_subscription)).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(recv.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        assert_eq!(recv.try_recv().err(), Some(TryRecvError::Empty));

        let (send, recv) = channel();
        let current_subscription = Subscription::new(send, Query::current());

        assert!(thread_send.send(ScannerAction::HandleSubscription(current_subscription)).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(recv.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        assert_eq!(recv.try_recv().err(), Some(TryRecvError::Disconnected));

        assert!(log.remove().is_ok());
    }
}
