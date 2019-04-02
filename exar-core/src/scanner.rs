use super::*;

use indexed_line_reader::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc::{channel, Receiver, Sender};

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
/// let log       = Log::new("/path/to/logs", "test", 100).expect("Unable to create log");
/// let publisher = Publisher::new(&PublisherConfig::default());
/// let event     = Event::new("data", vec!["tag1", "tag2"]);
///
/// let line_reader = log.open_line_reader().expect("Unable to open line reader");
/// let mut scanner = Scanner::new(&log, &publisher, &ScannerConfig::default()).expect("Unable to create scanner");
///
/// scanner.sender_mut().handle_query(Query::live()).unwrap();
///
/// drop(scanner);
/// # }
/// ```
#[derive(Debug)]
pub struct Scanner {
    threads: ControllableThreads<ScannerSender, ScannerThread>
}

impl Scanner {
    /// Creates a new log scanner using the given `IndexedLineReader` and a `Publisher`.
    pub fn new(log: &Log, publisher: &Publisher, config: &ScannerConfig) -> DatabaseResult<Self> {
        let mut senders = vec![];
        let mut threads = vec![];
        for _ in 0..config.threads {
            let (sender, receiver) = channel();
            senders.push(sender);
            let line_reader = log.open_line_reader_with_index()?;
            let publisher_sender = publisher.sender().clone();
            threads.push(ScannerThread::new(line_reader, receiver, publisher_sender))
        }
        let scanner_sender = ScannerSender::new(senders, config.routing_strategy.clone());
        Ok(Scanner {
            threads: ControllableThreads::new(scanner_sender, threads)
        })
    }

    pub fn sender(&self) -> &ScannerSender {
        self.threads.sender()
    }

    pub fn sender_mut(&mut self) -> &mut ScannerSender {
        self.threads.sender_mut()
    }

    pub fn start_threads(&mut self) -> DatabaseResult<()> {
        self.threads.start()
    }

    pub fn stop_threads(&mut self) -> DatabaseResult<()> {
        self.threads.stop()
    }
}

#[derive(Clone, Debug)]
pub struct ScannerSender {
    senders: Vec<Sender<ScannerMessage>>,
    routing_strategy: RoutingStrategy
}

impl ScannerSender {
    pub fn new(senders: Vec<Sender<ScannerMessage>>, routing_strategy: RoutingStrategy) -> Self {
        ScannerSender { senders, routing_strategy }
    }

    pub fn handle_query(&mut self, query: Query) -> DatabaseResult<(SubscriptionHandle, EventStream)> {
        let (sender, receiver)  = channel();
        let subscription_handle = SubscriptionHandle::new(sender.clone());
        let subscription        = Subscription::new(sender, query);
        let updated_strategy    = self.senders.route_message(ScannerMessage::HandleSubscription(subscription), &self.routing_strategy)?;
        self.routing_strategy   = updated_strategy;
        Ok((subscription_handle, EventStream::new(receiver)))
    }

    pub fn update_index(&mut self, index: LinesIndex) -> DatabaseResult<()> {
        self.senders.broadcast_message(ScannerMessage::UpdateIndex(index))
    }
}

impl Stop for ScannerSender {
    fn stop(&self) -> DatabaseResult<()> {
        self.senders.broadcast_message(ScannerMessage::Stop)
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
    receiver: Receiver<ScannerMessage>,
    publisher_sender: PublisherSender,
    subscriptions: Vec<Subscription>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, receiver: Receiver<ScannerMessage>, publisher_sender: PublisherSender) -> ScannerThread {
        ScannerThread { reader, receiver, publisher_sender, subscriptions: vec![] }
    }

    fn forward_subscriptions_to_publisher(&mut self) {
        for subscription in self.subscriptions.drain(..) {
            let _ = self.publisher_sender.handle_subscription(subscription);
        }
    }

    fn subscriptions_intervals(&self) -> Vec<Interval<u64>> {
        self.subscriptions.iter().map(|s| s.interval()).collect()
    }

    fn scan(&mut self) -> DatabaseResult<()> {
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

impl Run<Self> for ScannerThread {
    fn run(mut self) -> Self {
        'main: loop {
            while let Ok(message) = self.receiver.recv() {
                let mut messages = vec![ message ];
                while let Ok(message) = self.receiver.try_recv() {
                    messages.push(message);
                }
                for message in messages {
                    match message {
                        ScannerMessage::HandleSubscription(subscription) => {
                            self.subscriptions.push(subscription);
                        },
                        ScannerMessage::UpdateIndex(index) => {
                            self.reader.restore_index(index);
                        },
                        ScannerMessage::Stop => break 'main
                    }
                }
                if !self.subscriptions.is_empty() {
                    match self.scan() {
                        Ok(_)    => self.forward_subscriptions_to_publisher(),
                        Err(err) => error!("Unable to scan log: {}", err)
                    }
                }
            }
        };
        self
    }
}

#[derive(Clone, Debug)]
pub enum ScannerMessage {
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

    fn setup() -> (Log, IndexedLineReader<BufReader<File>>, Publisher, ScannerConfig) {
        let log         = Log::new("", &random_collection_name(), 10).expect("Unable to create log");
        let line_reader = log.open_line_reader().expect("Unable to open line reader");
        let publisher   = Publisher::new(&PublisherConfig::default());
        let config      = ScannerConfig::default();
        (log, line_reader, publisher, config)
    }

    #[test]
    fn test_scanner_constructor() {
        let (log, _, publisher, config) = setup();

        assert!(Scanner::new(&log, &publisher, &config).is_ok());
        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_new_indexed_line() {
        let (log, line_reader, publisher, _) = setup();

        let (sender, receiver) = channel();
        let scanner_thread     = ScannerThread::new(line_reader, receiver, publisher.sender().clone());
        let handle             = thread::spawn(|| scanner_thread.run());

        let mut index = LinesIndex::new(100);
        index.insert(100, 1234);

        assert!(sender.send(ScannerMessage::UpdateIndex(index.clone())).is_ok());
        assert!(sender.send(ScannerMessage::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.reader.get_index().byte_count_at_pos(&100), Some(1234));

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_subscriptions_management() {
        let (log, _, mut publisher, config) = setup();

        let scanner        = Scanner::new(&log, &publisher, &config).expect("Unable to create scanner");
        let mut logger     = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");
        let line_reader    = log.open_line_reader().expect("Unable to open line reader");
        let event          = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(publisher.start_thread().is_ok());
        assert!(logger.log(event).is_ok());

        let (thread_sender, thread_receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, thread_receiver, publisher.sender().clone());
        thread::spawn(|| scanner_thread.run());

        let (sender, receiver) = channel();
        let live_subscription = Subscription::new(sender, Query::live());

        assert!(thread_sender.send(ScannerMessage::HandleSubscription(live_subscription.clone())).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message                      => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        let (sender, receiver) = channel();
        let current_subscription = Subscription::new(sender, Query::current());

        assert!(thread_sender.send(ScannerMessage::HandleSubscription(current_subscription)).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message                      => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
        assert_eq!(receiver.try_recv(), Ok(EventStreamMessage::End));

        assert!(log.remove().is_ok());
    }
}
