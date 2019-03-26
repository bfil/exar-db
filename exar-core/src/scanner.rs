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
/// let publisher = Publisher::new(&PublisherConfig::default()).expect("Unable to create publisher");
/// let event     = Event::new("data", vec!["tag1", "tag2"]);
///
/// let line_reader = log.open_line_reader().unwrap();
/// let mut scanner = Scanner::new(&log, &publisher, &ScannerConfig::default()).expect("Unable to create scanner");
///
/// scanner.handle_query(Query::live()).unwrap();
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

    pub fn start(&mut self) -> DatabaseResult<()> {
        self.threads.start()
    }

    pub fn stop(&mut self) -> DatabaseResult<()> {
        self.threads.stop()
    }
}

#[derive(Clone, Debug)]
pub struct ScannerSender {
    senders: Vec<Sender<ScannerAction>>,
    routing_strategy: RoutingStrategy
}

impl ScannerSender {
    pub fn new(senders: Vec<Sender<ScannerAction>>, routing_strategy: RoutingStrategy) -> Self {
        ScannerSender { senders, routing_strategy }
    }

    pub fn handle_query(&mut self, query: Query) -> DatabaseResult<EventStream> {
        let (sender, receiver) = channel();
        let subscription = Subscription::new(sender, query);
        let updated_strategy = self.senders.route_action(ScannerAction::HandleSubscription(subscription), &self.routing_strategy)?;
        self.routing_strategy = updated_strategy;
        Ok(EventStream::new(receiver))
    }

    pub fn update_index(&mut self, index: LinesIndex) -> DatabaseResult<()> {
        self.senders.broadcast_action(ScannerAction::UpdateIndex(index))
    }
}

impl Stop for ScannerSender {
    fn stop(&mut self) -> DatabaseResult<()> {
        self.senders.broadcast_action(ScannerAction::Stop)
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
    publisher_sender: PublisherSender,
    subscriptions: Vec<Subscription>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, receiver: Receiver<ScannerAction>, publisher_sender: PublisherSender) -> ScannerThread {
        ScannerThread {
            reader: reader,
            action_receiver: receiver,
            publisher_sender: publisher_sender,
            subscriptions: vec![]
        }
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

    fn setup() -> (Log, IndexedLineReader<BufReader<File>>, Publisher, ScannerConfig) {
        let log         = Log::new("", &random_collection_name(), 10).expect("Unable to create log");
        let line_reader = log.open_line_reader().expect("Unable to open line reader");
        let publisher   = Publisher::new(&PublisherConfig::default()).expect("Unable to create publisher");
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
    fn test_scanner_message_passing() {
        let (log, _, publisher, config) = setup();

        let mut scanner = Scanner::new(&log, &publisher, &config).expect("Unable to create scanner");

        assert!(scanner.stop().is_ok());

        let query              = Query::live();
        let (sender, receiver) = channel();
        scanner.action_senders = vec![sender];

        assert!(scanner.handle_query(query.clone()).is_ok());

        match receiver.recv() {
            Ok(ScannerAction::HandleSubscription(s)) => {
                assert_eq!(s.is_live(), true);
            },
            _ => panic!("Expected to receive an HandleSubscription message")
        }

        let index = LinesIndex::new(100);

        assert!(scanner.update_index(index.clone()).is_ok());

        match receiver.recv() {
            Ok(ScannerAction::UpdateIndex(received_index)) => {
                assert_eq!(received_index, index);
            },
            _ => panic!("Expected to receive an UpdateIndex message")
        }

        assert!(scanner.stop().is_ok());

        match receiver.recv() {
            Ok(ScannerAction::Stop) => (),
            _                       => panic!("Expected to receive a Stop message")
        }

        drop(receiver);

        assert!(scanner.stop().is_ok());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_stop() {
        let (log, line_reader, publisher, _) = setup();

        let (sender, receiver) = channel();
        let scanner_thread     = ScannerThread::new(line_reader, receiver, publisher.clone());
        let handle             = thread::spawn(|| { scanner_thread.run() });

        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.subscriptions.len(), 0);

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_new_indexed_line() {
        let (log, line_reader, publisher, _) = setup();

        let (sender, receiver) = channel();
        let scanner_thread     = ScannerThread::new(line_reader, receiver, publisher.clone());
        let handle             = thread::spawn(|| { scanner_thread.run() });

        let mut index = LinesIndex::new(100);
        index.insert(100, 1234);

        assert!(sender.send(ScannerAction::UpdateIndex(index.clone())).is_ok());
        assert!(sender.send(ScannerAction::Stop).is_ok());

        let scanner_thread = handle.join().expect("Unable to join scanner thread");
        assert_eq!(scanner_thread.reader.get_index().byte_count_at_pos(&100), Some(1234));

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_subscriptions_management() {
        let (log, _, publisher, config) = setup();

        let scanner        = Scanner::new(&log, &publisher, &config).expect("Unable to create scanner");
        let mut logger     = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");
        let line_reader    = log.open_line_reader().expect("Unable to open line reader");
        let event          = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(logger.log(event).is_ok());

        let (thread_sender, thread_receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, thread_receiver, publisher.clone());
        thread::spawn(|| { scanner_thread.run() });

        let (sender, receiver) = channel();
        let live_subscription = Subscription::new(sender, Query::live());

        assert!(thread_sender.send(ScannerAction::HandleSubscription(live_subscription.clone())).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(receiver.try_recv().map(|e| match e {
            EventStreamMessage::Event(e) => e.id,
            message => panic!("Unexpected event stream message: {:?}", message),
        }), Ok(1));
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
        let (log, _, publisher, config) = setup();

        let mut scanner = Scanner::new(&log, &publisher, &config).expect("Unable to create scanner");

        let (sender, _)  = channel();
        let subscription = Subscription::new(sender, Query::current());

        scanner.routing_strategy = RoutingStrategy::RoundRobin(0);

        scanner.send_subscription(subscription.clone()).expect("Unable to apply routing strategy");

        assert_eq!(scanner.routing_strategy, RoutingStrategy::RoundRobin(1));

        scanner.send_subscription(subscription).expect("Unable to apply routing strategy");

        assert_eq!(scanner.routing_strategy, RoutingStrategy::RoundRobin(0));

        assert!(scanner.stop().is_ok());

        assert!(log.remove().is_ok());
    }
}
