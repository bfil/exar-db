use super::*;

use indexed_line_reader::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc::Receiver;

/// Exar DB's log file scanner.
///
/// It manages event emitters and scans portions of the log file
/// and emits events to the subscriptions depending on the event emitters' query parameters.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::sync::mpsc::channel;
///
/// let log       = Log::new("test", &DataConfig::default()).expect("Unable to create log");
/// let publisher = Publisher::new(&PublisherConfig::default()).expect("Unable to create publisher");
/// let event     = Event::new("data", vec!["tag1", "tag2"]);
///
/// let line_reader = log.open_line_reader().expect("Unable to open line reader");
/// let mut scanner = Scanner::new(&log, &publisher, &ScannerConfig::default()).expect("Unable to create scanner");
///
/// let (sender, _)   = channel();
/// let event_emitter = EventEmitter::new(sender, Query::live());
/// scanner.sender().register_event_emitter(event_emitter).unwrap();
///
/// drop(scanner);
/// # }
/// ```
#[derive(Debug)]
pub struct Scanner {
    executor: MultiThreadedExecutor<ScannerSender, ScannerThread>
}

impl Scanner {
    /// Creates a new log scanner using the given `Log`, `Publisher` and `ScannerConfig`.
    pub fn new(log: &Log, publisher: &Publisher, config: &ScannerConfig) -> DatabaseResult<Self> {
        Ok(Scanner {
            executor: MultiThreadedExecutor::new(config.threads,
                |senders| ScannerSender::new(Router::new(senders, config.routing_strategy.clone())),
                |receiver| {
                    let line_reader      = log.open_line_reader_with_index()?;
                    let publisher_sender = publisher.sender().clone();
                    Ok(ScannerThread::new(line_reader, receiver, publisher_sender))
                }
            )?
        })
    }

    /// Returns a reference to the `ScannerSender`.
    pub fn sender(&self) -> &ScannerSender {
        self.executor.sender()
    }
}

#[derive(Clone, Debug)]
pub struct ScannerSender {
    router: Router<ScannerMessage>
}

impl ScannerSender {
    /// Creates a new scanner sender to interact with the scanner threads.
    pub fn new(router: Router<ScannerMessage>) -> Self {
        ScannerSender { router }
    }

    /// Registers a new event emitter with one of the publisher threads.
    pub fn register_event_emitter(&self, event_emitter: EventEmitter) -> DatabaseResult<()> {
        self.router.route_message(ScannerMessage::RegisterEventEmitter(event_emitter))
    }

    /// Updates the scanner threads' line readers' index.
    pub fn update_index(&self, index: LinesIndex) -> DatabaseResult<()> {
        self.router.broadcast_message(ScannerMessage::UpdateIndex(index))
    }
}

impl Stop for ScannerSender {
    fn stop(&self) -> DatabaseResult<()> {
        self.router.broadcast_message(ScannerMessage::Stop)
    }
}

/// Exar DB's log file scanner thread.
///
/// It uses a channel receiver to receive actions to be performed between scans,
/// and it manages the thread that scans portions of the log file
/// depending on the event emitters' query parameters.
#[derive(Debug)]
pub struct ScannerThread {
    reader: IndexedLineReader<BufReader<File>>,
    receiver: Receiver<ScannerMessage>,
    publisher_sender: PublisherSender,
    event_emitters: Vec<EventEmitter>
}

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, receiver: Receiver<ScannerMessage>, publisher_sender: PublisherSender) -> ScannerThread {
        ScannerThread { reader, receiver, publisher_sender, event_emitters: vec![] }
    }

    fn forward_event_emitters_to_publisher(&mut self) {
        for event_emitter in self.event_emitters.drain(..) {
            let _ = self.publisher_sender.register_event_emitter(event_emitter);
        }
    }

    fn event_emitters_intervals(&self) -> Vec<Interval<u64>> {
        self.event_emitters.iter().map(|s| s.interval()).collect()
    }

    fn scan(&mut self) -> DatabaseResult<()> {
        for interval in self.event_emitters_intervals().merged() {
            match self.reader.seek(SeekFrom::Start(interval.start)) {
                Ok(_) => {
                    for line in (&mut self.reader).lines() {
                        match line {
                            Ok(line) => match Event::from_tab_separated_str(&line) {
                                Ok(ref event) => {
                                    for event_emitter in self.event_emitters.iter_mut() {
                                        let _ = event_emitter.emit(event.clone());
                                    }
                                    if interval.end == event.id || self.event_emitters.iter().all(|s| !s.is_active()) {
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

impl Run for ScannerThread {
    fn run(mut self) -> Self {
        'main: loop {
            while let Ok(message) = self.receiver.recv() {
                let mut messages = vec![ message ];
                while let Ok(message) = self.receiver.try_recv() {
                    messages.push(message);
                }
                for message in messages {
                    match message {
                        ScannerMessage::RegisterEventEmitter(event_emitter) => {
                            self.event_emitters.push(event_emitter);
                        },
                        ScannerMessage::UpdateIndex(index) => {
                            self.reader.restore_index(index);
                        },
                        ScannerMessage::Stop => break 'main
                    }
                }
                if !self.event_emitters.is_empty() {
                    match self.scan() {
                        Ok(_)    => self.forward_event_emitters_to_publisher(),
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
    RegisterEventEmitter(EventEmitter),
    UpdateIndex(LinesIndex),
    Stop
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use indexed_line_reader::*;

    use std::fs::*;
    use std::io::BufReader;
    use std::sync::mpsc::{channel, TryRecvError};
    use std::thread;
    use std::time::Duration;

    fn setup() -> (Log, IndexedLineReader<BufReader<File>>, Publisher, ScannerConfig) {
        let log         = temp_log(10);
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
    fn test_scanner_constructor_failure() {
        let (log, _, publisher, mut config) = setup();
        config.threads = 0;

        assert!(Scanner::new(&log, &publisher, &config).is_err());
        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_scanner_thread_index_updates() {
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
    fn test_scanner_thread_event_emitters_management() {
        let (log, _, publisher, config) = setup();

        let scanner        = Scanner::new(&log, &publisher, &config).expect("Unable to create scanner");
        let mut logger     = Logger::new(&log, &publisher, &scanner).expect("Unable to create logger");
        let line_reader    = log.open_line_reader().expect("Unable to open line reader");
        let event          = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(logger.log(event).is_ok());

        let (thread_sender, thread_receiver) = channel();
        let scanner_thread = ScannerThread::new(line_reader, thread_receiver, publisher.sender().clone());
        thread::spawn(|| scanner_thread.run());

        let (sender, receiver)  = channel();
        let live_events_emitter = EventEmitter::new(sender, Query::live());

        assert!(thread_sender.send(ScannerMessage::RegisterEventEmitter(live_events_emitter.clone())).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_event_received(&receiver, 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        let (sender, receiver)     = channel();
        let current_events_emitter = EventEmitter::new(sender, Query::current());

        assert!(thread_sender.send(ScannerMessage::RegisterEventEmitter(current_events_emitter)).is_ok());

        assert_event_received(&receiver, 1);
        assert_end_of_event_stream_received(&receiver);

        assert!(log.remove().is_ok());
    }
}
