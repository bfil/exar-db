use super::*;

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
    pub fn new(log: Log, sleep_duration: Duration) -> Result<Scanner, DatabaseError> {
        let (send, recv) = channel();
        log.open_line_reader().and_then(|mut reader| {
            match reader.update_index() {
                Ok(_) => {
                    ScannerThread::new(reader, recv, sleep_duration).run();
                    Ok(Scanner {
                        send: send
                    })
                },
                Err(err) => Err(DatabaseError::new_io_error(err))
            }
        })
    }

    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        match self.send.send(ScannerAction::HandleSubscription(subscription)) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
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

impl ScannerThread {
    fn new(reader: IndexedLineReader<BufReader<File>>, recv: Receiver<ScannerAction>,
        sleep_duration: Duration) -> ScannerThread {
        ScannerThread {
            reader: reader,
            recv: recv,
            sleep_duration: sleep_duration,
            subscriptions: vec![]
        }
    }

    fn run(mut self) -> JoinHandle<()> {
        thread::spawn(move || {
            'main: loop {
                while let Ok(action) = self.recv.try_recv() {
                    match action {
                        ScannerAction::HandleSubscription(subscription) => self.subscriptions.push(subscription),
                        ScannerAction::Stop => break 'main
                    }
                }
                if self.subscriptions.len() != 0 {
                    match self.scan() {
                        Ok(_) => self.retain_active_subscriptions(),
                        Err(err) => println!("Unable to scan log: {}", err)
                    }
                }
                thread::sleep(self.sleep_duration);
            };
            self.subscriptions.truncate(0);
        })
    }

    fn retain_active_subscriptions(&mut self) {
        self.subscriptions.retain(|s| {
            s.is_active() && s.query.is_live() && s.query.is_active()
        })
    }

    fn find_min_offset(&self) -> u64 {
        self.subscriptions.iter().map(|s| s.query.position).min().unwrap_or(0) as u64
    }

    fn scan(&mut self) -> Result<(), DatabaseError> {
        let offset = self.find_min_offset();
        match self.reader.seek(SeekFrom::Start(offset)) {
            Ok(_) => {
                for line in (&mut self.reader).lines() {
                    match line {
                        Ok(line) => match Event::from_tab_separated_str(&line) {
                            Ok(ref event) => {
                                for subscription in self.subscriptions.iter_mut().filter(|s| s.matches_event(event)) {
                                    let _ = subscription.emit(event.clone());
                                }
                            },
                            Err(err) => println!("Unable to deserialize log line: {}", err)
                        },
                        Err(err) => println!("Unable to read log line: {}", err)
                    }
                }
                Ok(())
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }
}

#[derive(Clone, Debug)]
pub enum ScannerAction {
    HandleSubscription(Subscription),
    Stop
}

#[derive(Debug)]
pub struct ScannerThread {
    reader: IndexedLineReader<BufReader<File>>,
    recv: Receiver<ScannerAction>,
    sleep_duration: Duration,
    subscriptions: Vec<Subscription>
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;

    fn create_log() -> Log {
        let ref collection_name = testkit::gen_collection_name();
        let log = Log::new("", collection_name);
        assert!(log.open_writer().is_ok());
        log
    }

    #[test]
    fn test_constructor() {
        let log = create_log();
        let sleep_duration = Duration::from_millis(10);

        assert!(Scanner::new(log.clone(), sleep_duration).is_ok());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        let ref collection_name = testkit::gen_collection_name();
        let log = Log::new("", collection_name);
        let sleep_duration = Duration::from_millis(10);

        assert!(Scanner::new(log.clone(), sleep_duration).is_err());

        assert!(log.remove().is_err());
    }

    #[test]
    fn test_message_passing() {
        let log = create_log();
        let sleep_duration = Duration::from_millis(10);

        let mut scanner = Scanner::new(log.clone(), sleep_duration).expect("Unable to run scanner");

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
    fn test_stop() {
        let log = create_log();
        let sleep_duration = Duration::from_millis(10);

        let scanner = Scanner::new(log.clone(), sleep_duration).expect("Unable to run scanner");

        assert!(scanner.stop().is_ok());

        assert!(log.remove().is_ok());
    }

    #[test]
    fn test_subscriptions_management() {
        let log = create_log();
        let mut logger = Logger::new(log.clone()).expect("Unable to create logger");
        let event = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(logger.log(event).is_ok());

        let scanner = Scanner::new(log.clone(), sleep_duration).expect("Unable to run scanner");

        let (send, recv) = channel();
        let live_subscription = Subscription::new(send, Query::live());

        assert!(scanner.handle_subscription(live_subscription).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(recv.try_recv().map(|e| e.id), Ok(1));

        let (send, recv) = channel();
        let current_subscription = Subscription::new(send, Query::current());

        assert!(scanner.handle_subscription(current_subscription).is_ok());
        thread::sleep(sleep_duration * 2);

        assert_eq!(recv.try_recv().map(|e| e.id), Ok(1));

        assert!(log.remove().is_ok());
    }
}
