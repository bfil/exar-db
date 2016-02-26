use super::*;

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Scanner {
    send: Sender<Subscription>,
    running: Arc<Mutex<bool>>,
    subscriptions: Arc<Mutex<Vec<Subscription>>>
}

impl Scanner {
    pub fn new(log: Log, sleep_duration: Duration) -> Result<Scanner, DatabaseError> {
        let (send, recv) = channel();
        let running = Arc::new(Mutex::new(true));
        let subscriptions = Arc::new(Mutex::new(vec![]));
        log.open_reader().and_then(|reader| {
            ScannerThread::run(reader, recv, sleep_duration, running.clone(), subscriptions.clone());
            Ok(Scanner {
                send: send,
                running: running,
                subscriptions: subscriptions
            })
        })
    }

    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        match self.send.send(subscription) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    fn stop(&self) {
        let mut running = self.running.lock().unwrap();
        *running = false;
        let mut subscriptions = self.subscriptions.lock().unwrap();
        subscriptions.truncate(0);
    }
}

impl Drop for Scanner {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct ScannerThread;

impl ScannerThread {
    fn run(mut reader: BufReader<File>, recv: Receiver<Subscription>, sleep_duration: Duration,
           running: Arc<Mutex<bool>>, subscriptions: Arc<Mutex<Vec<Subscription>>>) -> JoinHandle<()> {
        thread::spawn(move || {
            while *running.lock().unwrap() {
                let mut subscriptions = subscriptions.lock().unwrap();
                while let Ok(subscription) = recv.try_recv() {
                    subscriptions.push(subscription);
                }
                if subscriptions.len() != 0 {
                    match ScannerThread::scan(&mut reader, &mut subscriptions) {
                        Ok(_) => subscriptions.retain(|s| {
                            s.is_active() && s.query.is_live() && s.query.is_active()
                        }),
                        Err(err) => println!("Unable to scan file: {}", err)
                    }
                }
                thread::sleep(sleep_duration);
            };
        })
    }

    fn scan(mut reader: &mut BufReader<File>, subscriptions: &mut Vec<Subscription>) -> Result<(), DatabaseError> {
        let offset = subscriptions.iter().map(|s| s.query.position).min().unwrap_or(0);
        match reader.seek(SeekFrom::Start(0)) {
            Ok(_) => {
                for line in reader.lines().skip(offset) {
                    match line {
                        Ok(line) => {
                            match Event::from_tab_separated_str(&line) {
                                Ok(ref event) => {
                                    for subscription in subscriptions.iter_mut().filter(|s| {
                                        s.is_active() && s.query.is_active() && s.query.matches(event)
                                    }) {
                                        let _ = subscription.emit(event.clone());
                                    }
                                },
                                Err(err) => println!("Unable to deserialize database line: {}", err)
                            }
                        },
                        Err(err) => println!("Unable to read database line: {}", err)
                    };
                }
                Ok(())
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }
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

        let scanner = Scanner::new(log.clone(), sleep_duration).expect("Unable to run scanner");

        assert_eq!(*scanner.running.lock().unwrap(), true);
        assert_eq!(scanner.subscriptions.lock().unwrap().len(), 0);

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
    fn test_stop() {
        let log = create_log();
        let sleep_duration = Duration::from_millis(10);

        let scanner = Scanner::new(log.clone(), sleep_duration).expect("Unable to run scanner");

        assert_eq!(*scanner.running.lock().unwrap(), true);

        let (send, _) = channel();
        let subscription = Subscription::new(send, Query::live());

        assert!(scanner.handle_subscription(subscription).is_ok());
        thread::sleep(sleep_duration * 2);

        let subscriptions_len = scanner.subscriptions.lock().unwrap().len();
        assert_eq!(subscriptions_len, 1);

        scanner.stop();

        let subscriptions_len = scanner.subscriptions.lock().unwrap().len();
        assert_eq!(subscriptions_len, 0);

        assert_eq!(*scanner.running.lock().unwrap(), false);

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

        let subscriptions_len = scanner.subscriptions.lock().unwrap().len();
        assert_eq!(subscriptions_len, 1);

        let (send, recv) = channel();
        let current_subscription = Subscription::new(send, Query::current());

        assert!(scanner.handle_subscription(current_subscription).is_ok());
        thread::sleep(sleep_duration * 2);

        let subscriptions_len = scanner.subscriptions.lock().unwrap().len();
        assert_eq!(subscriptions_len, 1);

        assert_eq!(recv.try_recv().map(|e| e.id), Ok(1));

        assert!(log.remove().is_ok());
    }
}
