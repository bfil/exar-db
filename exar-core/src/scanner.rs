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
    log: Log,
    send: Arc<Mutex<Sender<Subscription>>>,
    running: Arc<Mutex<bool>>,
    sleep_duration: Duration,
    subscriptions: Arc<Mutex<Vec<Subscription>>>
}

impl Scanner {
    pub fn new(log: Log, sleep_duration: Duration) -> Result<Scanner, DatabaseError> {
        let (send, recv) = channel();
        let scanner = Scanner {
            log: log,
            send: Arc::new(Mutex::new(send)),
            running: Arc::new(Mutex::new(true)),
            sleep_duration: sleep_duration,
            subscriptions: Arc::new(Mutex::new(vec![]))
        };
        scanner.run_thread(recv).and_then(|_| Ok(scanner))
    }

    pub fn handle_subscription(&self, subscription: Subscription) -> Result<(), DatabaseError> {
        match self.send.lock().unwrap().send(subscription) {
            Ok(()) => Ok(()),
            Err(_) => Err(DatabaseError::EventStreamError(EventStreamError::Closed))
        }
    }

    fn run_thread(&self, recv: Receiver<Subscription>) -> Result<JoinHandle<()>, DatabaseError> {
        let running = self.running.clone();
        let subscriptions = self.subscriptions.clone();
        let sleep_duration = self.sleep_duration.clone();
        match self.log.open_reader() {
            Ok(mut file) => {
                Ok(thread::spawn(move || {
                    while *running.lock().unwrap() {
                        let mut subscriptions = subscriptions.lock().unwrap();
                        while let Ok(subscription) = recv.try_recv() {
                            subscriptions.push(subscription);
                        }
                        if subscriptions.len() != 0 {
                            match Scanner::scan(&mut file, &mut subscriptions) {
                                Ok(_) => subscriptions.retain(|s| {
                                    s.is_active() && s.query.is_live() && s.query.is_active()
                                }),
                                Err(err) => println!("Unable to scan file: {}", err)
                            }
                        }
                        thread::sleep(sleep_duration);
                    };
                }))
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    fn scan(mut file: &mut File, subscriptions: &mut Vec<Subscription>) -> Result<(), DatabaseError> {
        let offset = subscriptions.iter().map(|s| s.query.position).min().unwrap_or(0);
        match file.seek(SeekFrom::Start(0)) {
            Ok(_) => {
                for line in BufReader::new(file).lines().skip(offset) {
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

        assert_eq!(scanner.log, log);
        assert_eq!(*scanner.running.lock().unwrap(), true);
        assert_eq!(scanner.sleep_duration, sleep_duration);
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
        let writer = Writer::new(log.clone()).expect("Unable to create writer");
        let event = Event::new("data", vec!["tag1", "tag2"]);
        let sleep_duration = Duration::from_millis(10);

        assert!(writer.store(event).is_ok());

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
