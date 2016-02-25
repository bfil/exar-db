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
                                Ok(event) => {
                                    for subscription in subscriptions.iter_mut() {
                                        if subscription.query.is_active() && subscription.query.matches(&event) {
                                            let _ = subscription.emit(event.clone());
                                        }
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
