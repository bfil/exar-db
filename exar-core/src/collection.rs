use super::*;

use rand;
use rand::Rng;
use std::sync::mpsc::channel;

pub struct Collection {
    log: Log,
    scanners: Vec<Scanner>,
    routing_strategy: RoutingStrategy,
    writer: Writer
}

impl Collection {
    pub fn new(collection_name: &str, config: CollectionConfig) -> Result<Collection, DatabaseError> {
        let log = Log::new(&config.logs_path, collection_name);
        Writer::new(log.clone()).and_then(|writer| {
            let mut scanners = vec![];
            for _ in 0..config.num_scanners {
                let scanner = try!(Scanner::new(log.clone()));
                scanners.push(scanner);
            }
            Ok(Collection {
                log: log,
                scanners: scanners,
                routing_strategy: config.routing_strategy.clone(),
                writer: writer
            })
        })
    }

    pub fn publish(&self, event: Event) -> Result<usize, DatabaseError> {
        self.writer.store(event)
    }

    pub fn subscribe(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        let (send, recv) = channel();
        self.apply_strategy(Subscription::new(send, query)).and_then(|updated_strategy| {
            self.routing_strategy = updated_strategy;
            Ok(EventStream::new(recv))
        })
    }

    pub fn drop(&self) -> Result<(), DatabaseError> {
        self.drop_scanners();
        match self.log.remove() {
            Ok(()) => Ok(()),
            Err(err) => Err(DatabaseError::IoError(err))
        }
    }

    fn drop_scanners(&self) {
        for scanner in &self.scanners {
            drop(scanner);
        }
    }

    fn apply_strategy(&mut self, subscription: Subscription) -> Result<RoutingStrategy, DatabaseError> {
        match self.routing_strategy {
            RoutingStrategy::Random => match rand::thread_rng().choose(&self.scanners) {
                Some(random_scanner) => {
                    random_scanner.handle_subscription(subscription).and_then(|_| {
                        Ok(RoutingStrategy::Random)
                    })
                },
                None => Err(DatabaseError::SubscriptionError)
            },
            RoutingStrategy::RoundRobin(ref last_index) => {
                let mut new_index = 0;
                if last_index + 1 < self.scanners.len() {
                    new_index = last_index + 1;
                }
                match self.scanners.get(new_index) {
                    Some(scanner) => scanner.handle_subscription(subscription).and_then(|_| {
                        Ok(RoutingStrategy::RoundRobin(new_index))
                    }),
                    None => Err(DatabaseError::SubscriptionError)
                }
            }
        }
    }
}
