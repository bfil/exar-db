use super::*;

use rand;
use rand::Rng;
use std::sync::mpsc::channel;
use std::time::Duration;

#[derive(Debug)]
pub struct Collection {
    log: Log,
    live_scanner: Scanner,
    scanners: Vec<Scanner>,
    routing_strategy: RoutingStrategy,
    logger: Logger
}

impl Collection {
    pub fn new(collection_name: &str, config: CollectionConfig) -> Result<Collection, DatabaseError> {
        let log = Log::new(&config.logs_path, collection_name, config.index_granularity);
        log.compute_index().and_then(|index| {
            Logger::new(log.clone()).and_then(|logger| {
                let scanners_sleep_duration = Duration::from_millis(config.scanners_sleep_ms as u64);

                let live_scanner = try!(Scanner::new(log.clone(), scanners_sleep_duration));
                try!(live_scanner.update_index(index.clone()));

                let mut scanners = vec![];
                for _ in 0..config.scanners {
                    let mut scanner = try!(Scanner::new(log.clone(), scanners_sleep_duration));
                    try!(scanner.set_live_sender(live_scanner.clone_sender()));
                    try!(scanner.update_index(index.clone()));
                    scanners.push(scanner);
                }
                Ok(Collection {
                    log: log,
                    live_scanner: live_scanner,
                    scanners: scanners,
                    routing_strategy: config.routing_strategy.clone(),
                    logger: logger
                })
            })
        })
    }

    pub fn publish(&mut self, event: Event) -> Result<usize, DatabaseError> {
        self.logger.log(event).and_then(|event_id| {
            if event_id % 100000 == self.log.get_index_granularity() as usize - 1 {
                for scanner in &self.scanners {
                    try!(scanner.add_line_index(event_id, self.logger.bytes_written()))
                }
            }
            Ok(event_id)
        })
    }

    pub fn subscribe(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        let (send, recv) = channel();
        self.apply_routing_strategy(Subscription::new(send, query)).and_then(|updated_strategy| {
            self.routing_strategy = updated_strategy;
            Ok(EventStream::new(recv))
        })
    }

    pub fn drop(&mut self) -> Result<(), DatabaseError> {
        self.scanners.truncate(0);
        self.log.remove()
    }

    fn apply_routing_strategy(&mut self, subscription: Subscription) -> Result<RoutingStrategy, DatabaseError> {
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

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    use std::sync::mpsc::channel;

    #[test]
    fn test_constructor() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, config).expect("Unable to create collection");

        assert_eq!(collection.log, Log::new("", collection_name, 100000));
        assert_eq!(collection.scanners.len(), 2);
        assert_eq!(collection.routing_strategy, RoutingStrategy::default());

        assert!(collection.drop().is_ok());
    }

    #[test]
    fn test_constructor_error() {
        let ref collection_name = invalid_collection_name();
        assert!(Collection::new(collection_name, CollectionConfig::default()).is_err());
    }

    #[test]
    fn test_publish_and_subscribe() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, config).expect("Unable to create collection");

        let test_event = Event::new("data", vec!["tag1", "tag2"]);
        assert!(collection.publish(test_event.clone()).is_ok());

        let query = Query::current();
        let retrieved_events: Vec<_> = collection.subscribe(query).unwrap().take(1).collect();
        let expected_event = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);
        assert_eq!(retrieved_events, vec![expected_event]);

        assert!(collection.drop().is_ok());
    }

    #[test]
    fn test_drop() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, config).expect("Unable to create collection");

        assert_eq!(collection.scanners.len(), 2);

        assert!(collection.drop().is_ok());

        assert_eq!(collection.scanners.len(), 0);
    }

    #[test]
    fn test_apply_round_robin_routing_strategy() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, config)
                                        .expect("Unable to create collection");

        let (send, _) = channel();
        let subscription = Subscription::new(send, Query::current());

        collection.routing_strategy = RoutingStrategy::RoundRobin(0);

        assert_eq!(collection.scanners.len(), 2);

        let updated_strategy = collection.apply_routing_strategy(subscription.clone())
                                         .expect("Unable to apply routing strategy");

        assert_eq!(updated_strategy, RoutingStrategy::RoundRobin(1));

        collection.routing_strategy = updated_strategy;

        let updated_strategy = collection.apply_routing_strategy(subscription)
                                         .expect("Unable to apply routing strategy");

        assert_eq!(updated_strategy, RoutingStrategy::RoundRobin(0));

        assert!(collection.drop().is_ok());

        assert_eq!(collection.scanners.len(), 0);
    }
}
