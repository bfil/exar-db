use super::*;

use indexed_line_reader::LinesIndex;
use rand;
use rand::Rng;
use std::sync::mpsc::channel;

#[derive(Debug)]
pub struct Collection {
    index: LinesIndex,
    log: Log,
    scanners: Vec<Scanner>,
    tail_scanners: Vec<Scanner>,
    routing_strategy: RoutingStrategy,
    logger: Logger
}

impl Collection {
    pub fn new(collection_name: &str, config: &CollectionConfig) -> Result<Collection, DatabaseError> {
        let log = Log::new(&config.logs_path, collection_name, config.index_granularity);
        log.restore_index().and_then(|index| {
            Logger::new(log.clone()).and_then(|logger| {
                let (scanners, tail_scanners) = try!(Collection::run_scanners(&log, &index, &config));
                Ok(Collection {
                    index: index,
                    log: log,
                    scanners: scanners,
                    tail_scanners: tail_scanners,
                    routing_strategy: config.routing_strategy.clone(),
                    logger: logger
                })
            })
        })
    }

    pub fn publish(&mut self, event: Event) -> Result<u64, DatabaseError> {
        self.logger.log(event).and_then(|event_id| {
            if (event_id + 1) % (self.log.get_index_granularity()) == 0 {
                self.index.insert(event_id + 1, self.logger.bytes_written());
                let _ = self.log.persist_index(&self.index);
                for scanner in &self.scanners {
                    try!(scanner.add_line_index(event_id + 1, self.logger.bytes_written()))
                }
            }
            Ok(event_id)
        })
    }

    pub fn subscribe(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        let (sender, receiver) = channel();
        self.apply_routing_strategy(Subscription::new(sender, query)).and_then(|updated_strategy| {
            self.routing_strategy = updated_strategy;
            Ok(EventStream::new(receiver))
        })
    }

    pub fn drop(&mut self) -> Result<(), DatabaseError> {
        self.scanners.truncate(0);
        self.tail_scanners.truncate(0);
        self.log.remove()
    }

    fn run_scanners(log: &Log, index: &LinesIndex, config: &CollectionConfig) -> Result<(Vec<Scanner>, Vec<Scanner>), DatabaseError> {
        let mut scanners = vec![];
        let mut tail_scanners = vec![];
        for _ in 0..config.scanners.nr_of_scanners {
            let line_reader = try!(log.open_line_reader_with_index(index.clone()));
            let mut scanner = Scanner::new(line_reader, config.scanners_sleep_duration());

            let line_reader = try!(log.open_line_reader_with_index(index.clone()));
            let tail_scanner = Scanner::new(line_reader, config.scanners_sleep_duration());
            try!(scanner.set_tail_scanner_sender(tail_scanner.clone_action_sender()));

            scanners.push(scanner);
            tail_scanners.push(tail_scanner);
        }
        Ok((scanners, tail_scanners))
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
        let mut collection = Collection::new(collection_name, &config).expect("Unable to create collection");

        assert_eq!(collection.log, Log::new("", collection_name, 100000));
        assert_eq!(collection.scanners.len(), 2);
        assert_eq!(collection.routing_strategy, RoutingStrategy::default());

        assert!(collection.drop().is_ok());
    }

    #[test]
    fn test_constructor_error() {
        let ref collection_name = invalid_collection_name();
        assert!(Collection::new(collection_name, &CollectionConfig::default()).is_err());
    }

    #[test]
    fn test_publish_and_subscribe() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, &config).expect("Unable to create collection");

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
        let mut collection = Collection::new(collection_name, &config).expect("Unable to create collection");

        assert_eq!(collection.scanners.len(), 2);

        assert!(collection.drop().is_ok());

        assert_eq!(collection.scanners.len(), 0);
    }

    #[test]
    fn test_apply_round_robin_routing_strategy() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, &config)
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
