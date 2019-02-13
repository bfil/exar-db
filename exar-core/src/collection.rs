use super::*;

/// Exar DB's collection of events, containing the reference to the log and index files.
///
/// It is responsible of creating and managing the log scanner threads and the single-threaded logger.
/// It allows publishing and subscribing to the underling events log.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let collection_name = "test";
/// let collection_config = CollectionConfig::default();
/// let collection = Collection::new(collection_name, &collection_config).unwrap();
/// # }
/// ```
#[derive(Debug)]
pub struct Collection {
    log: Log,
    logger: Logger,
    publisher: Publisher,
    scanner: Scanner
}

impl Collection {
    /// Creates a new instance of a collection with the given name and configuration
    /// or a `DatabaseError` if a failure occurs.
    pub fn new(collection_name: &str, config: &CollectionConfig) -> Result<Collection, DatabaseError> {
        let log       = Log::new(&config.logs_path, collection_name, config.index_granularity)?;
        let publisher = Publisher::new(config.publisher.buffer_size);
        let scanner   = Scanner::new(&log, &publisher, &config)?;
        let logger    = Logger::new(&log, &publisher, &scanner)?;
        Ok(Collection { log, logger, publisher, scanner })
    }

    /// Publishes an event into the collection and returns the `id` for the event created
    /// or a `DatabaseError` if a failure occurs.
    pub fn publish(&mut self, event: Event) -> Result<u64, DatabaseError> {
        self.logger.log(event)
    }

    /// Subscribes to the collection of events using the given query and returns an event stream
    /// or a `DatabaseError` if a failure occurs.
    pub fn subscribe(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        self.scanner.handle_subscription(query)
    }

    /// Drops the collection and removes the log and index files.
    pub fn drop(&mut self) -> Result<(), DatabaseError> {
        self.log.remove()
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    use indexed_line_reader::LinesIndex;
    use std::sync::mpsc::channel;

    #[test]
    fn test_constructor() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, &config).expect("Unable to create collection");

        assert_eq!(collection.index, LinesIndex::new(100000));
        assert_eq!(collection.log, Log::new("", collection_name, 100000));

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
        assert_eq!(collection.publish(test_event.clone()), Ok(1));

        let query = Query::current();
        let retrieved_events: Vec<_> = collection.subscribe(query).unwrap().take(1).collect();
        let expected_event = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);
        assert_eq!(retrieved_events, vec![expected_event]);

        assert!(collection.drop().is_ok());
    }

    #[test]
    fn test_index_updates_on_publish() {
        let ref collection_name = random_collection_name();
        let mut config = CollectionConfig::default();
        config.index_granularity = 10;
        let mut collection = Collection::new(collection_name, &config).expect("Unable to create collection");

        let test_event = Event::new("data", vec!["tag1", "tag2"]);
        for i in 0..100 {
            assert_eq!(collection.publish(test_event.clone()), Ok(i+1));
        }

        let restored_index = collection.log.restore_index().expect("Unable to restore persisted index");
        assert_eq!(restored_index.line_count(), 100);

        assert!(collection.drop().is_ok());
    }

    #[test]
    fn test_drop() {
        let ref collection_name = random_collection_name();
        let config = CollectionConfig::default();
        let mut collection = Collection::new(collection_name, &config).expect("Unable to create collection");

        assert!(collection.drop().is_ok());
    }
}
