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
/// let collection_name   = "test";
/// let collection_config = CollectionConfig::default();
/// let collection        = Collection::new(collection_name, &collection_config).unwrap();
/// # }
/// ```
#[derive(Debug)]
pub struct Collection {
    log: Log,
    logger: Logger,
    publisher: Publisher,
    scanner: Scanner,
    scanner_sender: ScannerSender,
    connections_count: u16
}

impl Collection {
    /// Creates a new instance of a collection with the given name and configuration
    /// or a `DatabaseError` if a failure occurs.
    pub fn new(name: &str, config: &CollectionConfig) -> DatabaseResult<Collection> {
        let log            = Log::new(&config.logs_path, name, config.index_granularity)?;
        let publisher      = Publisher::new(&config.publisher);
        let scanner        = Scanner::new(&log, &publisher, &config.scanner)?;
        let scanner_sender = scanner.sender().clone();
        let logger         = Logger::new(&log, &publisher, &scanner)?;
        Ok(Collection { log, logger, publisher, scanner, scanner_sender, connections_count: 0 })
    }

    /// Publishes an event into the collection and returns the `id` for the event created
    /// or a `DatabaseError` if a failure occurs.
    pub fn publish(&mut self, event: Event) -> DatabaseResult<u64> {
        self.logger.log(event)
    }

    /// Subscribes to the collection of events using the given query and returns an event stream
    /// or a `DatabaseError` if a failure occurs.
    pub fn subscribe(&mut self, query: Query) -> DatabaseResult<(SubscriptionHandle, EventStream)> {
        self.scanner_sender.handle_query(query)
    }

    /// Returns the name of the collection.
    pub fn get_name(&self) -> &str {
        self.log.get_name()
    }

    /// Drops the collection and removes the log and index files.
    pub fn drop(&mut self) -> DatabaseResult<()> {
        self.log.remove()
    }

    /// Flushes the collection log's buffer.
    pub fn flush(&mut self) -> DatabaseResult<()> {
        self.logger.flush()
    }

    pub fn start_threads(&mut self) -> DatabaseResult<Vec<()>> {
        vec![
            self.publisher.start_thread(),
            self.scanner.start_threads()
        ].into_iter().collect()
    }

    pub fn stop_threads(&mut self) -> DatabaseResult<()> {
        vec![
            self.publisher.stop_thread(),
            self.scanner.stop_threads()
        ].into_iter().collect()
    }

    pub fn increase_connections_count(&mut self) {
        if self.connections_count == 0 {
            match self.start_threads() {
                Ok(_)    => (),
                Err(err) => warn!("Unable to start '{}' collection threads: {}", self.get_name(), err)
            }
        }
        self.connections_count += 1;
    }

    pub fn decrease_connections_count(&mut self) {
        self.connections_count -= 1;
        if self.connections_count == 0 {
            match self.stop_threads() {
                Ok(_)    => (),
                Err(err) => warn!("Unable to stop '{}' collection threads: {}", self.get_name(), err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    #[test]
    fn test_constructor() {
        let ref collection_name = random_collection_name();
        let config              = CollectionConfig::default();
        let mut collection      = Collection::new(collection_name, &config).expect("Unable to create collection");

        assert_eq!(collection.log.get_path(), format!("{}.log", collection_name));
        assert_eq!(collection.log.get_name(), collection_name);
        assert_eq!(collection.log.get_index_granularity(), 100000);

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
        let config              = CollectionConfig::default();
        let mut collection      = Collection::new(collection_name, &config).expect("Unable to create collection");
        let test_event          = Event::new("data", vec!["tag1", "tag2"]);

        collection.start_threads().expect("Unable to start collection threads");

        assert_eq!(collection.publish(test_event.clone()), Ok(1));

        let query                    = Query::current();
        let (_, event_stream)        = collection.subscribe(query).expect("Unable to subscribe");
        let retrieved_events: Vec<_> = event_stream.take(1).collect();
        let expected_event           = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);

        assert_eq!(retrieved_events, vec![expected_event]);

        assert!(collection.drop().is_ok());
    }

    #[test]
    fn test_drop() {
        let ref collection_name = random_collection_name();
        let config              = CollectionConfig::default();
        let mut collection      = Collection::new(collection_name, &config).expect("Unable to create collection");

        assert!(collection.drop().is_ok());
        assert!(collection.log.open_reader().is_err());
    }
}
