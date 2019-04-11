use super::*;

use std::sync::mpsc::channel;

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
/// let mut collection = Collection::new("test", &CollectionConfig::default()).expect("Unable to create the collection");
///
/// collection.publish(Event::new("data", vec!["tag1", "tag2"])).expect("Unable to publish event");
///
/// let subscription   = collection.subscribe(Query::current()).expect("Unable to subscribe");
/// let events: Vec<_> = subscription.event_stream().take(1).collect();
///
/// collection.truncate().expect("Unable to truncate the collection");
/// # }
/// ```
#[derive(Debug)]
pub struct Collection {
    name: String,
    config: CollectionConfig,
    log: Log,
    logger: Logger,
    publisher: Publisher,
    scanner: Scanner
}

impl Collection {
    /// Creates a new instance of a collection with the given name and configuration
    /// or a `DatabaseError` if a failure occurs.
    pub fn new(name: &str, config: &CollectionConfig) -> DatabaseResult<Collection> {
        let name           = name.to_owned();
        let config         = config.clone();
        let log            = Log::new(&name, &config.data)?;
        let publisher      = Publisher::new(&config.publisher)?;
        let scanner        = Scanner::new(&log, &publisher, &config.scanner)?;
        let logger         = Logger::new(&log, &publisher, &scanner)?;
        Ok(Collection { name, config, log, logger, publisher, scanner })
    }

    /// Publishes an event into the collection and returns the `id` for the event created
    /// or a `DatabaseError` if a failure occurs.
    pub fn publish(&mut self, event: Event) -> DatabaseResult<u64> {
        self.logger.log(event)
    }

    /// Subscribes to the collection of events using the given query and returns a subscription
    /// or a `DatabaseError` if a failure occurs.
    pub fn subscribe(&self, query: Query) -> DatabaseResult<Subscription> {
        let (sender, receiver) = channel();
        let event_emitter      = EventEmitter::new(sender.clone(), query);
        self.scanner.sender().register_event_emitter(event_emitter)?;
        Ok(Subscription::new(sender, receiver))
    }

    /// Returns the name of the collection.
    pub fn get_name(&self) -> &str {
        self.log.get_name()
    }

    fn reset(&mut self) -> DatabaseResult<()> {
        self.log       = Log::new(&self.name, &self.config.data)?;
        self.publisher = Publisher::new(&self.config.publisher)?;
        self.scanner   = Scanner::new(&self.log, &self.publisher, &self.config.scanner)?;
        self.logger    = Logger::new(&self.log, &self.publisher, &self.scanner)?;
        Ok(())
    }

    /// Truncates the collection by removing the log and index files and resetting its internal state.
    pub fn truncate(&mut self) -> DatabaseResult<()> {
        self.log.remove()?;
        self.reset()
    }

    /// Flushes the collection log's buffer.
    pub fn flush(&mut self) -> DatabaseResult<()> {
        self.logger.flush()
    }
}

impl Drop for Collection {
    fn drop(&mut self) {
        if self.logger.bytes_written() == 0 {
            let _ = self.log.remove();
        }
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_constructor() {
        let mut collection = temp_collection();

        assert!(collection.truncate().is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        assert!(Collection::new(&invalid_collection_name(), &temp_collection_config()).is_err());
    }

    #[test]
    fn test_publish_and_subscribe() {
        let mut collection = temp_collection();
        let test_event     = Event::new("data", vec!["tag1", "tag2"]);

        assert_eq!(collection.publish(test_event.clone()), Ok(1));

        let query                    = Query::current();
        let subscription             = collection.subscribe(query).expect("Unable to subscribe");
        let retrieved_events: Vec<_> = subscription.event_stream().take(1).collect();
        let expected_event           = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);

        assert_eq!(retrieved_events, vec![expected_event]);
    }

    #[test]
    fn test_truncate() {
        let mut collection = temp_collection();
        let test_event     = Event::new("data", vec!["tag1", "tag2"]);

        assert_eq!(collection.publish(test_event.clone()), Ok(1));

        let query                    = Query::current();
        let subscription             = collection.subscribe(query).expect("Unable to subscribe");
        let retrieved_events: Vec<_> = subscription.event_stream().collect();
        assert_eq!(retrieved_events.len(), 1);

        assert!(collection.truncate().is_ok());

        let query                    = Query::current();
        let subscription             = collection.subscribe(query).expect("Unable to subscribe");
        let retrieved_events: Vec<_> = subscription.event_stream().collect();
        assert_eq!(retrieved_events.len(), 0);

        let log = collection.log.clone();
        drop(collection);

        assert!(log.open_reader().is_err());
    }
}
