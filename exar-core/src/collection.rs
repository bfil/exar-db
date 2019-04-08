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
    scanner_sender: ScannerSender
}

impl Collection {
    /// Creates a new instance of a collection with the given name and configuration
    /// or a `DatabaseError` if a failure occurs.
    pub fn new(name: &str, config: &CollectionConfig) -> DatabaseResult<Collection> {
        let log            = Log::new(name, &config.data)?;
        let publisher      = Publisher::new(&config.publisher)?;
        let scanner        = Scanner::new(&log, &publisher, &config.scanner)?;
        let scanner_sender = scanner.sender().clone();
        let logger         = Logger::new(&log, &publisher, &scanner)?;
        Ok(Collection { log, logger, publisher, scanner, scanner_sender })
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
        self.scanner_sender.register_event_emitter(event_emitter)?;
        Ok(Subscription::new(sender, receiver))
    }

    /// Returns the name of the collection.
    pub fn get_name(&self) -> &str {
        self.log.get_name()
    }

    /// Drops the collection and removes the log and index files.
    pub fn delete(&self) -> DatabaseResult<()> {
        self.log.remove()
    }

    /// Flushes the collection log's buffer.
    pub fn flush(&mut self) -> DatabaseResult<()> {
        self.logger.flush()
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_constructor() {
        let collection = temp_collection();

        assert!(collection.delete().is_ok());
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
    fn test_delete() {
        let collection = temp_collection();

        assert!(collection.delete().is_ok());
        assert!(collection.log.open_reader().is_err());
    }
}
