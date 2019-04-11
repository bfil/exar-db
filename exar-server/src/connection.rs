use exar::*;

use std::sync::{Arc, Mutex};

/// Exar DB's database connection, which contains a reference to a collection wrapped into an Arc/Mutex.
/// It allows publishing and subscribing to the underling collection of events.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use exar_server::*;
/// use std::sync::{Arc, Mutex};
///
/// let collection = Collection::new("test", &CollectionConfig::default()).expect("Unable to create collection");
/// let connection = Connection::new(Arc::new(Mutex::new(collection)));
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Connection {
    collection: Arc<Mutex<Collection>>
}

impl Connection {
    /// Creates a new instance of a connection with the given collection.
    pub fn new(collection: Arc<Mutex<Collection>>) -> Self {
        Connection { collection }
    }

    /// Publishes an event into the underlying collection and returns the `id` for the event created
    /// or a `DatabaseError` if a failure occurs.
    pub fn publish(&self, event: Event) -> DatabaseResult<u64> {
        self.collection.lock().unwrap().publish(event)
    }

    /// Subscribes to the underlying collection of events using the given query and returns a subscription
    /// or a `DatabaseError` if a failure occurs.
    pub fn subscribe(&self, query: Query) -> DatabaseResult<Subscription> {
        self.collection.lock().unwrap().subscribe(query)
    }

    /// Closes the connection.
    pub fn close(self) {
        drop(self)
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_connection() {
        let mut db              = Database::new(DatabaseConfig::default());
        let ref collection_name = random_collection_name();
        let collection          = db.collection(collection_name).expect("Unable to get collection");
        let connection          = Connection::new(collection);
        let test_event          = Event::new("data", vec!["tag1", "tag2"]);

        assert_eq!(connection.publish(test_event.clone()), Ok(1));

        let query                    = Query::current();
        let subscription             = connection.subscribe(query).expect("Unable to subscribe");
        let retrieved_events: Vec<_> = subscription.event_stream().take(1).collect();
        let expected_event           = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);

        assert_eq!(retrieved_events, vec![expected_event]);

        connection.close();

        assert!(db.drop_collection(collection_name).is_ok());
    }
}
