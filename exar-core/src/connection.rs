use super::*;

use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Connection {
    collection: Arc<Mutex<Collection>>
}

impl Connection {
    pub fn new(collection: Arc<Mutex<Collection>>) -> Connection {
        Connection {
            collection: collection
        }
    }

    pub fn publish(&self, event: Event) -> Result<usize, DatabaseError> {
        self.collection.lock().unwrap().publish(event)
    }

    pub fn subscribe(&self, query: Query) -> Result<EventStream, DatabaseError> {
        self.collection.lock().unwrap().subscribe(query)
    }

    pub fn close(self) {
        drop(self)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_connection() {
        let mut db = Database::new(DatabaseConfig::default());

        let collection_name = "test-connection";
        let collection = db.get_collection(collection_name).expect("Unable to get collection");

        let conn = Connection::new(collection);

        let test_event = Event::new("data", vec!["tag1", "tag2"]);
        assert!(conn.publish(test_event.clone()).is_ok());

        let query = Query::current();
        let retrieved_events: Vec<_> = conn.subscribe(query).unwrap().map(|e| e.unwrap()).take(1).collect();
        let expected_event = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);
        assert_eq!(retrieved_events, vec![expected_event]);

        conn.close();

        assert!(db.drop_collection(collection_name).is_ok());
        assert!(!db.contains_collection(collection_name));
    }
}
