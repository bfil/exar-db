use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Database {
    config: DatabaseConfig,
    collections: HashMap<String, Arc<Mutex<Collection>>>
}

impl Database {
    pub fn new(config: DatabaseConfig) -> Database {
        Database {
            config: config,
            collections: HashMap::new()
        }
    }

    pub fn connect(&mut self, collection_name: &str) -> Result<Connection, DatabaseError> {
        match self.get_collection(collection_name) {
            Ok(collection) => Ok(Connection::new(collection)),
            Err(err) => Err(err)
        }
    }

    pub fn get_collection(&mut self, collection_name: &str) -> Result<Arc<Mutex<Collection>>, DatabaseError> {
        if !self.contains_collection(collection_name) {
            self.create_collection(collection_name)
        } else {
            match self.collections.get(collection_name) {
                Some(collection) => Ok(collection.clone()),
                None => unreachable!()
            }
        }
    }

    pub fn create_collection(&mut self, collection_name: &str) -> Result<Arc<Mutex<Collection>>, DatabaseError> {
        let collection_config = self.config.get_collection_config(collection_name);
        Collection::new(collection_name, collection_config).and_then(|collection| {
            let collection = Arc::new(Mutex::new(collection));
            self.collections.insert(collection_name.to_owned(), collection.clone());
            Ok(collection)
        })
    }

    pub fn drop_collection(&mut self, collection_name: &str) -> Result<(), DatabaseError> {
        self.get_collection(collection_name).and_then(|collection| {
            (*collection.lock().unwrap()).drop().and_then(|_| {
                self.collections.remove(collection_name);
                Ok(())
            })
        })
    }

    pub fn contains_collection(&self, collection_name: &str) -> bool {
        self.collections.contains_key(collection_name)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn basic_test() {
        let config = DatabaseConfig::default();

        let mut db = Database::new(config);
        db.drop_collection("test").expect("Unable to drop collection");
        db.create_collection("test").expect("Unable to create collection");

        let test_event = Event::new("1 2 3 4 5 6 7 8", vec!["tag1", "tag2"]);

        let conn = db.connect("test").unwrap();
        assert!(conn.publish(test_event.clone()).is_ok());

        let query = Query::current();
        let retrieved_events: Vec<_> = conn.subscribe(query).unwrap().map(|e| e.unwrap()).take(1).collect();
        let expected_events: Vec<_> = vec![test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp)];
        assert_eq!(retrieved_events, expected_events);

        assert!(db.drop_collection("test").is_ok());
    }
}
