use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
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
