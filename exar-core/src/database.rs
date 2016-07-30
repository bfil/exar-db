use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Exar DB's main component, containing the database configuration and the references to the
/// collections of events created. It is used to create new connections.
///
/// # Examples
/// ```no_run
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = DatabaseConfig::default();
/// let mut db = Database::new(config);
///
/// let collection_name = "test";
/// let connection = db.connect(collection_name).unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Database {
    config: DatabaseConfig,
    collections: HashMap<String, Arc<Mutex<Collection>>>
}

impl Database {
    /// Creates a new instance of the database with the given configuration.
    pub fn new(config: DatabaseConfig) -> Database {
        Database {
            config: config,
            collections: HashMap::new()
        }
    }

    /// Returns a connection instance with the given name or a `DatabaseError` if a failure occurs.
    pub fn connect(&mut self, collection_name: &str) -> Result<Connection, DatabaseError> {
        match self.get_collection(collection_name) {
            Ok(collection) => Ok(Connection::new(collection)),
            Err(err) => Err(err)
        }
    }

    /// Returns an existing collection instance with the given name wrapped into an `Arc`/`Mutex`
    /// or a `DatabaseError` if a failure occurs, it creates a new collection if it does not exist.
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

    /// Creates and returns a new collection instance with the given name wrapped into an `Arc`/`Mutex`
    /// or a `DatabaseError` if a failure occurs.
    pub fn create_collection(&mut self, collection_name: &str) -> Result<Arc<Mutex<Collection>>, DatabaseError> {
        let collection_config = self.config.collection_config(collection_name);
        Collection::new(collection_name, &collection_config).and_then(|collection| {
            let collection = Arc::new(Mutex::new(collection));
            self.collections.insert(collection_name.to_owned(), collection.clone());
            Ok(collection)
        })
    }

    /// Drops the collection with the given name or returns an error if a failure occurs.
    pub fn drop_collection(&mut self, collection_name: &str) -> Result<(), DatabaseError> {
        self.get_collection(collection_name).and_then(|collection| {
            (*collection.lock().unwrap()).drop().and_then(|_| {
                self.collections.remove(collection_name);
                Ok(())
            })
        })
    }

    /// Returns wether a collection with the given name exists.
    pub fn contains_collection(&self, collection_name: &str) -> bool {
        self.collections.contains_key(collection_name)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use exar_testkit::*;

    #[test]
    fn test_constructor() {
        let db = Database::new(DatabaseConfig::default());

        assert_eq!(db.config, DatabaseConfig::default());
        assert_eq!(db.collections.len(), 0);
    }

    #[test]
    fn test_connect() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = random_collection_name();
        assert!(db.connect(collection_name).is_ok());
        assert!(db.contains_collection(collection_name));
        assert!(db.drop_collection(collection_name).is_ok());
    }

    #[test]
    fn test_connection_failure() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = invalid_collection_name();
        assert!(db.connect(collection_name).is_err());
        assert!(!db.contains_collection(collection_name));
        assert!(db.drop_collection(collection_name).is_err());
    }

    #[test]
    fn test_collection_management() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = random_collection_name();
        assert!(!db.contains_collection(collection_name));
        assert!(db.get_collection(collection_name).is_ok());
        assert!(db.contains_collection(collection_name));
        assert_eq!(db.collections.len(), 1);

        assert!(db.get_collection(collection_name).is_ok());
        assert!(db.contains_collection(collection_name));
        assert_eq!(db.collections.len(), 1);

        assert!(db.drop_collection(collection_name).is_ok());
        assert!(!db.contains_collection(collection_name));
        assert_eq!(db.collections.len(), 0);
    }
}
