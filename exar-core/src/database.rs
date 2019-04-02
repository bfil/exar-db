use super::*;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
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
/// let connection      = db.connect(collection_name).unwrap();
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
        Database { config, collections: HashMap::new() }
    }

    /// Returns a connection instance with the given name or a `DatabaseError` if a failure occurs.
    pub fn connect(&mut self, collection_name: &str) -> DatabaseResult<Connection> {
        self.collection(collection_name).map(|collection| Connection::new(collection))
    }

    /// Returns an existing collection instance with the given name wrapped into an `Arc`/`Mutex`
    /// or a `DatabaseError` if a failure occurs, it initializes a new collection if it does not exist yet.
    pub fn collection(&mut self, collection_name: &str) -> DatabaseResult<Arc<Mutex<Collection>>> {
        let collection_config = self.config.collection_config(collection_name);
        match self.collections.entry(collection_name.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry)   => {
                                          let collection = Collection::new(collection_name, &collection_config)?;
                                          let shared_collection = Arc::new(Mutex::new(collection));
                                          entry.insert(shared_collection.clone());
                                          Ok(shared_collection)
                                      }
        }
    }

    /// Drops the collection with the given name or returns an error if a failure occurs.
    pub fn drop_collection(&mut self, collection_name: &str) -> DatabaseResult<()> {
        match self.collections.entry(collection_name.to_owned()) {
            Entry::Occupied(entry) => {
                                          let shared_collection = entry.remove();
                                          let mut collection = shared_collection.lock().unwrap();
                                          (*collection).drop()
                                      },
            Entry::Vacant(_)       => Err(DatabaseError::CollectionNotFound)
        }
    }

    /// Returns all active collections.
    pub fn collections(&self) -> &HashMap<String, Arc<Mutex<Collection>>> {
        &self.collections
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
        assert!(db.collections.contains_key(collection_name));
        assert!(db.drop_collection(collection_name).is_ok());
    }

    #[test]
    fn test_connection_failure() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = invalid_collection_name();
        assert!(db.connect(collection_name).is_err());
        assert!(!db.collections.contains_key(collection_name));
        assert!(db.drop_collection(collection_name).is_err());
    }

    #[test]
    fn test_collection_management() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = random_collection_name();
        assert!(!db.collections.contains_key(collection_name));
        assert!(db.collection(collection_name).is_ok());
        assert!(db.collections.contains_key(collection_name));
        assert_eq!(db.collections.len(), 1);

        assert!(db.collection(collection_name).is_ok());
        assert!(db.collections.contains_key(collection_name));
        assert_eq!(db.collections.len(), 1);

        assert!(db.drop_collection(collection_name).is_ok());
        assert!(!db.collections.contains_key(collection_name));
        assert_eq!(db.collections.len(), 0);
    }
}
