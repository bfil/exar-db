use super::*;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex, Weak};

/// Exar DB's main component, containing the database configuration and the references to the
/// collections of events created.
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
/// let collection_name   = "test";
/// let shared_collection = db.collection(collection_name).expect("Unable to retrieve collection");
/// let mut collection    = shared_collection.lock().unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Database {
    config: DatabaseConfig,
    collections: HashMap<String, Weak<Mutex<Collection>>>
}

impl Database {
    /// Creates a new instance of the database with the given configuration.
    pub fn new(config: DatabaseConfig) -> Self {
        Database { config, collections: HashMap::new() }
    }

    /// Returns an existing collection instance with the given name wrapped into an `Arc`/`Mutex`
    /// or a `DatabaseError` if a failure occurs, it initializes a new collection if it does not exist yet.
    pub fn collection(&mut self, collection_name: &str) -> DatabaseResult<Arc<Mutex<Collection>>> {
        match self.collections.entry(collection_name.to_owned()) {
            Entry::Occupied(mut entry) =>
                match entry.get_mut().upgrade() {
                    Some(collection) => Ok(collection.clone()),
                    None             => Database::create_collection(&self.config, collection_name).and_then(|collection| {
                                            entry.insert(Arc::downgrade(&collection));
                                            Ok(collection)
                                        })
                },
            Entry::Vacant(entry)     => Database::create_collection(&self.config, collection_name).and_then(|collection| {
                                            entry.insert(Arc::downgrade(&collection));
                                            Ok(collection)
                                        })
        }
    }

    fn create_collection(config: &DatabaseConfig, collection_name: &str) -> DatabaseResult<Arc<Mutex<Collection>>> {
        let collection_config = config.collection_config(collection_name);
        let collection        = Collection::new(collection_name, &collection_config)?;
        Ok(Arc::new(Mutex::new(collection)))
    }

    /// Drops the collection with the given name or returns an error if a failure occurs.
    pub fn delete_collection(&mut self, collection_name: &str) -> DatabaseResult<()> {
        let collection = self.collection(collection_name)?;
        collection.lock().unwrap().delete()?;
        self.collections.remove(collection_name);
        Ok(())
    }

    /// Attempts to flush buffer data to disk for all active collections.
    pub fn flush_collections(&self) {
        for collection in self.collections.values() {
            if let Some(collection) = collection.upgrade() {
                let mut collection = collection.lock().unwrap();
                match collection.flush() {
                    Ok(_)    => (),
                    Err(err) => warn!("Unable to flush data to log file for collection '{}': {}", collection.get_name(), err)
                }
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
        let db = Database::new(DatabaseConfig::default());

        assert_eq!(db.config, DatabaseConfig::default());
        assert_eq!(db.collections.len(), 0);
    }

    #[test]
    fn test_connect() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = random_collection_name();
        assert!(db.collection(collection_name).is_ok());
        assert!(db.collections.contains_key(collection_name));
        assert!(db.delete_collection(collection_name).is_ok());
    }

    #[test]
    fn test_connection_failure() {
        let mut db = Database::new(DatabaseConfig::default());

        let ref collection_name = invalid_collection_name();
        assert!(db.collection(collection_name).is_err());
        assert!(!db.collections.contains_key(collection_name));
        assert!(db.delete_collection(collection_name).is_err());
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

        assert!(db.delete_collection(collection_name).is_ok());
        assert!(!db.collections.contains_key(collection_name));
        assert_eq!(db.collections.len(), 0);
    }
}
