extern crate exar;

use exar::*;

#[test]
fn test_connect() {
    let mut db = Database::new(DatabaseConfig::default());

    let collection_name = "test-connect";
    assert!(db.connect(collection_name).is_ok());
    assert!(db.contains_collection(collection_name));
    assert!(db.drop_collection(collection_name).is_ok());

    let collection_name = "missing-directory/error";
    assert!(db.connect(collection_name).is_err());
    assert!(!db.contains_collection(collection_name));
    assert!(db.drop_collection(collection_name).is_err());
}

#[test]
fn test_collection_management() {
    let mut db = Database::new(DatabaseConfig::default());

    let collection_name = "test-get-collection";
    assert!(!db.contains_collection(collection_name));
    assert!(db.get_collection(collection_name).is_ok());
    assert!(db.contains_collection(collection_name));

    assert!(db.get_collection(collection_name).is_ok());
    assert!(db.contains_collection(collection_name));

    assert!(db.drop_collection(collection_name).is_ok());
    assert!(!db.contains_collection(collection_name));
}
