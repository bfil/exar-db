extern crate exar;

use exar::*;

#[test]
fn basic_test() {
    let config = DatabaseConfig::default();

    let mut db = Database::new(config);

    db.drop_collection("test").expect("Unable to drop collection");
    assert!(!db.contains_collection("test"));

    db.create_collection("test").expect("Unable to create collection");
    assert!(db.contains_collection("test"));

    let test_event = Event::new("1 2 3 4 5 6 7 8", vec!["tag1", "tag2"]);

    let conn = db.connect("test").unwrap();
    assert!(conn.publish(test_event.clone()).is_ok());

    let query = Query::current();
    let retrieved_events: Vec<_> = conn.subscribe(query).unwrap().map(|e| e.unwrap()).take(1).collect();
    let expected_events: Vec<_> = vec![test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp)];
    assert_eq!(retrieved_events, expected_events);

    assert!(db.drop_collection("test").is_ok());
    assert!(!db.contains_collection("test"));
}
