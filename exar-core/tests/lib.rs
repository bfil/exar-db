extern crate exar;
extern crate rand;

#[cfg(test)]
extern crate exar_testkit;

use exar::*;
use exar_testkit::*;

#[test]
fn integration_test() {
    let mut db = Database::new(DatabaseConfig::default());

    let collection_name   = &random_collection_name();
    let shared_collection = db.collection(collection_name).expect("Unable to retrieve collection");
    let mut collection    = shared_collection.lock().unwrap();

    let test_event = Event::new("data", vec!["tag1", "tag2"]);
    assert!(collection.publish(test_event.clone()).is_ok());

    let query                    = Query::current();
    let (_, event_stream)        = collection.subscribe(query).expect("Unable to subscribe");
    let retrieved_events: Vec<_> = event_stream.take(1).collect();
    let expected_event           = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);
    assert_eq!(retrieved_events, vec![expected_event]);

    drop(collection);

    assert!(db.delete_collection(collection_name).is_ok());
}
