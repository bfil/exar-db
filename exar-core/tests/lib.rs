extern crate exar;
extern crate rand;

#[cfg(test)]
extern crate exar_testkit;

use exar::*;
use exar_testkit::*;

use std::collections::BTreeMap;

#[test]
fn integration_test() {
    let mut db = Database::new(DatabaseConfig {
        data: DataConfig {
            path: temp_dir(),
            index_granularity: DEFAULT_INDEX_GRANULARITY,
            flush_mode: FlushMode::FixedSize,
            buffer_size: None
        },
        scanner: ScannerConfig::default(),
        publisher: PublisherConfig::default(),
        collections: BTreeMap::new()
    });

    let collection_name   = &random_collection_name();
    let shared_collection = db.collection(collection_name).expect("Unable to retrieve collection");
    let mut collection    = shared_collection.lock().unwrap();

    let test_event = Event::new("data", vec!["tag1", "tag2"]);
    assert!(collection.publish(test_event.clone()).is_ok());

    let query                    = Query::current();
    let subscription             = collection.subscribe(query).expect("Unable to subscribe");
    let retrieved_events: Vec<_> = subscription.event_stream().take(1).collect();
    let expected_event           = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);
    assert_eq!(retrieved_events, vec![expected_event]);

    assert!(subscription.unsubscribe().is_ok());
    assert!(collection.publish(test_event.clone()).is_ok());

    let retrieved_events: Vec<_> = subscription.event_stream().take(1).collect();
    assert_eq!(retrieved_events, vec![]);

    drop(collection);

    assert!(db.delete_collection(collection_name).is_ok());
}
