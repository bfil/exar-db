extern crate exar;
extern crate rand;

mod testkit {
    use rand;
    use rand::Rng;
    pub fn gen_collection_name() -> String {
        rand::thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect::<String>()
    }
}

use exar::*;

#[test]
fn integration_test() {
    let mut db = Database::new(DatabaseConfig::default());

    let ref collection_name = testkit::gen_collection_name();
    let connection = db.connect(collection_name).expect("Unable to connect");

    let test_event = Event::new("data", vec!["tag1", "tag2"]);
    assert!(connection.publish(test_event.clone()).is_ok());

    let query = Query::current();
    let retrieved_events: Vec<_> = connection.subscribe(query).unwrap().take(1).collect();
    let expected_event = test_event.clone().with_id(1).with_timestamp(retrieved_events[0].timestamp);
    assert_eq!(retrieved_events, vec![expected_event]);

    assert!(db.drop_collection(collection_name).is_ok());
    assert!(!db.contains_collection(collection_name));
}
