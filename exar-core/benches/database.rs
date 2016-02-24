#![feature(test)]

extern crate test;
use test::Bencher;

extern crate exar;

use exar::*;

#[bench]
fn bench_publish(b: &mut Bencher) {
    let ref collection_name = testkit::gen_collection_name();
    let config = DatabaseConfig::default();
    let mut db = Database::new(config);
    let connection = db.connect(collection_name).unwrap();
    let num_events = 1000;
    b.iter(|| {
        for _ in 0..num_events {
            let _ = connection.publish(Event::new("data", vec!["tag1"]));
        }
    });
    assert!(db.drop_collection(collection_name).is_ok());
    connection.close();
}
