#![feature(test)]

extern crate exar;
extern crate rand;
extern crate test;

#[cfg(test)]
extern crate exar_testkit;

use exar::*;
use exar_testkit::*;
use test::Bencher;

#[bench]
fn bench_publish(b: &mut Bencher) {
    let collection_name = &random_collection_name();
    let config = DatabaseConfig::default();
    let mut db = Database::new(config);
    let connection = db.connect(collection_name).unwrap();
    b.iter(|| {
        let _ = connection.publish(Event::new("data", vec!["tag1"]));
    });
    assert!(db.drop_collection(collection_name).is_ok());
    connection.close();
}
