#![feature(test)]

extern crate exar;
extern crate rand;
extern crate test;

use exar::*;
use test::Bencher;

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
