#![feature(test)]

extern crate test;
use test::Bencher;

extern crate exar;

use exar::*;

const COLLECTION_NAME: &'static str = "bench";
const PAYLOAD: &'static str = "1 2 3 4 \t 5 6 7 8 \\n 9 10";

fn setup() -> (Database, Connection) {
    let config = DatabaseConfig::default();
    let mut db = Database::new(config);
    (db.clone(), db.connect(COLLECTION_NAME).unwrap())
}

#[bench]
fn write_bench(b: &mut Bencher) {
    let (mut db, conn) = setup();
    let num_events = 1000;
    b.iter(|| {
        for _ in 0..num_events {
            let _ = conn.publish(Event::new(PAYLOAD, vec!["tag1"]));
        }
    });
    let _ = db.drop_collection(COLLECTION_NAME);
    conn.close();
}
