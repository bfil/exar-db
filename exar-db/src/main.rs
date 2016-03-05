#![allow(warnings)]

extern crate exar;
extern crate exar_net;
extern crate exar_client;
extern crate exar_server;
extern crate rustc_serialize;
extern crate stopwatch;
extern crate toml_config;

mod config;
mod util;

use config::*;

use exar::*;
use exar_client::*;
use exar_server::*;

use toml_config::ConfigFactory;

use std::path::Path;
use std::thread;
use std::time::Duration;
use stopwatch::Stopwatch;

use util::report_performance;

const PAYLOAD: &'static str = "1 2 3 4 \t 5 6 7 8 \\n 9 10";

fn setup() -> (Config, &'static str) {
    let mut config: Config = ConfigFactory::load(Path::new("config.toml"));
    config.database.logs_path = "/Users/bruno.filippone/Downloads".to_owned();
    {
        let mut db = Database::new(config.database.clone());
        let _ = db.drop_collection("test");
    }
    (config, "test")
}

fn perf_test(scanners: u8, num_subscribers: usize, num_events: usize) {

    let (mut config, collection_name) = setup();
    config.database.scanners = scanners;

    let mut db = Database::new(config.database);

    let connection = db.connect(collection_name).unwrap();

    let query = Query::live().offset(0).limit(num_events * 2).by_tag("tag1");

    println!("---------------------------------------------------------------");
    println!("Performance test with {} scanners, {} subscriptions and {} events", scanners, num_subscribers, num_events);

    // Subscribing - No Data
    for _ in 0..num_subscribers {
        let _ = connection.subscribe(query.clone());
    }

    // Writing
    let sw = Stopwatch::start_new();
    for i in 0..num_events {
        match connection.publish(Event::new(PAYLOAD, vec!["tag1"])) {
            Ok(_) => i,
            Err(err) => panic!("Unable to log to the database: {}", err)
        };
        match connection.publish(Event::new(PAYLOAD, vec!["tag2"])) {
            Ok(_) => i,
            Err(err) => panic!("Unable to log to the database: {}", err)
        };
    }
    report_performance(sw, num_events * 2, "Writing");

    // Reading
    let sw = Stopwatch::start_new();
    let _: Vec<_> = connection.subscribe(query.clone()).unwrap().take(num_events).collect();
    report_performance(sw, num_events, "Reading");

    // Reading Again
    let sw = Stopwatch::start_new();
    let _: Vec<_> = connection.subscribe(query.clone()).unwrap().take(num_events).collect();
    report_performance(sw, num_events, "Reading again");

    // Subscribing - With Data & No Scanners Running
    let sw = Stopwatch::start_new();
    for _ in 0..num_subscribers {
        let _ = connection.subscribe(query.clone());
    }
    println!("Subscribing {} times with {} events took {}ms..", num_subscribers, num_events, sw.elapsed_ms());

    thread::sleep(Duration::from_millis(100));

    // Subscribing Again - With Data & Scanners Running
    let sw = Stopwatch::start_new();
    for _ in 0..num_subscribers {
        let _ = connection.subscribe(query.clone());
    }
    println!("Subscribing again {} times with {} events took {}ms..", num_subscribers, num_events, sw.elapsed_ms());

    // Reading Yet Again
    let sw = Stopwatch::start_new();
    let _: Vec<_> = connection.subscribe(query.clone()).unwrap().take(num_events).collect();
    report_performance(sw, num_events, "Reading yet again");

    // Dropping Collection - While Scanning
    let _ = connection.subscribe(query.clone());
    thread::sleep(Duration::from_millis(100));
    let sw = Stopwatch::start_new();
    let _ = db.drop_collection(collection_name);
    println!("Dropping collection took {}ms..", sw.elapsed_ms());

    connection.close();
}

fn big_data_perf_test(num_events: usize) {

    let (mut config, collection_name) = setup();

    let mut db = Database::new(config.database);

    let connection = db.connect(collection_name).unwrap();

    let query = Query::live().offset(0).limit(num_events * 2).by_tag("tag1");

    println!("---------------------------------------------------------------");
    println!("Big data performance test with {} events", num_events);

    // Writing
    let sw = Stopwatch::start_new();
    for i in 0..num_events {
        match connection.publish(Event::new(PAYLOAD, vec!["tag1"])) {
            Ok(_) => i,
            Err(err) => panic!("Unable to log to the database: {}", err)
        };
        match connection.publish(Event::new(PAYLOAD, vec!["tag2"])) {
            Ok(_) => i,
            Err(err) => panic!("Unable to log to the database: {}", err)
        };
    }
    report_performance(sw, num_events * 2, "Writing");

    // Reading
    let sw = Stopwatch::start_new();
    let _: Vec<_> = connection.subscribe(query.clone()).unwrap().take(num_events).collect();
    report_performance(sw, num_events, "Reading");

    // Reading Last Element
    let sw = Stopwatch::start_new();
    let last_element_query = Query::current().offset((2 * num_events) - 1).limit(1);
    let _: Vec<_> = connection.subscribe(last_element_query).unwrap().take(1).collect();
    println!("Reading last element took {}ms..", sw.elapsed_ms());

    let _ = db.drop_collection(collection_name);

    connection.close();
}

fn server_test(num_clients: usize, num_events: usize) {

    let (config, collection_name) = setup();

    fn spawn_client(server_address: &str, collection_name: &str, num_events: usize) {
        let owned_collection_name = collection_name.to_owned();
        let owned_address = server_address.to_owned();
        thread::spawn(move || {
            let server_address: &str = owned_address.as_ref();
            let test_event = Event::new(PAYLOAD, vec!["tag1"]);
            thread::sleep(Duration::from_millis(100));
            let mut client = Client::connect(server_address, &owned_collection_name, Some("admin"), Some("secret")).unwrap();
            for _ in 0..num_events {
                let _ = client.publish(test_event.clone());
            }
            let query = Query::live().offset(0).limit(num_events).by_tag("tag1");
            let events: Vec<_> = client.subscribe(query).unwrap().take(num_events).collect();
            println!("Received {} events..", events.len());
            thread::sleep(Duration::from_millis(100));
        });
    }

    let ref server_address = config.server.address();

    for _ in 0..num_clients {
        spawn_client(server_address, collection_name, num_events);
    }

    let db = Database::new(config.database);
    let server = Server::new(config.server.clone(), db).unwrap();
    println!("---------------------------------------------------------------");
    println!("Server listening on {}..", config.server.address());
    server.listen();
    println!("Server shutting down..");
}
fn main() {
    perf_test(1, 10, 100000);
    perf_test(2, 10, 100000);
    // perf_test(1, 10, 1000000);
    // perf_test(1, 10, 10000000);
    // perf_test(1, 100, 100000);
    // perf_test(2, 100, 100000);
    // perf_test(1, 100, 1000000);
    // perf_test(1, 100, 10000000);
    // perf_test(1, 1000, 100000);
    // perf_test(2, 1000, 100000);
    // perf_test(1, 1000, 1000000);
    // perf_test(1, 1000, 10000000);
    // perf_test(1, 10000, 100000);
    // perf_test(1, 10000, 1000000);
    // perf_test(1, 10000, 10000000);

    big_data_perf_test(1000000);
    // big_data_perf_test(10000000);
    // big_data_perf_test(50000000);

    server_test(10, 10000);
}
