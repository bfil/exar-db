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
        // let _ = db.drop_collection("test");
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

fn big_data_perf_test(scanners: u8, num_events: usize) {

    let (mut config, collection_name) = setup();
    config.database.scanners = scanners;

    let mut db = Database::new(config.database);

    let connection = db.connect(collection_name).unwrap();

    println!("---------------------------------------------------------------");
    println!("Big data performance test with {} scanners and {} events", scanners, num_events);

    // Writing
    // let sw = Stopwatch::start_new();
    // for i in 0..num_events {
    //     match connection.publish(Event::new(PAYLOAD, vec!["tag1"])) {
    //         Ok(_) => i,
    //         Err(err) => panic!("Unable to log to the database: {}", err)
    //     };
    //     match connection.publish(Event::new(PAYLOAD, vec!["tag2"])) {
    //         Ok(_) => i,
    //         Err(err) => panic!("Unable to log to the database: {}", err)
    //     };
    // }
    // report_performance(sw, num_events * 2, "Writing");

    // Reading
    // let sw = Stopwatch::start_new();
    // let _: Vec<_> = connection.subscribe(query.clone()).unwrap().take(num_events).collect();
    // report_performance(sw, num_events, "Reading");

    // Reading Last Element
    let sw = Stopwatch::start_new();
    let last_element_query = Query::current().offset((2 * num_events) - 1).limit(1);
    let _: Vec<_> = connection.subscribe(last_element_query).unwrap().take(1).collect();
    println!("Reading last element took {}ms..", sw.elapsed_ms());

    // let _ = db.drop_collection(collection_name);

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
    println!("Server listening on {}..", config.server.address());
    server.listen();
    println!("Server shutting down..");
}

use std::fs::*;
use std::io::{BufRead, BufReader, Error, Lines, Read, Seek, SeekFrom};
use std::collections::BTreeMap;
use std::iter::Skip;

struct IndexedLineReader<T> {
    index: BTreeMap<u64, u64>,
    index_interval: u64,
    last_index: u64,
    pos: u64,
    reader: T
}

impl<T: BufRead + Seek> IndexedLineReader<T> {
    fn new(reader: T) -> IndexedLineReader<T> {
        let mut indexed_line_reader = IndexedLineReader {
            index: BTreeMap::new(),
            index_interval: 100000,
            last_index: 0,
            pos: 0,
            reader: reader
        };
        indexed_line_reader.index();
        indexed_line_reader
    }

    fn bytes_len_at_index(&self, index: &u64) -> u64 {
        self.index.get(index).map(|&x| x).unwrap_or(0)
    }

    fn index(&mut self) -> Result<(), Error> {
        let mut bytes_len = self.bytes_len_at_index(&self.last_index);
        let mut reader = &mut self.reader;
        // let current_pos = reader.seek(SeekFrom::Current(0)).unwrap();
        if bytes_len > 0 {
            reader.seek(SeekFrom::Start(bytes_len));
        }
        for (i, line) in reader.lines().enumerate() {
            match line {
                Ok(line) => {
                    bytes_len += (line.as_bytes().len() as u64 + 1);
                    if (i as u64 + 2) % self.index_interval == 0 {
                        self.last_index = i as u64 + 2;
                        self.index.insert(self.last_index, bytes_len);
                    }
                },
                Err(err) => return Err(err)
            }
        }
        // reader.seek(SeekFrom::Start(current_pos));
        Ok(())
    }
}

impl<T: Read> Read for IndexedLineReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.reader.read(buf)
    }
}

impl<T: BufRead> BufRead for IndexedLineReader<T> {
    fn fill_buf(&mut self) -> Result<&[u8], Error> {
        self.reader.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt)
    }
}

impl<T: BufRead + Seek> Seek for IndexedLineReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        match pos {
            SeekFrom::Start(pos) => {
                self.index();
                let mut extra_lines = pos % self.index_interval;
                let closest_index = pos - extra_lines;

                let mut bytes_len = self.bytes_len_at_index(&closest_index);

                self.reader.seek(SeekFrom::Start(bytes_len)).and_then(|new_pos| {
                    if extra_lines > 0 {
                        self.pos = closest_index;
                        self.seek(SeekFrom::Current(extra_lines as i64)).and_then(|new_extra_pos| {
                            self.pos = pos;
                            Ok(new_pos + new_extra_pos)
                        })
                    } else {
                        self.pos = pos;
                        Ok(new_pos)
                    }
                })
            },
            SeekFrom::Current(pos) => {
                self.index();
                let mut extra_bytes_len: u64 = 0;
                let mut lines_left = pos as u64;

                let mut extra_lines = lines_left % self.index_interval;

                let mut extra_lines_from_current_pos = self.pos % self.index_interval;
                let previous_closest_index = self.pos - extra_lines_from_current_pos;
                let closest_index = previous_closest_index + lines_left - extra_lines;

                let mut bytes_len = self.bytes_len_at_index(&closest_index);
                self.reader.seek(SeekFrom::Start(bytes_len));
                lines_left = extra_lines + extra_lines_from_current_pos;

                if lines_left > 0 {
                    for line in (&mut self.reader).lines() {
                        match line {
                            Ok(line) => {
                                lines_left -= 1;
                                extra_bytes_len += (line.as_bytes().len() as u64 + 1);
                                if lines_left == 0 { break }
                            },
                            Err(err) => return Err(err)
                        }
                    }
                }
                self.pos += pos as u64;
                Ok(extra_bytes_len)
            },
            _ => unimplemented!()
        }

    }
}

fn perf_fetch_line(mut line_reader: &mut IndexedLineReader<BufReader<File>>, seek_from: SeekFrom) -> () {
    let sw = Stopwatch::start_new();
    let _ = line_reader.seek(seek_from);
    println!("{:?}", (&mut line_reader).lines().next());
    println!("Fetching line after seeking using {:?} took {}ms..", seek_from, sw.elapsed_ms());
}

fn main() {
    // perf_test(1, 10, 100000);
    // perf_test(2, 10, 100000);
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

    // big_data_perf_test(1, 20000000);

    let log = Log::new("/Users/bruno.filippone/Downloads", "test");
    let reader = log.open_reader().unwrap();

    println!("Starting indexing lines..");
    let sw = Stopwatch::start_new();
    let mut line_reader = IndexedLineReader::new(reader);
    println!("Indexing took {}ms..", sw.elapsed_ms());

    perf_fetch_line(&mut line_reader, SeekFrom::Start(39000100));
    perf_fetch_line(&mut line_reader, SeekFrom::Start(23832100));
    perf_fetch_line(&mut line_reader, SeekFrom::Current(10000000));
    perf_fetch_line(&mut line_reader, SeekFrom::Current(1000005));
    perf_fetch_line(&mut line_reader, SeekFrom::Current(95));
    perf_fetch_line(&mut line_reader, SeekFrom::Current(1000005));



    // server_test(0, 0);
}
