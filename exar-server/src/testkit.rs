pub use super::*;

pub use exar::*;
pub use exar_net::*;
pub use exar_testkit::*;

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

pub fn temp_data_config(index_granularity: u64) -> DataConfig {
    DataConfig { path: temp_dir(), index_granularity }
}

pub fn temp_database_config() -> DatabaseConfig {
    DatabaseConfig {
        data: temp_data_config(DEFAULT_INDEX_GRANULARITY),
        scanner: ScannerConfig::default(),
        publisher: PublisherConfig::default(),
        collections: BTreeMap::new()
    }
}

pub fn temp_server_config() -> ServerConfig {
    let addr_string        = format!("{}", find_available_addr());
    let addr_parts: Vec<_> = addr_string.split(":").collect();
    let host               = addr_parts[0].parse().expect("Unable to parse host");
    let port               = addr_parts[1].parse().expect("Unable to parse port");
    ServerConfig { host, port, username: None, password: None }
}

pub fn invalid_server_config() -> ServerConfig {
    let addr_string        = format!("{}", find_available_addr());
    let addr_parts: Vec<_> = addr_string.split(":").collect();
    let host               = addr_parts[0].parse().expect("Unable to parse host");
    ServerConfig { host, port: 1000, username: None, password: None }
}

pub fn temp_database() -> Database {
    Database::new(temp_database_config())
}

pub fn temp_shared_database() -> Arc<Mutex<Database>> {
    Arc::new(Mutex::new(temp_database()))
}

pub fn create_client<A: ToSocketAddrs>(addr: A) -> TcpMessageStream<TcpStream> {
    let stream  = TcpStream::connect(addr).expect("Unable to connect to the TCP stream");
    TcpMessageStream::new(stream).expect("Unable to create TCP message stream client")
}

pub fn create_handler(addr: SocketAddr, credentials: Credentials) -> JoinHandle<()> {
    let db = temp_shared_database();
    let handle = thread::spawn(move || {
        let listener = TcpListener::bind(addr).expect("Unable to bind to address");
        match listener.accept() {
            Ok((stream, _)) => {
                let mut handler = Handler::new(stream, db, credentials).expect("Unable to create TCP connection handler");
                handler.run().expect("Unable to run handler");
            },
            Err(err) => panic!("Error: {}", err)
        }
    });
    thread::sleep(Duration::from_millis(100));
    handle
}
