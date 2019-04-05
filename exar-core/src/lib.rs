//! # Exar DB
//! Exar DB is an event store with streaming support
//! which uses a flat-file for each collection of events
//!
//! ## Database Initialization
//! ```
//! extern crate exar;
//!
//! # fn main() {
//! use exar::*;
//!
//! let config = DatabaseConfig::default();
//! let mut db = Database::new(config);
//! # }
//! ```
//! ## Publishing events
//! ```no_run
//! extern crate exar;
//!
//! # fn main() {
//! use exar::*;
//!
//! let config = DatabaseConfig::default();
//! let mut db = Database::new(config);
//!
//! let collection_name = "test";
//! let shared_collection = db.collection(collection_name).expect("Unable to retrieve collection");
//! let mut collection    = shared_collection.lock().unwrap();
//!
//! match collection.publish(Event::new("payload", vec!["tag1", "tag2"])) {
//!     Ok(event_id) => println!("Published event with ID: {}", event_id),
//!     Err(err)     => panic!("Unable to publish event: {}", err)
//! };
//! # }
//! ```
//! ## Querying events
//! ```no_run
//! extern crate exar;
//!
//! # fn main() {
//! use exar::*;
//!
//! let config = DatabaseConfig::default();
//! let mut db = Database::new(config);
//!
//! let collection_name = "test";
//! let shared_collection = db.collection(collection_name).expect("Unable to retrieve collection");
//! let mut collection    = shared_collection.lock().unwrap();
//!
//! let query             = Query::live().offset(0).limit(10).by_tag("tag1");
//! let (event_stream, _) = collection.subscribe(query).expect("Unable to subscribe");
//! for event in event_stream {
//!     println!("Received event: {}", event);
//! }
//! # }
//! ```

extern crate serde;

#[macro_use] extern crate serde_derive;
#[cfg(test)] extern crate exar_testkit;
#[macro_use] extern crate log as logging;

extern crate indexed_line_reader;
extern crate rand;
extern crate time;

mod logger;
mod config;
mod collection;
mod database;
mod encoding;
mod error;
mod event;
mod log;
mod publisher;
mod query;
mod scanner;
mod routing_strategy;
mod subscription;
mod thread;
mod util;
mod validation;

#[cfg(test)] mod testkit;

pub use self::logger::*;
pub use self::config::*;
pub use self::collection::*;
pub use self::database::*;
pub use self::encoding::*;
pub use self::error::*;
pub use self::event::*;
pub use self::log::*;
pub use self::publisher::*;
pub use self::query::*;
pub use self::routing_strategy::*;
pub use self::scanner::*;
pub use self::subscription::*;
pub use self::thread::*;
pub use self::util::*;
pub use self::validation::*;

pub type DatabaseResult<T> = Result<T, DatabaseError>;