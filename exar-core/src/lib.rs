#![cfg_attr(feature = "serde-serialization", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde-serialization", plugin(serde_macros))]

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
//! let connection = db.connect(collection_name).unwrap();
//!
//! match connection.publish(Event::new("payload", vec!["tag1", "tag2"])) {
//!     Ok(event_id) => println!("Published event with ID: {}", event_id),
//!     Err(err) => panic!("Unable to publish event: {}", err)
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
//! let connection = db.connect(collection_name).unwrap();
//!
//! let query = Query::live().offset(0).limit(10).by_tag("tag1");
//! let event_stream = connection.subscribe(query).unwrap();
//! for event in event_stream {
//!     println!("Received event: {}", event);
//! }
//! # }
//! ```

#[cfg(feature = "rustc-serialization")] extern crate rustc_serialize;
#[cfg(feature = "serde-serialization")] extern crate serde;

#[cfg(test)] #[macro_use]
extern crate exar_testkit;

#[macro_use]
extern crate log as logging;

extern crate indexed_line_reader;
extern crate rand;
extern crate time;

mod logger;
mod config;
mod collection;
mod connection;
mod database;
mod encoding;
mod error;
mod event;
mod log;
mod query;
mod scanner;
mod routing_strategy;
mod subscription;
mod util;
mod validation;

pub use self::logger::*;
pub use self::config::*;
pub use self::collection::*;
pub use self::connection::*;
pub use self::database::*;
pub use self::encoding::*;
pub use self::error::*;
pub use self::event::*;
pub use self::log::*;
pub use self::query::*;
pub use self::routing_strategy::*;
pub use self::scanner::*;
pub use self::subscription::*;
pub use self::util::*;
pub use self::validation::*;
