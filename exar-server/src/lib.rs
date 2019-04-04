//! # Exar DB's server
//! This module contains a server implementation that uses Exar DB's TCP protocol.
//!
//! It uses the one thread per connection model.
//!
//! ## Server Initialization
//! ```no_run
//! extern crate exar;
//! extern crate exar_server;
//!
//! # fn main() {
//! use exar::*;
//! use exar_server::*;
//! use std::sync::{Arc, Mutex};
//!
//! let db = Database::new(DatabaseConfig::default());
//!
//! let server_config = ServerConfig::default();
//! let server = Server::new(server_config.clone(), Arc::new(Mutex::new(db))).unwrap();
//!
//! println!("ExarDB's server running at {}", server_config.address());
//! server.listen();
//! println!("ExarDB's server shutting down");
//! # }
//! ```

extern crate exar;
extern crate exar_net;

extern crate serde;
#[macro_use] extern crate serde_derive;

#[cfg(test)]
extern crate exar_testkit;

#[macro_use]
extern crate log;

mod config;
mod connection;
mod credentials;
mod handler;
mod server;

pub use self::config::*;
pub use self::connection::*;
pub use self::credentials::*;
pub use self::handler::*;
pub use self::server::*;
