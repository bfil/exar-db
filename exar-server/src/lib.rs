#![cfg_attr(feature = "serde-serialization", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde-serialization", plugin(serde_macros))]

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
//!
//! let db = Database::new(DatabaseConfig::default());
//!
//! let server_config = ServerConfig::default();
//! let server = Server::new(server_config.clone(), db).unwrap();
//!
//! println!("ExarDB's server running at {}", server_config.address());
//! server.listen();
//! println!("ExarDB's server shutting down");
//! # }
//! ```

extern crate exar;
extern crate exar_net;

#[cfg(feature = "rustc-serialization")] extern crate rustc_serialize;
#[cfg(feature = "serde-serialization")] extern crate serde;

#[cfg(test)] #[macro_use]
extern crate exar_testkit;

#[macro_use]
extern crate log;

mod credentials;
mod config;
mod handler;
mod server;

pub use self::credentials::*;
pub use self::config::*;
pub use self::handler::*;
pub use self::server::*;
