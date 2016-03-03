#![cfg_attr(feature = "serde-serialization", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde-serialization", plugin(serde_macros))]

#[cfg(feature = "rustc-serialization")] extern crate rustc_serialize;
#[cfg(feature = "serde-serialization")] extern crate serde;

extern crate rand;
extern crate time;

#[cfg(test)]
pub mod testkit;

mod logger;
mod config;
mod collection;
mod connection;
mod database;
mod encoding;
mod error;
mod event;
mod line_reader;
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
pub use self::line_reader::*;
pub use self::log::*;
pub use self::query::*;
pub use self::routing_strategy::*;
pub use self::scanner::*;
pub use self::subscription::*;
pub use self::util::*;
pub use self::validation::*;
