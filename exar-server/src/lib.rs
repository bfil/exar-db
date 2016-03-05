#![feature(const_fn)]

#![cfg_attr(feature = "serde-serialization", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde-serialization", plugin(serde_macros))]

extern crate exar;
extern crate exar_net;

#[cfg(feature = "rustc-serialization")] extern crate rustc_serialize;
#[cfg(feature = "serde-serialization")] extern crate serde;

mod credentials;
mod config;
mod handler;
mod server;

pub use self::credentials::*;
pub use self::config::*;
pub use self::handler::*;
pub use self::server::*;
