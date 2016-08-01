//! # Exar DB
//! Exar DB is an event store with streaming support
//! which uses a flat-file for each collection of events
//!
//! ## Installation
//!
//! Install [`Cargo`](https://crates.io/install), then run:
//!
//! ```
//! cargo install exar-db
//! ```
//!
//! ## Starting the database
//!
//! Simply run `exar-db`.
//!
//! ## Configuring the database
//!
//! The database can be configured using a `TOML` configuration file, example below:
//!
//! ```toml
//! log4rs_path = "/path/to/log4rs.toml"
//! [database]
//! logs_path = "~/exar-db/data"
//! scanners = { nr_of_scanners = 2, sleep_time_in_ms = 10 }
//! [database.collections.my-collection]
//! routing_strategy = "Random"
//! scanners = { nr_of_scanners = 4, sleep_time_in_ms = 5 }
//! [server]
//! host = "127.0.0.1"
//! port = 38580
//! username = "my-username"
//! password = "my-secret"
//! ```
//!
//! Then run Exar DB by specifying the config file location: `exar-db --config=/path/to/config.toml`.
//!
//! For more information about the `database` and `server` configuration sections,
//! check the documentation about
//! [DatabaseConfig](https://bfil.github.io/exar-db/exar/struct.DatabaseConfig.html) and
//! [ServerConfig](https://bfil.github.io/exar-db/exar_server/struct.ServerConfig.html).
//!
//! ## Logging
//!
//! Logging can be configured using a [log4rs](https://github.com/sfackler/log4rs) config file in `TOML` format, example below:
//!
//! ```toml
//! [appenders.console]
//! kind = "console"
//!
//! [appenders.console.encoder]
//! pattern = "[{d(%+)(local)}] [{h({l})}] [{t}] {m}{n}"
//!
//! [appenders.file]
//! kind = "file"
//! path = "exar-db.log"
//!
//! [appenders.file.encoder]
//! pattern = "[{d(%+)(local)}] [{h({l})}] [{t}] {m}{n}"
//!
//! [root]
//! level = "info"
//! appenders = ["console", "file"]
//! ```

extern crate clap;
extern crate exar;
extern crate exar_server;
extern crate rustc_serialize;
extern crate toml_config;

#[macro_use]
extern crate log;
extern crate log4rs;

mod config;
use config::*;

use clap::App;
use exar::*;
use exar_server::*;
use log::LogLevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config as Log4rsConfig, Root};
use std::path::Path;
use toml_config::ConfigFactory;

fn main() {
    let matches = App::new("exar-db")
                      .version("0.1.0")
                      .author("Bruno Filippone <bruno.filippone@b-fil.com>")
                      .about("An event store with streaming support which uses a flat-file for each collection of events")
                      .args_from_usage(
                         "-c, --config=[FILE] 'Sets a custom config file'")
                      .get_matches();

    let config = match matches.value_of("config") {
        Some(config_file) => ConfigFactory::load(Path::new(config_file)),
        None => Config::default()
    };

    match log4rs::init_file(config.log4rs_path.clone(), Default::default()) {
        Ok(_) => info!("Loaded log4rs config file: {}", config.log4rs_path),
        Err(_) => {
            let console_appender = Appender::builder()
                                            .build("console".to_owned(), Box::new(ConsoleAppender::builder().build()));
            let root = Root::builder()
                            .appender("console".to_owned())
                            .build(LogLevelFilter::Info);
            let log4rs_config = Log4rsConfig::builder()
                                             .appender(console_appender)
                                             .build(root).expect("Unable to build log4rs config");
            log4rs::init_config(log4rs_config).expect("Unable to initialize log4rs config");
            info!("Unable to load config file '{}', using default console appender", config.log4rs_path);
        }
    };

    let db = Database::new(config.database);
    match Server::new(config.server.clone(), db) {
        Ok(server) => {
            info!("ExarDB running at {}", config.server.address());
            server.listen();
            info!("ExarDB shutting down");
        },
        Err(err) => error!("Unable to run ExarDB: {}", err)
    }
}
