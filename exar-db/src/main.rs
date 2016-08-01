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
