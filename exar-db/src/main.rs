extern crate clap;
extern crate exar;
extern crate exar_server;
extern crate rustc_serialize;
extern crate toml_config;

mod config;
use config::*;

use clap::App;
use exar::*;
use exar_server::*;
use std::path::Path;
use toml_config::ConfigFactory;

fn main() {
    let matches = App::new("exar-db")
                      .version("0.1.0")
                      .author("Bruno Filippone <bruno.filippone@b-fil.com>")
                      .about("An event store built with Rust")
                      .args_from_usage(
                         "-c, --config=[FILE] 'Sets a custom config file'")
                      .get_matches();

    let config = match matches.value_of("config") {
        Some(config_file) => ConfigFactory::load(Path::new(config_file)),
        None => Config::default()
    };

    let db = Database::new(config.database);
    let server = Server::new(config.server.clone(), db).unwrap();

    println!("ExarDB running at {}..", config.server.address());
    server.listen();
    println!("ExarDB shutting down..");
}
