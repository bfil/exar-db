# Exar DB

An event store with streaming support, it uses flat-file based collections.

[![Crates.io](https://img.shields.io/crates/v/exar-server.svg)](https://crates.io/crates/exar-db)

[Documentation](https://bfil.github.io/exar-db/exar_db/index.html)

## Installation

Install [`Cargo`](https://crates.io/install), then run:

```
cargo install exar-db
```

## Starting the database

Simply run `exar-db`.

## Configuring the database

The database can be configured using a `TOML` configuration file, example below:

```toml
log4rs_path = "/path/to/log4rs.toml"
[database]
data = { path = "~/exar-db/data" }
scanner = { threads = 2 }
[database.collections.my-collection]
scanner = { threads = 4, routing_strategy = "Random" }
publisher = { buffer_size = 10000 }
[server]
host = "127.0.0.1"
port = 38580
username = "my-username"
password = "my-secret"
```

Then run Exar DB by specifying the config file location: `exar-db --config=/path/to/config.toml`.

For more information about the `database` and `server` configuration sections,
check the documentation about
[DatabaseConfig](https://bfil.github.io/exar-db/exar/struct.DatabaseConfig.html) and
[ServerConfig](https://bfil.github.io/exar-db/exar_server/struct.ServerConfig.html).

## Logging

Logging can be configured using a [log4rs](https://github.com/sfackler/log4rs) config file in `TOML` format, example below:

```toml
[appenders.console]
kind = "console"

[appenders.console.encoder]
pattern = "[{d(%+)(local)}] [{h({l})}] [{t}] {m}{n}"

[appenders.file]
kind = "file"
path = "exar-db.log"

[appenders.file.encoder]
pattern = "[{d(%+)(local)}] [{h({l})}] [{t}] {m}{n}"

[root]
level = "info"
appenders = ["console", "file"]
```
