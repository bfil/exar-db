# Exar DB

An event store with streaming support, it uses flat-file based collections.

## Modules

The database is split into the following modules:

- [exar-core](https://github.com/bfil/exar-db/tree/master/exar-core): the core engine of Exar DB
- [exar-net](https://github.com/bfil/exar-db/tree/master/exar-net): a TCP protocol for Exar DB
- [exar-server](https://github.com/bfil/exar-db/tree/master/exar-server): a TCP server built on top of `exar-net`
- [exar-client](https://github.com/bfil/exar-db/tree/master/exar-client): a TCP client built on top of `exar-net`
- [exar-db](https://github.com/bfil/exar-db/tree/master/exar-db): the main executable of Exar DB

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
logs_path = "~/exar-db/data"
scanners = { nr_of_scanners = 2, sleep_time_in_ms = 10 }
[database.collections.my-collection]
routing_strategy = "Random"
scanners = { nr_of_scanners = 4, sleep_time_in_ms = 5 }
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

## Interacting with the database via TCP

To interact with the database a very simple TCP protocol can be used even via `telnet`.

```
telnet 127.0.0.1 38580
```

Once the TCP connection has been established, you can use the commands defined in the
[exar-net](https://bfil.github.io/exar-db/exar_net/index.html)
section of the documentation

## Exar UI

A simple user interface, built with Electron, useful to interact with the database is available [here](https://github.com/bfil/exar-db/tree/master/exar-ui), but it currently needs to be run from source.
