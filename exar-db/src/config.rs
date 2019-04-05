use exar::*;
use exar_server::*;

use std::io::Read;
use std::fs::File;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config  {
    pub log4rs_path: String,
    pub database: DatabaseConfig,
    pub server: ServerConfig
}

impl Config {
    pub fn load(toml_file: &Path) -> Config {
        let mut toml_config = String::new();

        let mut file = match File::open(toml_file) {
            Ok(file) => file,
            Err(_)   => panic!("Config file not found: {}", toml_file.display())
        };

        file.read_to_string(&mut toml_config)
            .unwrap_or_else(|err| panic!("Unable to read config file: {}", err));

        match toml::from_str(&toml_config) {
            Ok(config) => config,
            Err(_)     => panic!("Config file could not be parsed: {}", toml_file.display())
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            log4rs_path: "log4rs.toml".to_owned(),
            database: DatabaseConfig::default(),
            server: ServerConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::io::Write;
    use std::collections::BTreeMap;

    #[test]
    fn test_config_load() {

        let toml_file = tempfile!(r#"
            [database]
            data = { path = "/path/to/logs" }
            scanner = { threads = 1, routing_strategy = "Random" }

            [database.collections.test]
            data = { path = "/other/path/to/logs", index_granularity = 10000 }
            scanner = { threads = 3, routing_strategy = "RoundRobin" }
            publisher = { buffer_size = 10000 }

            [server]
            host = "127.0.0.1"
            port = 38580
            username = "admin"
            password = "secret"
        "#);

        let loaded_config: Config = Config::load(toml_file.path());

        let mut expected_config = Config {
            log4rs_path: "log4rs.toml".to_owned(),
            database: DatabaseConfig {
                data: DataConfig {
                    path: "/path/to/logs".to_owned(),
                    index_granularity: 100000,
                },
                scanner: ScannerConfig {
                    routing_strategy: RoutingStrategy::Random,
                    threads: 1
                },
                publisher: PublisherConfig {
                    buffer_size: 1000
                },
                collections: BTreeMap::new()
            },
            server: ServerConfig {
                host: "127.0.0.1".to_owned(),
                port: 38580,
                username: Some("admin".to_owned()),
                password: Some("secret".to_owned())
            }
        };

        expected_config.database.collections.insert("test".to_owned(), PartialCollectionConfig {
            data: Some(PartialDataConfig {
                path: Some("/other/path/to/logs".to_owned()),
                index_granularity: Some(10000)
            }),
            scanner: Some(PartialScannerConfig {
                routing_strategy: Some(RoutingStrategy::RoundRobin(0)),
                threads: Some(3)
            }),
            publisher: Some(PartialPublisherConfig {
                buffer_size: Some(10000)
            })
        });

        assert_eq!(loaded_config, expected_config);

    }
}

