use exar::*;
use exar_server::*;

#[derive(RustcEncodable, RustcDecodable)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config  {
    pub database: DatabaseConfig,
    pub server: ServerConfig
}

impl Default for Config {
    fn default() -> Config {
        Config {
            database: DatabaseConfig::default(),
            server: ServerConfig::default()
        }
    }
}
