use exar::*;
use exar_server::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config  {
    pub log4rs_path: String,
    pub database: DatabaseConfig,
    pub server: ServerConfig
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
