/// Exar DB's server configuration.
///
/// # Examples
/// ```
/// extern crate exar_server;
///
/// # fn main() {
/// use exar_server::*;
///
/// let config = ServerConfig {
///     host: "127.0.0.1".to_owned(),
///     port: 38580,
///     username: Some("username".to_owned()),
///     password: Some("password".to_owned())
/// };
/// # }
/// ```
#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig  {
    /// The server host.
    pub host: String,
    /// The server port.
    pub port: u16,
    /// The server authentication's username.
    pub username: Option<String>,
    /// The server authentication's password.
    pub password: Option<String>
}

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 38580,
            username: None,
            password: None
        }
    }
}

impl ServerConfig {
    /// Returns a string representation of the server address (`host:port`).
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
