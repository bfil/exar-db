use super::*;

use exar::*;

use std::net::{ToSocketAddrs, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;

/// Exar DB's server.
///
/// It manages TCP connections.
///
/// # Examples
/// ```no_run
/// extern crate exar;
/// extern crate exar_server;
///
/// # fn main() {
/// use exar::*;
/// use exar_server::*;
/// use std::sync::{Arc, Mutex};
///
/// let db = Database::new(DatabaseConfig::default());
/// let config = ServerConfig::default();
///
/// let mut server = Server::new(Arc::new(Mutex::new(db)), config).expect("Unable to create server");
/// server.listen();
/// # }
/// ```
#[derive(Debug)]
pub struct Server {
    credentials: Credentials,
    db: Arc<Mutex<Database>>,
    listener: TcpListener
}

impl Server {
    /// Creates a server with the given config and database and binds it to the configured host and port,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn new(db: Arc<Mutex<Database>>, config: ServerConfig) -> DatabaseResult<Server> {
        let listener = TcpListener::bind(&*config.address()).map_err(DatabaseError::from_io_error)?;
        let credentials = Credentials {
            username: config.username,
            password: config.password
        };
        Ok(Server { credentials, db, listener })
    }

    /// Creates a server database and binds it to the given address,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn bind<A: ToSocketAddrs>(db: Arc<Mutex<Database>>, address: A) -> DatabaseResult<Server> {
        let listener = TcpListener::bind(address).map_err(DatabaseError::from_io_error)?;
        let credentials = Credentials::empty();
        Ok(Server { credentials, db, listener })
    }

    /// Returns a modified version of the server by setting its credentials to the given value.
    pub fn with_credentials(mut self, username: &str, password: &str) -> Server {
        self.credentials.username = Some(username.to_string());
        self.credentials.password = Some(password.to_string());
        self
    }

    /// Starts listening for incoming TCP connections and blocks the current thread indefinitely.
    pub fn listen(&self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db          = self.db.clone();
                    let credentials = self.credentials.clone();
                    thread::spawn(|| {
                        let peer_addr = stream.peer_addr().ok().map(|addr| format!("{}", addr))
                                                               .unwrap_or("unknown peer address".to_owned());
                        match Handler::new(stream, db, credentials) {
                            Ok(handler) => {
                                info!("Client connected: {}", peer_addr);
                                match handler.run() {
                                    Ok(())   => info!("Client disconnected: {}", peer_addr),
                                    Err(err) => warn!("Client disconnected: {} ({})", peer_addr, err),
                                }
                            },
                            Err(err) => warn!("Unable to accept client connection: {}", err)
                        }
                    });
                },
                Err(err) => warn!("Client connection failed: {}", err)
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::thread;

    #[test]
    fn test_constructor() {
        let db            = temp_shared_database();
        let server_config = temp_server_config();
        assert!(Server::new(db, server_config).is_ok());
    }

    #[test]
    fn test_constructor_failure() {
        let db            = temp_shared_database();
        let server_config = invalid_server_config();
        assert!(Server::new(db, server_config).is_err());
    }

    #[test]
    fn test_bind() {
        let db   = temp_shared_database();
        let addr = find_available_addr();
        assert!(Server::bind(db, &addr).is_ok());
    }

    #[test]
    fn test_bind_failure() {
        let db = temp_shared_database();
        assert!(Server::bind(db, "127.0.0.1:1000").is_err());
    }

    #[test]
    fn test_connection() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();
        let db              = temp_shared_database();
        let server          = Server::bind(db, addr).expect("Unable to start the TCP server");

        thread::spawn(move || {
            server.listen();
        });

        let mut client = create_client(addr);

        let connect_without_credentials = TcpMessage::Connect(collection_name.to_owned(), None, None);
        assert!(client.write_message(connect_without_credentials).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Connected));
    }

    #[test]
    fn test_connection_with_credentials() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();
        let db              = temp_shared_database();
        let server          = Server::bind(db, addr).expect("Unable to start the TCP server");
        let server          = server.with_credentials("username", "password");

        thread::spawn(move || {
            server.listen();
        });

        let mut client = create_client(addr);

        let connect_without_credentials = TcpMessage::Connect(collection_name.to_owned(), None, None);
        assert!(client.write_message(connect_without_credentials).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Error(DatabaseError::AuthenticationError)));

        let connect_with_credentials = TcpMessage::Connect(collection_name.to_owned(), Some("username".to_owned()), Some("password".to_owned()));
        assert!(client.write_message(connect_with_credentials).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Connected));
    }
}
