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
///
/// let db = Database::new(DatabaseConfig::default());
/// let config = ServerConfig::default();
///
/// let mut server = Server::new(config, db).unwrap();
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
    pub fn new(config: ServerConfig, db: Database) -> Result<Server, DatabaseError> {
        let db = Arc::new(Mutex::new(db));
        match TcpListener::bind(&*config.address()) {
            Ok(listener) => Ok(Server {
                credentials: Credentials {
                    username: config.username,
                    password: config.password
                },
                db: db,
                listener: listener
            }),
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Creates a server database and binds it to the given address,
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn bind<A: ToSocketAddrs>(address: A, db: Database) -> Result<Server, DatabaseError> {
        let db = Arc::new(Mutex::new(db));
        match TcpListener::bind(address) {
            Ok(listener) => {
                Ok(Server {
                    credentials: Credentials {
                        username: None,
                        password: None
                    },
                    db: db,
                    listener: listener
                })
            },
            Err(err) => Err(DatabaseError::from_io_error(err))
        }
    }

    /// Returns a modified version of the server by setting its credentials to the given value.
    pub fn with_credentials(mut self, username: &str, password: &str) -> Server {
        self.credentials.username = Some(username.to_string());
        self.credentials.password = Some(password.to_string());
        self
    }

    /// Starts listening for incoming TCP connections.
    ///
    /// It will block the current thread indefinitely.
    pub fn listen(&self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = self.db.clone();
                    let config = self.credentials.clone();
                    thread::spawn(|| {
                        let peer_addr = match stream.peer_addr() {
                            Ok(addr) => Some(addr),
                            Err(_) => None
                        };
                        match Handler::new(stream, db, config) {
                            Ok(mut handler) => {
                                match peer_addr {
                                    Some(addr) => info!("Client connected: {}", addr),
                                    None => info!("Client connected: unknown peer address")
                                }
                                handler.run();
                                match peer_addr {
                                    Some(addr) => info!("Client disconnected: {}", addr),
                                    None => info!("Client disconnected: unknown peer address")
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
    use exar::*;
    use exar_net::*;
    use exar_testkit::*;
    use super::super::*;

    use std::fs::*;
    use std::net::{TcpStream, ToSocketAddrs};
    use std::thread;

    fn create_client<A: ToSocketAddrs>(addr: A) -> TcpMessageStream<TcpStream> {
        let stream  = TcpStream::connect(addr).expect("Unable to connect to the TCP stream");
        TcpMessageStream::new(stream).expect("Unable to create TCP message stream client")
    }

    #[test]
    fn test_constructor() {
        with_addr(&mut |addr| {
            let db = Database::new(DatabaseConfig::default());
            let mut config = ServerConfig::default();

            let addr_string = format!("{}", addr);
            let addr_parts: Vec<_> = addr_string.split(":").collect();
            config.host = addr_parts[0].parse().expect("Unable to parse host");
            config.port = addr_parts[1].parse().expect("Unable to parse port");

            assert!(Server::new(config, db).is_ok());
        });
    }

    #[test]
    fn test_constructor_failure() {
        let db = Database::new(DatabaseConfig::default());
        let mut config = ServerConfig::default();
        config.port = 1000;
        assert!(Server::new(config, db).is_err());
    }

    #[test]
    fn test_bind() {
        with_addr(&mut |addr| {
            let db = Database::new(DatabaseConfig::default());
            assert!(Server::bind(&addr, db).is_ok());
        });
    }

    #[test]
    fn test_bind_failure() {
        let db = Database::new(DatabaseConfig::default());
        assert!(Server::bind("127.0.0.1:1000", db).is_err());
    }

    #[test]
    fn test_connection() {
        with_addr(&mut |addr| {
            let collection_name = random_collection_name();
            let db = Database::new(DatabaseConfig::default());
            let server = Server::bind(addr, db).expect("Unable to start the TCP server");
            thread::spawn(move || {
                server.listen();
            });
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Connected));

            assert!(remove_file(format!("{}.log", collection_name)).is_ok());
            assert!(remove_file(format!("{}.index.log", collection_name)).is_ok());
        });
    }

    #[test]
    fn test_connection_with_credentials() {
        with_addr(&mut |addr| {
            let collection_name = random_collection_name();
            let db = Database::new(DatabaseConfig::default());
            let server = Server::bind(addr, db).expect("Unable to start the TCP server");
            let server = server.with_credentials("username", "password");
            thread::spawn(move || {
                server.listen();
            });
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Error(DatabaseError::AuthenticationError)));

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        Some("username".to_owned()), Some("password".to_owned()))).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Connected));

            assert!(remove_file(format!("{}.log", collection_name)).is_ok());
            assert!(remove_file(format!("{}.index.log", collection_name)).is_ok());
        });
    }
}
