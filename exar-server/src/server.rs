use super::*;

use exar::*;

use std::net::{ToSocketAddrs, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug)]
pub struct Server {
    credentials: Credentials,
    db: Arc<Mutex<Database>>,
    listener: TcpListener
}

impl Server {
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
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

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
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Server {
        self.credentials.username = Some(username.to_string());
        self.credentials.password = Some(password.to_string());
        self
    }

    pub fn listen(&self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = self.db.clone();
                    let config = self.credentials.clone();
                    thread::spawn(|| {
                        match Handler::new(stream, db, config) {
                            Ok(mut handler) => {
                                println!("Client connected..");
                                handler.run();
                                println!("Client disconnected..");
                            },
                            Err(err) => println!("Unable to accept client connection: {}", err)
                        }
                    });
                },
                Err(err) => println!("Client connection failed: {}", err)
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
