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
    use super::super::*;

    use std::env;
    use std::fs::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream, ToSocketAddrs};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    static PORT: AtomicUsize = AtomicUsize::new(0);

    fn base_port() -> u16 {
        let cwd = env::current_dir().unwrap();
        let dirs = ["32-opt", "32-nopt", "musl-64-opt", "cross-opt",
                    "64-opt", "64-nopt", "64-opt-vg", "64-debug-opt",
                    "all-opt", "snap3", "dist"];
        dirs.iter().enumerate().find(|&(_, dir)| {
            cwd.to_str().unwrap().contains(dir)
        }).map(|p| p.0).unwrap_or(0) as u16 * 1000 + 19600
    }

    pub fn next_test_ip4() -> SocketAddr {
        let port = PORT.fetch_add(1, Ordering::SeqCst) as u16 + base_port();
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port))
    }

    fn each_ip(f: &mut FnMut(SocketAddr)) {
        f(next_test_ip4());
    }

    fn create_client<A: ToSocketAddrs>(addr: A) -> TcpMessageStream<TcpStream> {
        let stream  = TcpStream::connect(addr).expect("Unable to connect to the TCP stream");
        TcpMessageStream::new(stream).expect("Unable to create TCP message stream client")
    }

    #[test]
    fn test_constructor() {
        each_ip(&mut |addr| {
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
    fn test_bind() {
        each_ip(&mut |addr| {
            let db = Database::new(DatabaseConfig::default());
            assert!(Server::bind(&addr, db).is_ok());
        });
    }

    #[test]
    fn test_server() {
        each_ip(&mut |addr| {
            let collection_name = testkit::gen_collection_name();
            let db = Database::new(DatabaseConfig::default());
            let server = Server::bind(addr, db).expect("Unable to start the TCP server");
            thread::spawn(move || {
                server.listen();
            });
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(), None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Connected));

            assert!(remove_file(format!("{}.log", collection_name)).is_ok());
        });
    }
}
