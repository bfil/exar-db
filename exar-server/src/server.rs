use super::*;

use exar::*;

use std::net::{ToSocketAddrs, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug)]
pub struct Server {
    config: ServerConfig,
    db: Arc<Mutex<Database>>,
    listener: TcpListener
}

impl Server {
    pub fn new(config: ServerConfig, db: Database) -> Result<Server, DatabaseError> {
        let db = Arc::new(Mutex::new(db));
        match TcpListener::bind(&*config.address()) {
            Ok(listener) => Ok(Server {
                config: config,
                db: db,
                listener: listener
            }),
            Err(err) => Err(DatabaseError::IoError(err.kind(), format!("{}", err)))
        }
    }

    pub fn bind<A: ToSocketAddrs>(address: A, db: Database) -> Result<Server, DatabaseError> {
        let db = Arc::new(Mutex::new(db));
        match TcpListener::bind(address) {
            Ok(listener) => {
                match listener.local_addr() {
                    Ok(local_addr) => {
                        let address_string = local_addr.to_string();
                        let address_parts: Vec<_> = address_string.split(":").collect();
                        match address_parts.get(0) {
                            Some(&host) => Ok(Server {
                                config: ServerConfig {
                                    host: host.to_owned(),
                                    port: local_addr.port(),
                                    username: None,
                                    password: None
                                },
                                db: db,
                                listener: listener
                            }),
                            None => Err(DatabaseError::ConnectionError)
                        }

                    },
                    Err(err) => Err(DatabaseError::IoError(err.kind(), format!("{}", err)))
                }
            },
            Err(err) => Err(DatabaseError::IoError(err.kind(), format!("{}", err)))
        }
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Server {
        self.config.username = Some(username.to_string());
        self.config.password = Some(password.to_string());
        self
    }

    pub fn listen(&self) {
        println!("Server listening on {}..", self.config.address());
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = self.db.clone();
                    let config = self.config.clone();
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
        println!("Server shutting down..");
    }
}
