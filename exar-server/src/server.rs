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
    use super::super::*;

    #[test]
    fn test_constructor() {
        let db = Database::new(DatabaseConfig::default());
        assert!(Server::new(ServerConfig::default(), db).is_ok());

        let db = Database::new(DatabaseConfig::default());
        assert!(Server::bind("127.0.0.1:38581", db).is_ok());
    }
}
