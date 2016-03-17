use super::*;

use exar::*;
use exar_net::*;

use std::io::ErrorKind;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub struct Handler {
    credentials: Credentials,
    stream: TcpMessageStream<TcpStream>,
    state: State
}

impl Handler {
    pub fn new(stream: TcpStream, db: Arc<Mutex<Database>>, credentials: Credentials) -> Result<Handler, DatabaseError> {
        TcpMessageStream::new(stream).and_then(|stream| {
            Ok(Handler {
                credentials: credentials,
                stream: stream,
                state: State::Idle(db)
            })
        })
    }

    pub fn run(&mut self) {
        match self.stream.try_clone() {
            Ok(stream) => {
                for message in stream.messages() {
                    let _ = match message {
                        Ok(message) => match self.recv(message) {
                            Ok(result) => self.send(result),
                            Err(err) => self.fail(err)
                        },
                        Err(err) => self.fail(err)
                    };
                }
            },
            Err(err) => println!("Unable to accept client connection: {}", err)
        }
    }

    fn update_state(&mut self, state: State) {
        self.state = state;
    }

    fn needs_authentication(&self) -> bool {
        self.credentials.username.is_some() && self.credentials.password.is_some()
    }

    fn verify_authentication(&self, username: Option<String>, password: Option<String>) -> bool {
        if self.needs_authentication() {
            if username.is_some() && password.is_some() {
                self.credentials.username == username && self.credentials.password == password
            } else { false }
        } else { true }
    }

    fn recv(&mut self, message: TcpMessage) -> Result<ActionResult, DatabaseError> {
        match (message, self.state.clone()) {
            (TcpMessage::Connect(collection_name, given_username, given_password), State::Idle(db)) => {
                if self.verify_authentication(given_username, given_password) {
                    match db.lock().unwrap().connect(&collection_name) {
                        Ok(connection) => {
                            self.update_state(State::Connected(connection));
                            Ok(ActionResult::Connected)
                        },
                        Err(err) => Err(err)
                    }
                } else {
                    Err(DatabaseError::AuthenticationError)
                }
            },
            (TcpMessage::Publish(event), State::Connected(connection)) => {
                connection.publish(event).and_then(|event_id| {
                    Ok(ActionResult::Published(event_id))
                })
            },
            (TcpMessage::Subscribe(live, offset, limit, tag), State::Connected(connection)) => {
                connection.subscribe(Query::new(live, offset, limit, tag)).and_then(|event_stream| {
                    Ok(ActionResult::EventStream(event_stream))
                })
            },
            _ => Err(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned()))
        }
    }

    fn send(&mut self, result: ActionResult) -> Result<(), DatabaseError> {
        match result {
            ActionResult::Connected => self.stream.send_message(TcpMessage::Connected),
            ActionResult::Published(event_id) => self.stream.send_message(TcpMessage::Published(event_id)),
            ActionResult::EventStream(event_stream) => {
                self.stream.send_message(TcpMessage::Subscribed).and_then(|_| {
                    for event in event_stream {
                        let send_result = self.stream.send_message(TcpMessage::Event(event));
                        if send_result.is_err() { return send_result }
                    }
                    self.stream.send_message(TcpMessage::EndOfEventStream)
                })
            }
        }
    }

    fn fail(&mut self, error: DatabaseError) -> Result<(), DatabaseError> {
        let error = TcpMessage::Error(error);
        self.stream.send_message(error)
    }
}

#[derive(Clone)]
pub enum State {
    Idle(Arc<Mutex<Database>>),
    Connected(Connection)
}

impl ToString for State {
    fn to_string(&self) -> String {
        match *self {
            State::Idle(_) => "Idle".to_owned(),
            State::Connected(_) => "Connected".to_owned()
        }
    }
}

pub enum ActionResult {
    Connected,
    Published(usize),
    EventStream(EventStream)
}

#[cfg(test)]
mod tests {
    use exar::*;
    use exar_net::*;
    use exar_testkit::*;
    use super::super::*;

    use std::fs::*;
    use std::io::ErrorKind;
    use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    fn create_handler(addr: SocketAddr, credentials: Credentials) -> JoinHandle<()> {
        let db = Arc::new(Mutex::new(Database::new(DatabaseConfig::default())));
        let handle = thread::spawn(move || {
            let listener = TcpListener::bind(addr).expect("Unable to bind to address");
            match listener.accept() {
                Ok((stream, _)) => {
                    let mut handler = Handler::new(stream, db, credentials).expect("Unable to create TCP connection handler");
                    handler.run();
                },
                Err(err) => panic!("Error: {}", err)
            }
        });
        thread::sleep(Duration::from_millis(100));
        handle
    }

    fn create_client<A: ToSocketAddrs>(addr: A) -> TcpMessageStream<TcpStream> {
        let stream  = TcpStream::connect(addr).expect("Unable to connect to the TCP stream");
        TcpMessageStream::new(stream).expect("Unable to create TCP message stream client")
    }

    #[test]
    fn test_connection() {
        with_addr(&mut |addr| {
            let collection_name = random_collection_name();

            let handle = create_handler(addr, Credentials::empty());
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Connected));

            drop(client);

            assert!(remove_file(format!("{}.log", collection_name)).is_ok());

             handle.join().expect("Unable to join server thread");
        });
    }

    #[test]
    fn test_connection_with_credentials() {
        with_addr(&mut |addr| {
            let collection_name = random_collection_name();

            let handle = create_handler(addr, Credentials::new("username", "password"));
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Error(DatabaseError::AuthenticationError)));

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        Some("username".to_owned()), Some("password".to_owned()))).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Connected));

            drop(client);

            assert!(remove_file(format!("{}.log", collection_name)).is_ok());

            handle.join().expect("Unable to join server thread");
        });
    }

    #[test]
    fn test_publish_and_subscribe() {
        with_addr(&mut |addr| {
            let collection_name = random_collection_name();

            let handle = create_handler(addr, Credentials::empty());
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Connect(collection_name.to_owned(),
                                        None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Connected));

            let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);

            assert!(client.send_message(TcpMessage::Publish(event.clone())).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Published(1)));

            assert!(client.send_message(TcpMessage::Subscribe(false, 0, None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Subscribed));
            if let Ok(TcpMessage::Event(received_event)) = client.recv_message() {
                assert_eq!(received_event, event.with_id(1));
                assert_eq!(client.recv_message(), Ok(TcpMessage::EndOfEventStream));
            } else {
                panic!("Unable to receive event");
            }

            drop(client);

            assert!(remove_file(format!("{}.log", collection_name)).is_ok());

             handle.join().expect("Unable to join server thread");
        });
    }

    #[test]
    fn test_unexpected_tcp_message() {
        with_addr(&mut |addr| {
            let handle = create_handler(addr, Credentials::empty());
            let mut client = create_client(addr);

            assert!(client.send_message(TcpMessage::Subscribe(false, 0, None, None)).is_ok());
            assert_eq!(client.recv_message(), Ok(TcpMessage::Error(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned()))));

            drop(client);

             handle.join().expect("Unable to join server thread");
        });
    }
}
