use super::*;

use exar::*;
use exar_net::*;

use std::io::ErrorKind;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;

/// Exar DB's server connection handler.
///
/// It manages the TCP stream associated to a single remote connection.
pub struct Handler {
    credentials: Credentials,
    stream: TcpMessageStream<TcpStream>,
    state: Arc<Mutex<State>>
}

impl Handler {
    /// Creates a connection handler using the given TCP stream, database and credentials,
    /// or a `DatabaseError` if a failure occurs.
    pub fn new(stream: TcpStream, db: Arc<Mutex<Database>>, credentials: Credentials) -> DatabaseResult<Handler> {
        TcpMessageStream::new(stream).and_then(|stream| {
            let state = Arc::new(Mutex::new(State::Idle(db)));
            Ok(Handler { credentials, stream, state })
        })
    }

    /// Runs the connection handler which processes one incoming TCP message at a time.
    pub fn run(mut self) -> DatabaseResult<()> {
        let stream = self.stream.try_clone()?;
        for message in stream.messages() {
            let _ = match message {
                Ok(message) => match self.handle_message(message) {
                    Ok(())   => Ok(()),
                    Err(err) => self.send_error_message(err)
                },
                Err(err) => self.send_error_message(err)
            };
        }
        match *self.state.lock().unwrap() {
            State::Subscribed(_, ref unsubscribe_handle) => unsubscribe_handle.unsubscribe(),
            _                                            => Ok(())
        }
    }

    fn update_state(&mut self, state: State) {
        *self.state.lock().unwrap() = state;
    }

    fn requires_authentication(&self) -> bool {
        self.credentials.username.is_some() && self.credentials.password.is_some()
    }

    fn verify_authentication(&self, username: Option<String>, password: Option<String>) -> bool {
        if self.requires_authentication() {
            if username.is_some() && password.is_some() {
                self.credentials.username == username && self.credentials.password == password
            } else { false }
        } else { true }
    }

    fn handle_message(&mut self, message: TcpMessage) -> DatabaseResult<()> {
        let state = self.state.lock().unwrap().clone();
        match (message, state) {
            (TcpMessage::Connect(collection_name, given_username, given_password), State::Idle(db)) => {
                if self.verify_authentication(given_username, given_password) {
                    match db.lock().unwrap().collection(&collection_name) {
                        Ok(collection) => {
                            let connection = Connection::new(collection);
                            self.update_state(State::Connected(connection));
                            self.stream.write_message(TcpMessage::Connected)
                        },
                        Err(err) => Err(err)
                    }
                } else {
                    Err(DatabaseError::AuthenticationError)
                }
            },
            (TcpMessage::Publish(event), State::Connected(connection)) => {
                let event_id = connection.publish(event)?;
                self.stream.write_message(TcpMessage::Published(event_id))
            },
            (TcpMessage::Subscribe(live, offset, limit, tag), State::Connected(connection)) => {
                let subscription = connection.subscribe(Query::new(live, offset, limit, tag))?;
                let (event_stream, unsubscribe_handle) = subscription.into_event_stream_and_unsubscribe_handle();
                self.stream.write_message(TcpMessage::Subscribed)?;
                if live {
                    let cloned_state      = self.state.clone();
                    let cloned_connection = connection.clone();
                    self.update_state(State::Subscribed(connection, unsubscribe_handle));
                    let mut stream        = self.stream.try_clone()?;
                    thread::spawn(move || {
                        for event in event_stream {
                            let send_result = stream.write_message(TcpMessage::Event(event));
                            if send_result.is_err() { return send_result }
                        }
                        *cloned_state.lock().unwrap() = State::Connected(cloned_connection);
                        stream.write_message(TcpMessage::EndOfEventStream)
                    });
                    Ok(())
                } else {
                    for event in event_stream {
                        let send_result = self.stream.write_message(TcpMessage::Event(event));
                        if send_result.is_err() { return send_result }
                    }
                    self.stream.write_message(TcpMessage::EndOfEventStream)
                }
            },
            (TcpMessage::Unsubscribe, State::Subscribed(_, unsubscribe_handle)) => {
                unsubscribe_handle.unsubscribe()
            },
            _ => Err(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned()))
        }
    }

    fn send_error_message(&mut self, error: DatabaseError) -> DatabaseResult<()> {
        self.stream.write_message(TcpMessage::Error(error))
    }
}

/// A list specifying categories of connection state.
#[derive(Clone)]
pub enum State {
    /// The connection is idle and awaiting a `Connect` message.
    Idle(Arc<Mutex<Database>>),
    /// The connection has been established.
    Connected(Connection),
    /// The connection is subscribed to an event stream.
    Subscribed(Connection, UnsubscribeHandle)
}

impl ToString for State {
    fn to_string(&self) -> String {
        match *self {
            State::Idle(_)          => "Idle".to_owned(),
            State::Connected(_)     => "Connected".to_owned(),
            State::Subscribed(_, _) => "Subscribed".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    use std::io::ErrorKind;

    #[test]
    fn test_connection() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();

        let handle     = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Connect(collection_name.to_owned(),
                                                         None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Connected));

        drop(client);

        handle.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_connection_with_credentials() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();

        let handle     = create_handler(addr, Credentials::new("username", "password"));
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Connect(collection_name.to_owned(),
                                                         None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Error(DatabaseError::AuthenticationError)));

        assert!(client.write_message(TcpMessage::Connect(collection_name.to_owned(),
                                                         Some("username".to_owned()), Some("password".to_owned()))).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Connected));

        drop(client);

        handle.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_publish_and_subscribe() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();

        let handle     = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Connect(collection_name.to_owned(),
                                                         None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Connected));

        let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);

        assert!(client.write_message(TcpMessage::Publish(event.clone())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Published(1)));

        assert!(client.write_message(TcpMessage::Subscribe(false, 0, None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Subscribed));
        if let Ok(TcpMessage::Event(received_event)) = client.read_message() {
            assert_eq!(received_event, event.with_id(1));
            assert_eq!(client.read_message(), Ok(TcpMessage::EndOfEventStream));
        } else {
            panic!("Unable to receive event");
        }

        drop(client);

        handle.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_unexpected_tcp_message() {
        let addr       = find_available_addr();
        let handle     = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Subscribe(false, 0, None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Error(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned()))));

        drop(client);

        handle.join().expect("Unable to join server thread");
    }
}
