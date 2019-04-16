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
        let stream = TcpMessageStream::new(stream)?;
        let state  = if credentials.username.is_some() && credentials.password.is_some() {
                         Arc::new(Mutex::new(State::AuthenticationRequired(db)))
                     } else {
                         Arc::new(Mutex::new(State::Connected(db)))
                     };
        Ok(Handler { credentials, stream, state })
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
            State::Subscribed(_, _, ref unsubscribe_handle) => unsubscribe_handle.unsubscribe(),
            _                                               => Ok(())
        }
    }

    fn update_state(&mut self, state: State) {
        *self.state.lock().unwrap() = state;
    }

    fn verify_authentication(&self, username: String, password: String) -> bool {
        self.credentials.username == Some(username) && self.credentials.password == Some(password)
    }

    fn handle_message(&mut self, message: TcpMessage) -> DatabaseResult<()> {
        let state = self.state.lock().unwrap().clone();
        match (message, state) {
            (TcpMessage::Authenticate(given_username, given_password), State::AuthenticationRequired(db)) => {
                if self.verify_authentication(given_username, given_password) {
                    self.update_state(State::Connected(db));
                    self.stream.write_message(TcpMessage::Authenticated)
                } else {
                    Err(DatabaseError::AuthenticationError)
                }
            },
            (TcpMessage::Select(collection_name), State::Connected(db)) => {
                match db.lock().unwrap().collection(&collection_name) {
                    Ok(collection) => {
                        let connection = Connection::new(collection);
                        self.update_state(State::CollectionSelected(db.clone(), connection));
                        self.stream.write_message(TcpMessage::Selected)
                    },
                    Err(err) => Err(err)
                }
            },
            (TcpMessage::Select(collection_name), State::CollectionSelected(db, _)) => {
                match db.lock().unwrap().collection(&collection_name) {
                    Ok(collection) => {
                        let connection = Connection::new(collection);
                        self.update_state(State::CollectionSelected(db.clone(), connection));
                        self.stream.write_message(TcpMessage::Selected)
                    },
                    Err(err) => Err(err)
                }
            },
            (TcpMessage::Publish(event), State::CollectionSelected(_, connection)) => {
                let event_id = connection.publish(event)?;
                self.stream.write_message(TcpMessage::Published(event_id))
            },
            (TcpMessage::Subscribe(live, offset, limit, tag), State::CollectionSelected(db, connection)) => {
                let subscription = connection.subscribe(Query::new(live, offset, limit, tag))?;
                let (event_stream, unsubscribe_handle) = subscription.into_event_stream_and_unsubscribe_handle();
                self.stream.write_message(TcpMessage::Subscribed)?;
                if live {
                    let cloned_db         = db.clone();
                    let cloned_state      = self.state.clone();
                    let cloned_connection = connection.clone();
                    self.update_state(State::Subscribed(db, connection, unsubscribe_handle));
                    let mut stream        = self.stream.try_clone()?;
                    thread::spawn(move || {
                        for event in event_stream {
                            let send_result = stream.write_message(TcpMessage::Event(event));
                            if send_result.is_err() { return send_result }
                        }
                        *cloned_state.lock().unwrap() = State::CollectionSelected(cloned_db, cloned_connection);
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
            (TcpMessage::Drop(collection_name), State::Connected(db)) => {
                db.lock().unwrap().drop_collection(&collection_name)?;
                self.stream.write_message(TcpMessage::Dropped)
            },
            (TcpMessage::Drop(collection_name), State::CollectionSelected(db, _)) => {
                db.lock().unwrap().drop_collection(&collection_name)?;
                self.stream.write_message(TcpMessage::Dropped)
            },
            (TcpMessage::Unsubscribe, State::Subscribed(_, _, unsubscribe_handle)) => {
                unsubscribe_handle.unsubscribe()
            },
            (_, State::AuthenticationRequired(_)) => Err(DatabaseError::AuthenticationError),
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
    /// The connection requires authentication.
    AuthenticationRequired(Arc<Mutex<Database>>),
    /// The connection has been established.
    Connected(Arc<Mutex<Database>>),
    /// The connection has been established and a collection has been selected.
    CollectionSelected(Arc<Mutex<Database>>, Connection),
    /// The connection is subscribed to an event stream.
    Subscribed(Arc<Mutex<Database>>, Connection, UnsubscribeHandle)
}

impl ToString for State {
    fn to_string(&self) -> String {
        match *self {
            State::AuthenticationRequired(_) => "AuthenticationRequired".to_owned(),
            State::Connected(_)              => "Connected".to_owned(),
            State::CollectionSelected(_, _)  => "CollectionSelected".to_owned(),
            State::Subscribed(_, _, _)       => "Subscribed".to_owned()
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

        let handler    = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Select(collection_name.to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Selected));

        drop(client);

        handler.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_connection_with_credentials() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();
        let credentials     = Credentials::new("username", "password");

        let handler    = create_handler(addr, credentials.clone());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Authenticate("username".to_owned(), "wrong_password".to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Error(DatabaseError::AuthenticationError)));

        assert!(client.write_message(TcpMessage::Authenticate("username".to_owned(), "password".to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Authenticated));

        assert!(client.write_message(TcpMessage::Select(collection_name.to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Selected));

        drop(client);

        handler.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_select_and_drop() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();

        let handler    = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Select(collection_name.to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Selected));

        assert!(client.write_message(TcpMessage::Drop(collection_name.to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Dropped));

        drop(client);

        handler.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_publish_and_subscribe() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();

        let handler    = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Select(collection_name.to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Selected));

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_timestamp(1234567890);

        assert!(client.write_message(TcpMessage::Publish(event.clone())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Published(1)));

        assert!(client.write_message(TcpMessage::Subscribe(false, 0, None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Subscribed));

        assert_eq!(client.read_message(), Ok(TcpMessage::Event(event.clone().with_id(1))));
        assert_eq!(client.read_message(), Ok(TcpMessage::EndOfEventStream));

        drop(client);

        handler.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_unsubscribe() {
        let addr            = find_available_addr();
        let collection_name = random_collection_name();

        let handler    = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Select(collection_name.to_owned())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Selected));

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_timestamp(1234567890);

        assert!(client.write_message(TcpMessage::Publish(event.clone())).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Published(1)));

        assert!(client.write_message(TcpMessage::Subscribe(true, 0, None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Subscribed));

        assert_eq!(client.read_message(), Ok(TcpMessage::Event(event.clone().with_id(1))));

        assert!(client.write_message(TcpMessage::Unsubscribe).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::EndOfEventStream));

        drop(client);

        handler.join().expect("Unable to join server thread");
    }

    #[test]
    fn test_unexpected_tcp_message() {
        let addr       = find_available_addr();
        let handler    = create_handler(addr, Credentials::empty());
        let mut client = create_client(addr);

        assert!(client.write_message(TcpMessage::Subscribe(false, 0, None, None)).is_ok());
        assert_eq!(client.read_message(), Ok(TcpMessage::Error(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned()))));

        drop(client);

        handler.join().expect("Unable to join server thread");
    }
}
