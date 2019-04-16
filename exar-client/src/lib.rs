//! # Exar DB's client
//! This module contains a client implementation that uses Exar DB's TCP protocol.
//!
//! ## Client Initialization
//! ```no_run
//! extern crate exar_client;
//!
//! # fn main() {
//! use exar_client::*;
//!
//! let addr   = "127.0.0.1:38580";
//! let client = Client::connect(addr, "collection", Some("username"), Some("password")).unwrap();
//! # }
//! ```
//! ## Publishing events
//! ```no_run
//! extern crate exar;
//! extern crate exar_client;
//!
//! # fn main() {
//! use exar::*;
//! use exar_client::*;
//!
//! let addr       = "127.0.0.1:38580";
//! let mut client = Client::connect(addr, "collection", Some("username"), Some("password")).expect("Unable to connect");
//!
//! let event = Event::new("payload", vec![Tag::new("tag1"), Tag::new("tag2")]);
//! match client.publish(event) {
//!     Ok(event_id) => println!("Published event with ID: {}", event_id),
//!     Err(err)     => panic!("Unable to publish event: {}", err)
//! };
//! # }
//! ```
//! ## Querying events
//! ```no_run
//! extern crate exar;
//! extern crate exar_client;
//!
//! # fn main() {
//! use exar::*;
//! use exar_client::*;
//!
//! let addr       = "127.0.0.1:38580";
//! let mut client = Client::connect(addr, "collection", Some("username"), Some("password")).expect("Unable to connect");
//!
//! let query        = Query::live().offset(0).limit(10).by_tag(Tag::new("tag1"));
//! let event_stream = client.subscribe(query).expect("Unable to subscribe");
//! for event in event_stream {
//!     println!("Received event: {}", event);
//! }
//! # }
//! ```
//! ## Selecting and dropping collections
//! ```no_run
//! extern crate exar;
//! extern crate exar_client;
//!
//! # fn main() {
//! use exar::*;
//! use exar_client::*;
//!
//! let addr       = "127.0.0.1:38580";
//! let mut client = Client::connect(addr, "collection", Some("username"), Some("password")).expect("Unable to connect");
//!
//! client.select_collection("another_collection").expect("Unable to select collection");
//! client.drop_collection("collection").expect("Unable to drop collection");
//! # }
//! ```

extern crate exar;
extern crate exar_net;

#[cfg(test)] extern crate exar_testkit;
#[macro_use] extern crate log;

#[cfg(test)] mod testkit;

use exar::*;
use exar_net::*;

use std::io::ErrorKind;
use std::net::{ToSocketAddrs, TcpStream};
use std::sync::mpsc::channel;
use std::thread;

/// # Exar DB's client
pub struct Client {
    stream: TcpMessageStream<TcpStream>
}

impl Client {
    /// Connects to the given address and collection, optionally authenticating using the credentials provided,
    /// it returns a `Client` or a `DatabaseError` if a failure occurs.
    pub fn connect<A: ToSocketAddrs>(address: A, collection_name: &str, username: Option<&str>, password: Option<&str>) -> DatabaseResult<Client> {
        let stream = TcpStream::connect(address).map_err(DatabaseError::from_io_error)?;
        let mut client = Client { stream: TcpMessageStream::new(stream)? };
        match (username, password) {
            (Some(username), Some(password)) => client.authenticate(username, password)?,
            _                                => ()
        }
        client.select_collection(collection_name)?;
        Ok(client)
    }

    fn authenticate(&mut self, username: &str, password: &str) -> DatabaseResult<()> {
        self.stream.write_message(TcpMessage::Authenticate(username.to_owned(), password.to_owned()))?;
        match self.stream.read_message() {
            Ok(TcpMessage::Authenticated) => Ok(()),
            Ok(TcpMessage::Error(error))  => Err(error),
            Ok(_)                         => Err(DatabaseError::ConnectionError),
            Err(err)                      => Err(err)
        }
    }

    /// Selects the given collection
    /// it returns a `Client` or a `DatabaseError` if a failure occurs.
    pub fn select_collection(&mut self, collection_name: &str) -> DatabaseResult<()> {
        self.stream.write_message(TcpMessage::Select(collection_name.to_owned()))?;
        match self.stream.read_message() {
            Ok(TcpMessage::Selected)      => Ok(()),
            Ok(TcpMessage::Error(error))  => Err(error),
            Ok(_)                         => Err(DatabaseError::ConnectionError),
            Err(err)                      => Err(err)
        }
    }

    /// Publishes an event and returns the `id` for the event created
    /// or a `DatabaseError` if a failure occurs.
    pub fn publish(&mut self, event: Event) -> DatabaseResult<u64> {
        self.stream.write_message(TcpMessage::Publish(event))?;
        match self.stream.read_message() {
            Ok(TcpMessage::Published(event_id)) => Ok(event_id),
            Ok(TcpMessage::Error(error))        => Err(error),
            Ok(_)                               => Err(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned())),
            Err(err)                            => Err(err)
        }
    }

    /// Subscribes using the given query and returns an event stream
    /// or a `DatabaseError` if a failure occurs.
    pub fn subscribe(&mut self, query: Query) -> DatabaseResult<EventStream> {
        let subscribe_message = TcpMessage::Subscribe(query.live_stream, query.offset, query.limit, query.tag);
        self.stream.write_message(subscribe_message)?;
        match self.stream.read_message()? {
            TcpMessage::Subscribed => {
                let (sender, receiver) = channel();
                let cloned_stream = self.stream.try_clone()?;
                thread::spawn(move || {
                    for message in cloned_stream.messages() {
                        match message {
                            Ok(TcpMessage::Event(event))     => match sender.send(EventStreamMessage::Event(event)) {
                                                                    Ok(_)    => continue,
                                                                    Err(err) => error!("Unable to send event to the event stream: {}", err)
                                                                },
                            Ok(TcpMessage::EndOfEventStream) => {
                                                                    let _ = sender.send(EventStreamMessage::End);
                                                                },
                            Ok(TcpMessage::Error(error))     => error!("Received error from TCP stream: {}", error),
                            Ok(message)                      => error!("Unexpected TCP message: {}", message),
                            Err(err)                         => error!("Unable to read TCP message from stream: {}", err)
                        };
                        break
                    }
                });
                Ok(EventStream::new(receiver))
            },
            TcpMessage::Error(err) => Err(err),
            _                      => Err(DatabaseError::SubscriptionError)
        }
    }

    /// Unsubscribes from the event stream
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn unsubscribe(&mut self) -> DatabaseResult<()> {
        self.stream.write_message(TcpMessage::Unsubscribe)
    }

    /// Drops the currently selected collection
    /// or returns a `DatabaseError` if a failure occurs.
    pub fn drop_collection(&mut self, collection_name: &str) -> DatabaseResult<()> {
        self.stream.write_message(TcpMessage::Drop(collection_name.to_owned()))?;
        match self.stream.read_message() {
            Ok(TcpMessage::Dropped)      => Ok(()),
            Ok(TcpMessage::Error(error)) => Err(error),
            Ok(_)                        => Err(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned())),
            Err(err)                     => Err(err)
        }
    }

    /// Closes the connection.
    pub fn close(self) {
        drop(self)
    }
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_connect() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected)
        ]);

        assert!(Client::connect(addr, "collection", None, None).is_ok());
    }

    #[test]
    fn test_connect_failure() {
        let addr = find_available_addr();

        let connection_refused_error = DatabaseError::IoError(ErrorKind::ConnectionRefused, "Connection refused (os error 61)".to_owned());
        assert_eq!(Client::connect(addr, "collection", None, None).err(), Some(connection_refused_error));
    }

    #[test]
    fn test_connect_with_authentication() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Authenticate("username".to_owned(), "password".to_owned())),
            StreamAction::Write(TcpMessage::Authenticated),
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected)
        ]);

        assert!(Client::connect(addr, "collection", Some("username"), Some("password")).is_ok());
    }

    #[test]
    fn test_connect_with_authentication_failure() {
        let addr                     = find_available_addr();
        let connection_refused_error = DatabaseError::IoError(ErrorKind::ConnectionRefused, "Connection refused (os error 61)".to_owned());

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Authenticate("username".to_owned(), "password".to_owned())),
            StreamAction::Write(TcpMessage::Error(connection_refused_error.clone()))
        ]);

        assert_eq!(Client::connect(addr, "collection", Some("username"), Some("password")).err(), Some(connection_refused_error));
    }

    #[test]
    fn test_select_collection() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Select("another_collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected)
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.select_collection("another_collection"), Ok(()));
    }

    #[test]
    fn test_select_collection_failure() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Select("another_collection".to_owned())),
            StreamAction::Write(TcpMessage::Error(DatabaseError::ConnectionError))
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.select_collection("another_collection"), Err(DatabaseError::ConnectionError));
    }

    #[test]
    fn test_publish() {
        let addr = find_available_addr();

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_timestamp(1234567890);

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Publish(event.clone())),
            StreamAction::Write(TcpMessage::Published(1))
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.publish(event.clone()), Ok(1));
    }

    #[test]
    fn test_publish_failure() {
        let addr = find_available_addr();

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_timestamp(1234567890);
        let validation_error = ValidationError::new("validation error");

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Publish(event.clone())),
            StreamAction::Write(TcpMessage::Error(DatabaseError::ValidationError(validation_error.clone())))
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.publish(event.clone()), Err(DatabaseError::ValidationError(validation_error)));
    }

    #[test]
    fn test_subscribe() {
        let addr = find_available_addr();

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_timestamp(1234567890);

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Subscribe(true, 0, None, None)),
            StreamAction::Write(TcpMessage::Subscribed),
            StreamAction::Write(TcpMessage::Event(event.clone().with_id(1))),
            StreamAction::Write(TcpMessage::Event(event.clone().with_id(2))),
            StreamAction::Write(TcpMessage::EndOfEventStream)
        ]);

        let mut client       = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        let mut event_stream = client.subscribe(Query::live()).expect("Unable to subscribe");
        assert_eq!(event_stream.next(), Some(event.clone().with_id(1)));
        assert_eq!(event_stream.next(), Some(event.clone().with_id(2)));
        assert_eq!(event_stream.next(), None);
    }

    #[test]
    fn test_subscribe_failure() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Subscribe(true, 0, None, None)),
            StreamAction::Write(TcpMessage::Error(DatabaseError::SubscriptionError))
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.subscribe(Query::live()).err(), Some(DatabaseError::SubscriptionError));
    }

    #[test]
    fn test_unsubscribe() {
        let addr = find_available_addr();

        let event = Event::new("data", vec![Tag::new("tag1"), Tag::new("tag2")]).with_timestamp(1234567890);

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Subscribe(true, 0, None, None)),
            StreamAction::Write(TcpMessage::Subscribed),
            StreamAction::Write(TcpMessage::Event(event.clone().with_id(1))),
            StreamAction::Write(TcpMessage::Event(event.clone().with_id(2))),
            StreamAction::Read(TcpMessage::Unsubscribe),
            StreamAction::Write(TcpMessage::EndOfEventStream)
        ]);

        let mut client       = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        let mut event_stream = client.subscribe(Query::live()).expect("Unable to subscribe");
        assert_eq!(event_stream.next(), Some(event.clone().with_id(1)));
        assert_eq!(event_stream.next(), Some(event.clone().with_id(2)));

        assert!(client.unsubscribe().is_ok());

        assert_eq!(event_stream.next(), None);
    }

    #[test]
    fn test_drop_collection() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Drop("another_collection".to_owned())),
            StreamAction::Write(TcpMessage::Dropped),
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.drop_collection("another_collection"), Ok(()));
    }

    #[test]
    fn test_drop_collection_failure() {
        let addr = find_available_addr();

        stub_server(addr.clone(), vec![
            StreamAction::Read(TcpMessage::Select("collection".to_owned())),
            StreamAction::Write(TcpMessage::Selected),
            StreamAction::Read(TcpMessage::Drop("another_collection".to_owned())),
            StreamAction::Write(TcpMessage::Error(DatabaseError::ConnectionError))
        ]);

        let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
        assert_eq!(client.drop_collection("another_collection"), Err(DatabaseError::ConnectionError));
    }
}
