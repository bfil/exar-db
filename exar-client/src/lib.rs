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
//! let addr = "127.0.0.1:38580";
//! let client = Client::connect(addr, "test", Some("username"), Some("password")).unwrap();
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
//! let addr = "127.0.0.1:38580";
//! let mut client = Client::connect(addr, "test", Some("username"), Some("password")).expect("Unable to connect");
//!
//! let event = Event::new("payload", vec!["tag1", "tag2"]);
//!
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
//! let addr = "127.0.0.1:38580";
//! let mut client = Client::connect(addr, "test", Some("username"), Some("password")).expect("Unable to connect");
//!
//! let query        = Query::live().offset(0).limit(10).by_tag("tag1");
//! let event_stream = client.subscribe(query).expect("Unable to subscribe");
//! for event in event_stream {
//!     println!("Received event: {}", event);
//! }
//! # }
//! ```

extern crate exar;
extern crate exar_net;

#[cfg(test)]
extern crate exar_testkit;

#[macro_use]
extern crate log;

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
    /// Connects to the given address and collection, optionally using the credentials provided,
    /// it returns a `Client` or a `DatabaseError` if a failure occurs.
    pub fn connect<A: ToSocketAddrs>(address: A, collection_name: &str, username: Option<&str>, password: Option<&str>) -> DatabaseResult<Client> {
        let stream = TcpStream::connect(address).map_err(DatabaseError::from_io_error)?;
        let mut stream = TcpMessageStream::new(stream)?;
        let username = username.map(|u| u.to_owned());
        let password = password.map(|p| p.to_owned());
        let connection_message = TcpMessage::Connect(collection_name.to_owned(), username, password);
        stream.write_message(connection_message)?;
        match stream.read_message() {
            Ok(TcpMessage::Connected)    => Ok(Client { stream }),
            Ok(TcpMessage::Error(error)) => Err(error),
            Ok(_)                        => Err(DatabaseError::ConnectionError),
            Err(err)                     => Err(err)
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

    /// Unsubscribed from the event stream
    /// or a `DatabaseError` if a failure occurs.
    pub fn unsubscribe(&mut self) -> DatabaseResult<()> {
        self.stream.write_message(TcpMessage::Unsubscribe)
    }

    /// Closes the connection.
    pub fn close(self) {
        drop(self)
    }
}

#[cfg(test)]
mod tests {
    use exar::*;
    use exar_net::*;
    use exar_testkit::*;
    use super::*;

    use std::net::{TcpListener, ToSocketAddrs};
    use std::thread;
    use std::time::Duration;

    enum StreamAction {
        Read(TcpMessage),
        Write(TcpMessage)
    }

    fn stub_server<A: Send + ToSocketAddrs + 'static>(addr: A, actions: Vec<StreamAction>) {
        thread::spawn(|| {
            let listener = TcpListener::bind(addr).expect("Unable to bind to address");
            match listener.accept() {
                Ok((stream, _)) => {
                    let mut stream = TcpMessageStream::new(stream).expect("Unable to create message stream");
                    for action in actions {
                        match action {
                            StreamAction::Read(message)  => assert_eq!(stream.read_message(), Ok(message)),
                            StreamAction::Write(message) => assert!(stream.write_message(message).is_ok())
                        }
                        thread::sleep(Duration::from_millis(10));
                    }
                },
                Err(err) => panic!("Error: {}", err)
            }
        });
        thread::sleep(Duration::from_millis(100));
    }

    #[test]
    fn test_connect() {
        with_addr(&mut |addr| {

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
            ]);

            assert!(Client::connect(addr, "collection", None, None).is_ok());
        });
    }

    #[test]
    fn test_connect_with_authentication() {
        with_addr(&mut |addr| {

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect(
                    "collection".to_owned(), Some("username".to_owned()), Some("password".to_owned()
                ))),
                StreamAction::Write(TcpMessage::Connected),
            ]);

            assert!(Client::connect(addr, "collection", Some("username"), Some("password")).is_ok());
        });
    }

    #[test]
    fn test_connect_failure() {
        with_addr(&mut |addr| {

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Error(DatabaseError::ConnectionError))
            ]);

            assert_eq!(Client::connect(addr, "collection", None, None).err(), Some(DatabaseError::ConnectionError));
        });
    }

    #[test]
    fn test_publish() {
        with_addr(&mut |addr| {

            let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
                StreamAction::Read(TcpMessage::Publish(event.clone())),
                StreamAction::Write(TcpMessage::Published(1))
            ]);

            let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
            assert_eq!(client.publish(event.clone()), Ok(1));
        });
    }

    #[test]
    fn test_publish_failure() {
        with_addr(&mut |addr| {

            let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);
            let validation_error = ValidationError::new("validation error");

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
                StreamAction::Read(TcpMessage::Publish(event.clone())),
                StreamAction::Write(TcpMessage::Error(DatabaseError::ValidationError(validation_error.clone())))
            ]);

            let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
            assert_eq!(client.publish(event.clone()), Err(DatabaseError::ValidationError(validation_error)));
        });
    }

    #[test]
    fn test_subscribe() {
        with_addr(&mut |addr| {

            let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
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
        });
    }

    #[test]
    fn test_subscribe_failure() {
        with_addr(&mut |addr| {

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
                StreamAction::Read(TcpMessage::Subscribe(true, 0, None, None)),
                StreamAction::Write(TcpMessage::Error(DatabaseError::SubscriptionError))
            ]);

            let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
            assert_eq!(client.subscribe(Query::live()).err(), Some(DatabaseError::SubscriptionError));
        });
    }

    #[test]
    fn test_unsubscribe() {
        with_addr(&mut |addr| {

            let event = Event::new("data", vec!["tag1", "tag2"]).with_timestamp(1234567890);

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
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

            client.unsubscribe().is_ok();

            assert_eq!(event_stream.next(), None);
        });
    }
}
