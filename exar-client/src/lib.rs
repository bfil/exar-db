#![feature(const_fn)]

extern crate exar;
extern crate exar_net;

use exar::*;
use exar_net::*;

use std::io::ErrorKind;
use std::net::{ToSocketAddrs, TcpStream};
use std::sync::mpsc::channel;
use std::thread;

pub struct Client {
    stream: TcpMessageStream<TcpStream>
}

impl Client {
    pub fn connect<A: ToSocketAddrs>(address: A, collection_name: &str,
        username: Option<&str>, password: Option<&str>) -> Result<Client, DatabaseError> {
        match TcpStream::connect(address) {
            Ok(stream) => {
                let mut stream = try!(TcpMessageStream::new(stream));
                let username = username.map(|u| u.to_owned());
                let password = password.map(|p| p.to_owned());
                let connection_message = TcpMessage::Connect(collection_name.to_owned(), username, password);
                try!(stream.send_message(connection_message));
                match stream.recv_message() {
                    Ok(TcpMessage::Connected) => Ok(Client { stream: stream }),
                    Ok(TcpMessage::Error(error)) => Err(error),
                    Ok(_) => Err(DatabaseError::ConnectionError),
                    Err(err) => Err(err)
                }
            },
            Err(err) => Err(DatabaseError::new_io_error(err))
        }
    }

    pub fn publish(&mut self, event: Event) -> Result<usize, DatabaseError> {
        try!(self.stream.send_message(TcpMessage::Publish(event)));
        match self.stream.recv_message() {
            Ok(TcpMessage::Published(event_id)) => Ok(event_id),
            Ok(TcpMessage::Error(error)) => Err(error),
            Ok(_) => Err(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned())),
            Err(err) => Err(err)
        }
    }

    pub fn subscribe(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        let subscribe_message = TcpMessage::Subscribe(query.live, query.offset, query.limit, query.tag);
        self.stream.send_message(subscribe_message).and_then(|_| {
            self.stream.recv_message().and_then(|message| {
                match message {
                    TcpMessage::Subscribed => {
                        let (send, recv) = channel();
                        self.stream.try_clone().and_then(|cloned_stream| {
                            thread::spawn(move || {
                                for message in cloned_stream.messages() {
                                    match message {
                                        Ok(TcpMessage::Event(event)) => match send.send(event) {
                                            Ok(_) => continue,
                                            Err(err) => println!("Unable to send event to the event stream: {}", err)
                                        },
                                        Ok(TcpMessage::EndOfEventStream) => (),
                                        Ok(TcpMessage::Error(error)) => println!("Received error from TCP stream: {}", error),
                                        Ok(message) => println!("Unexpected TCP message: {}", message),
                                        Err(err) => println!("Unable to read TCP message from stream: {}", err)
                                    };
                                    break
                                }
                            });
                            Ok(EventStream::new(recv))
                        })
                    },
                    TcpMessage::Error(err) => Err(err),
                    _ => Err(DatabaseError::SubscriptionError)
                }
            })
        })
    }

    pub fn close(self) {
        drop(self)
    }
}

#[cfg(test)]
mod tests {
    use exar::*;
    use exar_net::*;
    use super::*;

    use std::env;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, ToSocketAddrs};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

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
                            StreamAction::Read(message) => assert_eq!(stream.recv_message(), Ok(message)),
                            StreamAction::Write(message) => assert!(stream.send_message(message).is_ok())
                        }
                    }
                },
                Err(err) => println!("Error: {}", err)
            }
        });
        thread::sleep(Duration::from_millis(100));
    }

    #[test]
    fn test_connect() {
        each_ip(&mut |addr| {

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Connected),
            ]);

            assert!(Client::connect(addr, "collection", None, None).is_ok());
        });
    }

    #[test]
    fn test_connect_with_authentication() {
        each_ip(&mut |addr| {

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
        each_ip(&mut |addr| {

            stub_server(addr.clone(), vec![
                StreamAction::Read(TcpMessage::Connect("collection".to_owned(), None, None)),
                StreamAction::Write(TcpMessage::Error(DatabaseError::ConnectionError))
            ]);

            assert_eq!(Client::connect(addr, "collection", None, None).err(), Some(DatabaseError::ConnectionError));
        });
    }

    #[test]
    fn test_publish() {
        each_ip(&mut |addr| {

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
        each_ip(&mut |addr| {

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
        each_ip(&mut |addr| {

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

            let mut client = Client::connect(addr, "collection", None, None).expect("Unable to connect");
            let mut event_stream = client.subscribe(Query::live()).expect("Unable to subscribe");
            assert_eq!(event_stream.next(), Some(event.clone().with_id(1)));
            assert_eq!(event_stream.next(), Some(event.clone().with_id(2)));
            assert_eq!(event_stream.next(), None);
        });
    }

    #[test]
    fn test_subscribe_failure() {
        each_ip(&mut |addr| {

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
}
