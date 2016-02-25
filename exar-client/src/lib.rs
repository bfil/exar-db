extern crate exar;
extern crate exar_net;

use exar::*;
use exar_net::*;

use std::io::ErrorKind;
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::thread;

pub struct Client {
    stream: Stream
}

impl Client {
    pub fn connect(address: &str, collection_name: &str, username: Option<&str>, password: Option<&str>) -> Result<Client, DatabaseError> {
        match TcpStream::connect(address) {
            Ok(stream) => {
                let mut stream = try!(Stream::new(stream));
                let username = username.map(|u| u.to_owned());
                let password = password.map(|p| p.to_owned());
                let connection_message = TcpMessage::Connect(collection_name.to_owned(), username, password);
                try!(stream.send_message(connection_message));
                match stream.receive_message() {
                    Ok(TcpMessage::Connected) => Ok(Client { stream: stream }),
                    Ok(TcpMessage::Error(error)) => Err(error),
                    Ok(_) => Err(DatabaseError::ConnectionError),
                    Err(err) => Err(err)
                }
            },
            Err(err) => Err(DatabaseError::io_error(err))
        }
    }

    pub fn publish(&mut self, event: Event) -> Result<usize, DatabaseError> {
        try!(self.stream.send_message(TcpMessage::Publish(event)));
        match self.stream.receive_message() {
            Ok(TcpMessage::Published(event_id)) => Ok(event_id),
            Ok(TcpMessage::Error(error)) => Err(error),
            Ok(_) => Err(DatabaseError::IoError(ErrorKind::InvalidData, "unexpected TCP message".to_owned())),
            Err(err) => Err(err)
        }
    }

    pub fn subscribe(&mut self, query: Query) -> Result<EventStream, DatabaseError> {
        let subscribe_message = TcpMessage::Subscribe(query.live, query.offset, query.limit, query.tag);
        self.stream.send_message(subscribe_message).and_then(|_| {
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
        })
    }

    pub fn close(self) {
        drop(self)
    }
}
