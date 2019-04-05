pub use exar::*;
pub use exar_net::*;
pub use exar_testkit::*;
pub use super::*;

use std::net::{TcpListener, ToSocketAddrs};
use std::thread;
use std::time::Duration;

pub enum StreamAction {
    Read(TcpMessage),
    Write(TcpMessage)
}

pub fn stub_server<A: Send + ToSocketAddrs + 'static>(addr: A, actions: Vec<StreamAction>) {
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