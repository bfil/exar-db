use super::*;

use exar::*;
use exar_net::*;

use std::io::ErrorKind;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub struct Handler {
    config: ServerConfig,
    stream: TcpMessageStream<TcpStream>,
    state: State
}

impl Handler {
    pub fn new(stream: TcpStream, db: Arc<Mutex<Database>>, config: ServerConfig) -> Result<Handler, DatabaseError> {
        TcpMessageStream::new(stream).and_then(|stream| {
            Ok(Handler {
                config: config,
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
        self.config.username.is_some() && self.config.password.is_some()
    }

    fn verify_authentication(&self, username: Option<String>, password: Option<String>) -> bool {
        if self.needs_authentication() {
            if username.is_some() && password.is_some() {
                self.config.username == username && self.config.password == password
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
                for event in event_stream {
                    let send_result = self.stream.send_message(TcpMessage::Event(event));
                    if send_result.is_err() { return send_result }
                }
                self.stream.send_message(TcpMessage::EndOfEventStream)
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
