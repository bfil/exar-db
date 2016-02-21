use super::*;

use std::sync::mpsc::Sender;

pub struct Subscription {
    active: bool,
    pub send: Sender<Event>,
    pub query: Query
}

impl Subscription {
    pub fn new(send: Sender<Event>, query: Query) -> Subscription {
        Subscription {
            active: true,
            send: send,
            query: query
        }
    }

    pub fn emit(&mut self, event: Event) -> Result<(), DatabaseError> {
        let event_id = event.id;
        match self.send.send(event) {
            Ok(_) => Ok(self.query.update(event_id)),
            Err(_) => {
                self.active = false;
                Err(DatabaseError::EventStreamClosed)
            }
        }
    }

    pub fn is_active(&self) -> bool {
        self.active
    }
}
