use super::*;

use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Connection {
    collection: Arc<Mutex<Collection>>
}

impl Connection {
    pub fn new(collection: Arc<Mutex<Collection>>) -> Connection {
        Connection {
            collection: collection
        }
    }

    pub fn publish(&self, event: Event) -> Result<usize, DatabaseError> {
        self.collection.lock().unwrap().publish(event)
    }

    pub fn subscribe(&self, query: Query) -> Result<EventStream, DatabaseError> {
        self.collection.lock().unwrap().subscribe(query)
    }

    pub fn close(self) {
        drop(self)
    }
}
