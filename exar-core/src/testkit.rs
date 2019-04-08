pub use super::*;

pub use exar_testkit::*;

use std::collections::BTreeMap;
use std::sync::mpsc::Receiver;

pub fn temp_data_config(index_granularity: u64) -> DataConfig {
    DataConfig { path: temp_dir(), index_granularity }
}

pub fn temp_collection_config() -> CollectionConfig {
    CollectionConfig {
        data: temp_data_config(DEFAULT_INDEX_GRANULARITY),
        scanner: ScannerConfig::default(),
        publisher: PublisherConfig::default()
    }
}

pub fn temp_database_config() -> DatabaseConfig {
    DatabaseConfig {
        data: temp_data_config(DEFAULT_INDEX_GRANULARITY),
        scanner: ScannerConfig::default(),
        publisher: PublisherConfig::default(),
        collections: BTreeMap::new()
    }
}

pub fn temp_log(index_granularity: u64) -> Log {
    Log::new(&random_collection_name(), &temp_data_config(index_granularity)).expect("Unable to create log")
}

pub fn temp_collection() -> Collection {
    Collection::new(&random_collection_name(), &temp_collection_config()).expect("Unable to create collection")
}

pub fn temp_database() -> Database {
    Database::new(temp_database_config())
}

pub fn assert_event_received(receiver: &Receiver<EventStreamMessage>, event_id: u64) {
    match receiver.recv().expect("Unable to receive event") {
        EventStreamMessage::Event(event) => assert_eq!(event.id, event_id),
        EventStreamMessage::End          => panic!("Unexpected end of event stream")
    };
}

pub fn assert_end_of_event_stream_received(receiver: &Receiver<EventStreamMessage>) {
    match receiver.recv().expect("Unable to receive event") {
        EventStreamMessage::Event(event) => panic!("Unexpected event: {}", event),
        EventStreamMessage::End          => ()
    };
}