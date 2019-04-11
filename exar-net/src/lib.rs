//! # Exar DB's TCP protocol
//! This module defines the TCP protocol used by Exar DB.
//!
//! ## Protocol messages
//! The protocol is text-based and uses line-separated messages,
//! each message consists of tab-separated values.
//!
//! ### Authenticate
//! Message used to authenticate to Exar DB.
//!
//! ```text
//! Authenticate    username    password
//! ```
//!
//! - The 1st field is the string `Authenticate`.
//! - The 2nd field is the authentication username.
//! - The 3rd field is the authentication password.
//!
//! ### Authenticated
//! Message used to acknowledge a successful authentication.
//!
//! *If the Exar DB server requires authentication,
//! this must be the first interaction in order to be allowed any other operation*.
//!
//! ```text
//! Authenticated
//! ```
//!
//! - A single field containing the string `Authenticated`.
//!
//! ### Select
//! Message used to select an Exar DB collection.
//!
//! ```text
//! Select    collection
//! ```
//!
//! - The 1st field is the string `Select`.
//! - The 2nd field is the collection name.
//!
//! ### Selected
//! Message used to acknowledge a successful collection selection.
//!
//! ```text
//! Selected
//! ```
//!
//! - A single field containing the string `Selected`.
//!
//! ### Publish
//! Message used to publish an event into a collection.
//!
//! *It can only be used after a collection has been selected*.
//!
//! ```text
//! Publish    tag1 tag2    timestamp    event_data
//! ```
//!
//! - The 1st field is the string `Publish`.
//! - The 2nd field is a space-separated list of tags, the event must contain at least one tag.
//! - The 3rd field is the event timestamp (in ms), if set to 0 the timestamp will be set by the event logger.
//! - The 4th field is the event data/payload, it can contain tabs (`\t`) but new-lines (`\n`) must be escaped.
//!
//! ### Published
//! Message used to acknowledge a successfully published event.
//!
//! ```text
//! Published    event_id
//! ```
//!
//! - The 1st field is the string `Published`.
//! - The 2nd field is the `id` (or sequence number) of the event that has been published.
//!
//! ### Subscribe
//! Message used to subscribe to an event stream.
//!
//! *It can only be used after a collection has been selected*.
//!
//! ```text
//! Subscribe    live    offset    limit    [tag1]
//! ```
//!
//! - The 1st field is the string `Subscribe`.
//! - The 2nd field is a boolean specifying whether to keep the subscription listening to real-time events.
//! - The 3rd field is the query offset.
//! - The 4th field is the maximum number of events to consume, if set to 0 a limit is not set.
//! - The 5th field is the tag the events must contain (optional).
//!
//! ### Subscribed
//! Message used to acknowledge a successful subscription.
//!
//! ```text
//! Subscribed
//! ```
//!
//! - A single field containing the string `Subscribed`.
//!
//! ### Unsubscribe
//! Message used to unsubscribe from an event stream.
//!
//! ```text
//! Unsubscribe
//! ```
//!
//! - A single field containing the string `Unsubscribe`.
//!
//! ### Event
//! Message containing an event.
//!
//! *It is received after a successful subscription*.
//!
//! ```text
//! Event    event_id    tag1 tag2    timestamp    event_data
//! ```
//!
//! - The 1st field is the string `Event`.
//! - The 2nd field is the `id` (or sequence number) of the event.
//! - The 3rd field is a space-separated list of event tags.
//! - The 4th field is the event timestamp (in ms).
//! - The 5th field is the event data/payload.
//!
//! ### EndOfEventStream
//! Message signaling the end of an event stream.
//!
//! *It is received after a `Subscribed` or a list of `Event`s*.
//!
//! ```text
//! EndOfEventStream
//! ```
//!
//! - A single field containing the string `EndOfEventStream`.
//!
//! ### Drop
//! Messaged used to drop an Exar DB collection.
//!
//! ```text
//! Drop    collection
//! ```
//!
//! - The 1st field is the string `Drop`.
//! - The 2nd field is the collection name.
//!
//! ### Dropped
//! Message used to acknowledge a successful collection drop.
//!
//! ```text
//! Dropped
//! ```
//!
//! - A single field containing the string `Dropped`.
//!
//! ### Error
//! Message containing an error.
//!
//! *It can be received after a `Connect`, `Publish`, `Subscribe`, or during an event stream*.
//!
//! ```text
//! Error    type    [subtype]    description
//! ```
//!
//! - The 1st field is the string `Error`.
//! - The 2nd field is the type of the error, possible values are:
//!   `AuthenticationError`, `ConnectionError`, `EventStreamError`, `IoError`, `ParseError`,
//!   `SubscriptionError`, `ValidationError`, `InternalError`.
//! - The 3rd field is the sub-type of the error (optional), possible values are:
//!   `Empty` or `Closed` if the error type is `EventStreamError`,
//!   `ParseError` or `MissingField` if the error type is `ParseError`,
//!   or a stringified value of `std::io::ErrorKind` if the error type is `IoError`.
//! - The 4th field is the error message/description.
//!

extern crate exar;

#[cfg(test)] extern crate exar_testkit;

mod message;
mod stream;

#[cfg(test)] mod testkit;

pub use self::message::*;
pub use self::stream::*;
