#[cfg(feature = "rustc-serialization")] use rustc_serialize::{Encoder, Encodable, Decoder, Decodable};
#[cfg(feature = "serde-serialization")] use serde::{Serialize, Serializer, Deserialize, Deserializer};
#[cfg(feature = "serde-serialization")] use serde::de::{Error, Visitor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutingStrategy {
    Random,
    RoundRobin(usize)
}

#[cfg(feature = "rustc-serialization")]
impl Encodable for RoutingStrategy {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        match *self {
            RoutingStrategy::Random => s.emit_str("Random"),
            RoutingStrategy::RoundRobin(_) => s.emit_str("RoundRobin")
        }
    }

}

#[cfg(feature = "rustc-serialization")]
impl Decodable for RoutingStrategy {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        d.read_str().map(|s| {
            match s.as_ref() {
                "Random" => RoutingStrategy::Random,
                "RoundRobin" => RoutingStrategy::RoundRobin(0),
                _ => RoutingStrategy::default()
            }
        })
    }
}

#[cfg(feature = "serde-serialization")]
impl Serialize for RoutingStrategy {
    fn serialize<S: Serializer>(&self, serializer: &mut S) -> Result<(), S::Error> {
        match *self {
            RoutingStrategy::Random => serializer.visit_str("Random"),
            RoutingStrategy::RoundRobin(_) => serializer.visit_str("RoundRobin")
        }
    }
}

#[cfg(feature = "serde-serialization")]
impl Deserialize for RoutingStrategy {
    fn deserialize<D: Deserializer>(deserializer: &mut D) -> Result<Self, D::Error> {
        deserializer.visit_string(RoutingStrategyVisitor)
    }
}

#[cfg(feature = "serde-serialization")]
struct RoutingStrategyVisitor;

#[cfg(feature = "serde-serialization")]
impl Visitor for RoutingStrategyVisitor {
    type Value = RoutingStrategy;
    fn visit_string<E: Error>(&mut self, s: String) -> Result<RoutingStrategy, E> {
        match s.as_ref() {
            "Random" => Ok(RoutingStrategy::Random),
            "RoundRobin" => Ok(RoutingStrategy::RoundRobin(0)),
            _ => Ok(RoutingStrategy::default())
        }
    }
}

impl Default for RoutingStrategy {
    fn default() -> Self {
        RoutingStrategy::RoundRobin(0)
    }
}
