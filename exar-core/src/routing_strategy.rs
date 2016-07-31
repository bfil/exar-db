#[cfg(feature = "rustc-serialization")] use rustc_serialize::{Encoder, Encodable, Decoder, Decodable};
#[cfg(feature = "serde-serialization")] use serde::{Serialize, Serializer, Deserialize, Deserializer};
#[cfg(feature = "serde-serialization")] use serde::de::{Error, Visitor};

/// A list specifying categories of routing strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// The next element is picked at random.
    Random,
    /// The next element is picked using the round-robin algorithm.
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
            RoutingStrategy::Random => serializer.serialize_str("Random"),
            RoutingStrategy::RoundRobin(_) => serializer.serialize_str("RoundRobin")
        }
    }
}

#[cfg(feature = "serde-serialization")]
impl Deserialize for RoutingStrategy {
    fn deserialize<D: Deserializer>(deserializer: &mut D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(RoutingStrategyVisitor)
    }
}

#[cfg(feature = "serde-serialization")]
struct RoutingStrategyVisitor;

#[cfg(feature = "serde-serialization")]
impl Visitor for RoutingStrategyVisitor {
    type Value = RoutingStrategy;
    fn visit_str<E: Error>(&mut self, s: &str) -> Result<RoutingStrategy, E> {
        match s {
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

#[cfg(test)]
mod tests {
    use super::super::*;

    #[cfg(feature = "rustc-serialization")]
    use rustc_serialize::json;

    #[cfg(feature = "serde-serialization")]
    extern crate serde_json;

    #[test]
    fn test_default() {
        assert_eq!(RoutingStrategy::default(), RoutingStrategy::RoundRobin(0));
    }

    #[test]
    #[cfg(feature = "rustc-serialization")]
    fn test_rustc_serialization() {
        let routing_strategy = RoutingStrategy::Random;
        assert_eq!(json::encode(&routing_strategy).unwrap(), "\"Random\"");
        assert_eq!(json::decode::<RoutingStrategy>("\"Random\"").unwrap(), routing_strategy);

        let routing_strategy = RoutingStrategy::RoundRobin(0);
        assert_eq!(json::encode(&routing_strategy).unwrap(), "\"RoundRobin\"");
        assert_eq!(json::decode::<RoutingStrategy>("\"RoundRobin\"").unwrap(), routing_strategy);
    }

    #[test]
    #[cfg(feature = "serde-serialization")]
    fn test_serde_serialization() {
        let routing_strategy = RoutingStrategy::Random;
        assert_eq!(serde_json::to_string(&routing_strategy).unwrap(), "\"Random\"");
        assert_eq!(serde_json::from_str::<RoutingStrategy>("\"Random\"").unwrap(), routing_strategy);

        let routing_strategy = RoutingStrategy::RoundRobin(0);
        assert_eq!(serde_json::to_string(&routing_strategy).unwrap(), "\"RoundRobin\"");
        assert_eq!(serde_json::from_str::<RoutingStrategy>("\"RoundRobin\"").unwrap(), routing_strategy);
    }
}
