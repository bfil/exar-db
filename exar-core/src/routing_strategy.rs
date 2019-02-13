use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{Error, Visitor};
use std::fmt;

/// A list specifying categories of routing strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// The next element is picked at random.
    Random,
    /// The next element is picked using the round-robin algorithm.
    RoundRobin(usize)
}

impl Serialize for RoutingStrategy {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match *self {
            RoutingStrategy::Random        => serializer.serialize_str("Random"),
            RoutingStrategy::RoundRobin(_) => serializer.serialize_str("RoundRobin")
        }
    }
}

impl<'de> Deserialize<'de> for RoutingStrategy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(RoutingStrategyVisitor)
    }
}

struct RoutingStrategyVisitor;

impl<'de> Visitor<'de> for RoutingStrategyVisitor {
    type Value = RoutingStrategy;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Random or RoundRobin")
    }
    fn visit_str<E: Error>(self, s: &str) -> Result<RoutingStrategy, E> {
        match s {
            "Random"     => Ok(RoutingStrategy::Random),
            "RoundRobin" => Ok(RoutingStrategy::RoundRobin(0)),
            _            => Ok(RoutingStrategy::default())
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

    extern crate serde_json;

    #[test]
    fn test_default() {
        assert_eq!(RoutingStrategy::default(), RoutingStrategy::RoundRobin(0));
    }

    #[test]
    fn test_serde_serialization() {
        let routing_strategy = RoutingStrategy::Random;
        assert_eq!(serde_json::to_string(&routing_strategy).unwrap(), "\"Random\"");
        assert_eq!(serde_json::from_str::<RoutingStrategy>("\"Random\"").unwrap(), routing_strategy);

        let routing_strategy = RoutingStrategy::RoundRobin(0);
        assert_eq!(serde_json::to_string(&routing_strategy).unwrap(), "\"RoundRobin\"");
        assert_eq!(serde_json::from_str::<RoutingStrategy>("\"RoundRobin\"").unwrap(), routing_strategy);
    }
}
