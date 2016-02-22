use super::*;

use std::collections::BTreeMap;

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseConfig  {
    pub logs_path: String,
    pub num_scanners: u8,
    pub routing_strategy: RoutingStrategy,
    pub collections: BTreeMap<String, PartialCollectionConfig>
}

impl Default for DatabaseConfig {
    fn default() -> DatabaseConfig {
        DatabaseConfig {
            logs_path: "".to_owned(),
            num_scanners: 2,
            routing_strategy: RoutingStrategy::default(),
            collections: BTreeMap::new()
        }
    }
}

impl DatabaseConfig {
    pub fn get_collection_config(&self, collection_name: &str) -> CollectionConfig {
        match self.collections.get(collection_name) {
            Some(collection_config) => {
                let coll = collection_config.clone();
                CollectionConfig {
                    logs_path: coll.logs_path.unwrap_or(self.logs_path.clone()),
                    num_scanners: coll.num_scanners.unwrap_or(self.num_scanners),
                    routing_strategy: coll.routing_strategy.unwrap_or(self.routing_strategy.clone())
                }
            },
            None => CollectionConfig {
                logs_path: self.logs_path.clone(),
                num_scanners: self.num_scanners,
                routing_strategy: self.routing_strategy.clone()
            }
        }
    }
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionConfig {
    pub logs_path: String,
    pub num_scanners: u8,
    pub routing_strategy: RoutingStrategy
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialCollectionConfig {
    pub logs_path: Option<String>,
    pub num_scanners: Option<u8>,
    pub routing_strategy: Option<RoutingStrategy>
}
