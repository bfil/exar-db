use super::*;

use std::collections::BTreeMap;

#[cfg_attr(feature = "rustc-serialize", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct DatabaseConfig  {
    pub logs_path: String,
    pub num_scanners: u8,
    pub routing_strategy: RoutingStrategy,
    pub collections: BTreeMap<String, CollectionConfig>
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
            Some(collection_config) => collection_config.clone(),
            None => CollectionConfig {
                logs_path: self.logs_path.clone(),
                num_scanners: self.num_scanners,
                routing_strategy: self.routing_strategy.clone()
            }
        }
    }
}

#[cfg_attr(feature = "rustc-serialize", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct CollectionConfig {
    pub logs_path: String,
    pub num_scanners: u8,
    pub routing_strategy: RoutingStrategy
}
