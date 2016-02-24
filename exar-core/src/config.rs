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
                let config = collection_config.clone();
                CollectionConfig {
                    logs_path: config.logs_path.unwrap_or(self.logs_path.clone()),
                    num_scanners: config.num_scanners.unwrap_or(self.num_scanners),
                    routing_strategy: config.routing_strategy.unwrap_or(self.routing_strategy.clone())
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

impl Default for CollectionConfig {
    fn default() -> CollectionConfig {
        CollectionConfig {
            logs_path: "".to_owned(),
            num_scanners: 2,
            routing_strategy: RoutingStrategy::default()
        }
    }
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialCollectionConfig {
    pub logs_path: Option<String>,
    pub num_scanners: Option<u8>,
    pub routing_strategy: Option<RoutingStrategy>
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_get_collection_config() {
        let mut db_config = DatabaseConfig::default();

        let collection_config = db_config.get_collection_config("test");

        assert_eq!(collection_config.logs_path, db_config.logs_path);
        assert_eq!(collection_config.num_scanners, db_config.num_scanners);
        assert_eq!(collection_config.routing_strategy, db_config.routing_strategy);

        db_config.collections.insert("test".to_owned(), PartialCollectionConfig {
            logs_path: Some("test".to_owned()),
            num_scanners: Some(10),
            routing_strategy: Some(RoutingStrategy::Random)
        });

        let collection_config = db_config.get_collection_config("test");

        assert_eq!(collection_config.logs_path, "test".to_owned());
        assert_eq!(collection_config.num_scanners, 10);
        assert_eq!(collection_config.routing_strategy, RoutingStrategy::Random);
    }
}
