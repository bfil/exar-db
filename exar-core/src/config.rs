use super::*;

use std::collections::BTreeMap;

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseConfig  {
    pub logs_path: String,
    pub index_granularity: u64,
    pub routing_strategy: RoutingStrategy,
    pub scanners: u8,
    pub scanners_sleep_ms: u8,
    pub collections: BTreeMap<String, PartialCollectionConfig>
}

impl Default for DatabaseConfig {
    fn default() -> DatabaseConfig {
        DatabaseConfig {
            logs_path: "".to_owned(),
            index_granularity: 100000,
            scanners: 2,
            routing_strategy: RoutingStrategy::default(),
            scanners_sleep_ms: 10,
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
                    index_granularity: config.index_granularity.unwrap_or(self.index_granularity.clone()),
                    scanners: config.scanners.unwrap_or(self.scanners),
                    scanners_sleep_ms: config.scanners_sleep_ms.unwrap_or(self.scanners_sleep_ms),
                    routing_strategy: config.routing_strategy.unwrap_or(self.routing_strategy.clone())
                }
            },
            None => CollectionConfig {
                logs_path: self.logs_path.clone(),
                index_granularity: self.index_granularity,
                scanners: self.scanners,
                scanners_sleep_ms: self.scanners_sleep_ms,
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
    pub index_granularity: u64,
    pub routing_strategy: RoutingStrategy,
    pub scanners: u8,
    pub scanners_sleep_ms: u8
}

impl Default for CollectionConfig {
    fn default() -> CollectionConfig {
        let db_defaults = DatabaseConfig::default();
        CollectionConfig {
            logs_path: db_defaults.logs_path,
            index_granularity: db_defaults.index_granularity,
            scanners: db_defaults.scanners,
            scanners_sleep_ms: db_defaults.scanners_sleep_ms,
            routing_strategy: db_defaults.routing_strategy
        }
    }
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialCollectionConfig {
    pub logs_path: Option<String>,
    pub index_granularity: Option<u64>,
    pub routing_strategy: Option<RoutingStrategy>,
    pub scanners: Option<u8>,
    pub scanners_sleep_ms: Option<u8>
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_get_collection_config() {
        let mut db_config = DatabaseConfig::default();

        let collection_config = db_config.get_collection_config("test");

        assert_eq!(collection_config.logs_path, db_config.logs_path);
        assert_eq!(collection_config.index_granularity, db_config.index_granularity);
        assert_eq!(collection_config.scanners, db_config.scanners);
        assert_eq!(collection_config.routing_strategy, db_config.routing_strategy);

        db_config.collections.insert("test".to_owned(), PartialCollectionConfig {
            logs_path: Some("test".to_owned()),
            index_granularity: Some(1000),
            scanners: Some(10),
            scanners_sleep_ms: Some(5),
            routing_strategy: Some(RoutingStrategy::Random)
        });

        let collection_config = db_config.get_collection_config("test");

        assert_eq!(collection_config.logs_path, "test".to_owned());
        assert_eq!(collection_config.index_granularity, 1000);
        assert_eq!(collection_config.scanners, 10);
        assert_eq!(collection_config.scanners_sleep_ms, 5);
        assert_eq!(collection_config.routing_strategy, RoutingStrategy::Random);
    }
}
