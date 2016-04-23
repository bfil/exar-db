use super::*;

use std::collections::BTreeMap;
use std::time::Duration;

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseConfig  {
    pub logs_path: String,
    pub index_granularity: u64,
    pub routing_strategy: RoutingStrategy,
    pub scanners: ScannersConfig,
    pub collections: BTreeMap<String, PartialCollectionConfig>
}

impl Default for DatabaseConfig {
    fn default() -> DatabaseConfig {
        DatabaseConfig {
            logs_path: "".to_owned(),
            index_granularity: 100000,
            routing_strategy: RoutingStrategy::default(),
            scanners: ScannersConfig::default(),
            collections: BTreeMap::new()
        }
    }
}

impl DatabaseConfig {
    pub fn collection_config(&self, collection_name: &str) -> CollectionConfig {
        match self.collections.get(collection_name) {
            Some(collection_config) => {
                let config = collection_config.clone();
                CollectionConfig {
                    logs_path: config.logs_path.unwrap_or(self.logs_path.clone()),
                    index_granularity: config.index_granularity.unwrap_or(self.index_granularity.clone()),
                    scanners: match config.scanners {
                        Some(scanners_config) => ScannersConfig {
                            nr_of_scanners: scanners_config.nr_of_scanners.unwrap_or(self.scanners.nr_of_scanners),
                            sleep_time_in_ms: scanners_config.sleep_time_in_ms.unwrap_or(self.scanners.sleep_time_in_ms)
                        },
                        None => ScannersConfig {
                            nr_of_scanners: self.scanners.nr_of_scanners,
                            sleep_time_in_ms: self.scanners.sleep_time_in_ms
                        }
                    },
                    routing_strategy: config.routing_strategy.unwrap_or(self.routing_strategy.clone())
                }
            },
            None => CollectionConfig {
                logs_path: self.logs_path.clone(),
                index_granularity: self.index_granularity,
                scanners: self.scanners.clone(),
                routing_strategy: self.routing_strategy.clone()
            }
        }
    }
    pub fn scanners_sleep_duration(&self) -> Duration {
        Duration::from_millis(self.scanners.sleep_time_in_ms)
    }
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannersConfig {
    pub nr_of_scanners: u8,
    pub sleep_time_in_ms: u64
}

impl Default for ScannersConfig {
    fn default() -> ScannersConfig {
        ScannersConfig {
            nr_of_scanners: 2,
            sleep_time_in_ms: 10
        }
    }
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialScannersConfig {
    pub nr_of_scanners: Option<u8>,
    pub sleep_time_in_ms: Option<u64>
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionConfig {
    pub logs_path: String,
    pub index_granularity: u64,
    pub routing_strategy: RoutingStrategy,
    pub scanners: ScannersConfig
}

impl Default for CollectionConfig {
    fn default() -> CollectionConfig {
        let db_defaults = DatabaseConfig::default();
        CollectionConfig {
            logs_path: db_defaults.logs_path,
            index_granularity: db_defaults.index_granularity,
            scanners: db_defaults.scanners,
            routing_strategy: db_defaults.routing_strategy
        }
    }
}

impl CollectionConfig {
    pub fn scanners_sleep_duration(&self) -> Duration {
        Duration::from_millis(self.scanners.sleep_time_in_ms)
    }
}

#[cfg_attr(feature = "rustc-serialization", derive(RustcEncodable, RustcDecodable))]
#[cfg_attr(feature = "serde-serialization", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialCollectionConfig {
    pub logs_path: Option<String>,
    pub index_granularity: Option<u64>,
    pub routing_strategy: Option<RoutingStrategy>,
    pub scanners: Option<PartialScannersConfig>
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_collection_config() {
        let mut db_config = DatabaseConfig::default();

        let collection_config = db_config.collection_config("test");

        assert_eq!(collection_config.logs_path, db_config.logs_path);
        assert_eq!(collection_config.index_granularity, db_config.index_granularity);
        assert_eq!(collection_config.scanners, db_config.scanners);
        assert_eq!(collection_config.routing_strategy, db_config.routing_strategy);

        db_config.collections.insert("test".to_owned(), PartialCollectionConfig {
            logs_path: Some("test".to_owned()),
            index_granularity: Some(1000),
            scanners: Some(PartialScannersConfig {
                nr_of_scanners: Some(3),
                sleep_time_in_ms: Some(5)
            }),
            routing_strategy: Some(RoutingStrategy::Random)
        });

        let collection_config = db_config.collection_config("test");

        assert_eq!(collection_config.logs_path, "test".to_owned());
        assert_eq!(collection_config.index_granularity, 1000);
        assert_eq!(collection_config.scanners, ScannersConfig {
            nr_of_scanners: 3,
            sleep_time_in_ms: 5
        });
        assert_eq!(collection_config.routing_strategy, RoutingStrategy::Random);
    }
}
