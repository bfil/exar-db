use super::*;

use std::collections::BTreeMap;

/// Exar DB's configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
/// use std::collections::BTreeMap;
///
/// let config = DatabaseConfig {
///     logs_path: "/path/to/logs".to_owned(),
///     index_granularity: 100000,
///     routing_strategy: RoutingStrategy::default(),
///     scanners: ScannersConfig {
///         nr_of_scanners: 2
///     },
///     collections: BTreeMap::new()
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig  {
    /// Path to the logs directory.
    pub logs_path: String,
    /// Granularity of the log lines index (used by `IndexedLineReader`).
    pub index_granularity: u64,
    /// Subscriptions' routing strategy.
    pub routing_strategy: RoutingStrategy,
    /// Log scanners' configuration.
    pub scanners: ScannersConfig,
    /// Holds collection-specific configuration overrides.
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
    /// Returns the configuration for a given collection
    /// by applying overrides to the base DatabaseConfig.
    pub fn collection_config(&self, collection_name: &str) -> CollectionConfig {
        match self.collections.get(collection_name) {
            Some(collection_config) => {
                let config = collection_config.clone();
                CollectionConfig {
                    logs_path: config.logs_path.unwrap_or_else(|| self.logs_path.clone()),
                    index_granularity: config.index_granularity.unwrap_or_else(|| self.index_granularity),
                    scanners: match config.scanners {
                        Some(scanners_config) => ScannersConfig {
                            nr_of_scanners: scanners_config.nr_of_scanners.unwrap_or(self.scanners.nr_of_scanners)
                        },
                        None => ScannersConfig {
                            nr_of_scanners: self.scanners.nr_of_scanners
                        }
                    },
                    routing_strategy: config.routing_strategy.unwrap_or_else(|| self.routing_strategy.clone())
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
}

/// Exar DB's scanners configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = ScannersConfig {
///     nr_of_scanners: 2
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannersConfig {
    /// Number of scanners for each log file (spawns 2 threads for each scanner).
    pub nr_of_scanners: u8
}

impl Default for ScannersConfig {
    fn default() -> ScannersConfig {
        ScannersConfig {
            nr_of_scanners: 2
        }
    }
}

/// Exar DB's partial scanners configuration.
/// Holds overrides for the main database configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = PartialScannersConfig {
///     nr_of_scanners: Some(2)
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialScannersConfig {
    /// Number of scanners for each log file (spawns 2 threads for each scanner).
    pub nr_of_scanners: Option<u8>
}

/// Exar DB's collection configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = CollectionConfig {
///     logs_path: "/path/to/logs".to_owned(),
///     index_granularity: 100000,
///     routing_strategy: RoutingStrategy::default(),
///     scanners: ScannersConfig {
///         nr_of_scanners: 2
///     }
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CollectionConfig {
    /// Path to the logs directory.
    pub logs_path: String,
    /// Granularity of the log lines index (used by `IndexedLineReader`).
    pub index_granularity: u64,
    /// Subscriptions' routing strategy.
    pub routing_strategy: RoutingStrategy,
    /// Log scanners' configuration.
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

/// Exar DB's partial collection configuration.
/// Holds overrides for the main database configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = PartialCollectionConfig {
///     logs_path: Some("/path/to/logs".to_owned()),
///     index_granularity: Some(100000),
///     routing_strategy: Some(RoutingStrategy::default()),
///     scanners: Some(PartialScannersConfig {
///         nr_of_scanners: Some(2)
///     })
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialCollectionConfig {
    /// Path to the logs directory.
    pub logs_path: Option<String>,
    /// Granularity of the log lines index (used by `IndexedLineReader`).
    pub index_granularity: Option<u64>,
    /// Subscriptions' routing strategy.
    pub routing_strategy: Option<RoutingStrategy>,
    /// Log scanners' configuration.
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
                nr_of_scanners: Some(3)
            }),
            routing_strategy: Some(RoutingStrategy::Random)
        });

        let collection_config = db_config.collection_config("test");

        assert_eq!(collection_config.logs_path, "test".to_owned());
        assert_eq!(collection_config.index_granularity, 1000);
        assert_eq!(collection_config.scanners, ScannersConfig {
            nr_of_scanners: 3
        });
        assert_eq!(collection_config.routing_strategy, RoutingStrategy::Random);
    }
}
