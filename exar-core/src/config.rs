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
///     scanner: ScannerConfig {
///         routing_strategy: RoutingStrategy::default(),
///         threads: 2
///     },
///     publisher: PublisherConfig {
///         buffer_size: 100
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
    /// Log scanners' configuration.
    pub scanner: ScannerConfig,
    /// Real-time events publisher's configuration.
    pub publisher: PublisherConfig,
    /// Holds collection-specific configuration overrides.
    pub collections: BTreeMap<String, PartialCollectionConfig>
}

impl Default for DatabaseConfig {
    fn default() -> DatabaseConfig {
        DatabaseConfig {
            logs_path: "".to_owned(),
            index_granularity: 100000,
            scanner: ScannerConfig::default(),
            publisher: PublisherConfig::default(),
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
                    scanner: match config.scanner {
                        Some(scanners_config) => ScannerConfig {
                            routing_strategy: scanners_config.routing_strategy.unwrap_or_else(|| self.scanner.routing_strategy.clone()),
                            threads: scanners_config.threads.unwrap_or(self.scanner.threads)
                        },
                        None => self.scanner.clone()
                    },
                    publisher: match config.publisher {
                        Some(publisher_config) => PublisherConfig {
                            buffer_size: publisher_config.buffer_size.unwrap_or(self.publisher.buffer_size)
                        },
                        None => self.publisher.clone()
                    }
                }
            },
            None => CollectionConfig {
                logs_path: self.logs_path.clone(),
                index_granularity: self.index_granularity,
                scanner: self.scanner.clone(),
                publisher: self.publisher.clone()
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
/// let config = ScannerConfig {
///     routing_strategy: RoutingStrategy::default(),
///     threads: 2
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerConfig {
    /// Subscriptions' routing strategy.
    pub routing_strategy: RoutingStrategy,
    /// Number of scanner threads for each log file.
    pub threads: u8
}

impl Default for ScannerConfig {
    fn default() -> ScannerConfig {
        ScannerConfig {
            routing_strategy: RoutingStrategy::default(),
            threads: 2
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
/// let config = PartialScannerConfig {
///     routing_strategy: Some(RoutingStrategy::default()),
///     threads: Some(2)
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialScannerConfig {
    /// Subscriptions' routing strategy.
    pub routing_strategy: Option<RoutingStrategy>,
    /// Number of scanner threads for each log file.
    pub threads: Option<u8>
}

/// Exar DB's publisher configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = PublisherConfig {
///     buffer_size: 1000
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PublisherConfig {
    /// Buffer size for events buffered in the `Publisher`.
    pub buffer_size: usize
}

impl Default for PublisherConfig {
    fn default() -> PublisherConfig {
        PublisherConfig {
            buffer_size: 1000
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
/// let config = PartialPublisherConfig {
///     buffer_size: Some(10000)
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialPublisherConfig {
    /// Buffer size for events buffered in the `Publisher`.
    pub buffer_size: Option<usize>
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
///     scanner: ScannerConfig {
///         routing_strategy: RoutingStrategy::default(),
///         threads: 2
///     },
///     publisher: PublisherConfig {
///         buffer_size: 1000
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
    /// Log scanners' configuration.
    pub scanner: ScannerConfig,
    /// Real-time events publisher's configuration.
    pub publisher: PublisherConfig
}

impl Default for CollectionConfig {
    fn default() -> CollectionConfig {
        let db_defaults = DatabaseConfig::default();
        CollectionConfig {
            logs_path: db_defaults.logs_path,
            index_granularity: db_defaults.index_granularity,
            scanner: db_defaults.scanner,
            publisher: db_defaults.publisher
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
///     scanner: Some(PartialScannerConfig {
///         routing_strategy: Some(RoutingStrategy::default()),
///         threads: Some(2)
///     }),
///     publisher: Some(PartialPublisherConfig {
///         buffer_size: Some(10000)
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
    /// Log scanners' configuration.
    pub scanner: Option<PartialScannerConfig>,
    /// Real-time events publisher's configuration.
    pub publisher: Option<PartialPublisherConfig>
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
        assert_eq!(collection_config.scanner, db_config.scanner);
        assert_eq!(collection_config.publisher, db_config.publisher);

        db_config.collections.insert("test".to_owned(), PartialCollectionConfig {
            logs_path: Some("test".to_owned()),
            index_granularity: Some(1000),
            scanner: Some(PartialScannerConfig {
                routing_strategy: Some(RoutingStrategy::Random),
                threads: Some(3)
            }),
            publisher: Some(PartialPublisherConfig {
                buffer_size: Some(10000)
            })
        });

        let collection_config = db_config.collection_config("test");

        assert_eq!(collection_config.logs_path, "test".to_owned());
        assert_eq!(collection_config.index_granularity, 1000);
        assert_eq!(collection_config.scanner, ScannerConfig {
            routing_strategy: RoutingStrategy::Random,
            threads: 3
        });
        assert_eq!(collection_config.publisher, PublisherConfig {
            buffer_size: 10000
        });
    }
}
