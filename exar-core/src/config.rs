use super::*;

use std::collections::BTreeMap;

pub const DEFAULT_SCANNER_THREADS: u8    = 2;
pub const DEFAULT_INDEX_GRANULARITY: u64 = 100000;

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
///     data: DataConfig {
///         path: "/path/to/logs".to_owned(),
///         index_granularity: 100000,
///     },
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
    /// Data configuration.
    pub data: DataConfig,
    /// Log scanners' configuration.
    pub scanner: ScannerConfig,
    /// Real-time events publisher's configuration.
    pub publisher: PublisherConfig,
    /// Holds collection-specific configuration overrides.
    pub collections: BTreeMap<String, PartialCollectionConfig>
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            data: DataConfig::default(),
            scanner: ScannerConfig::default(),
            publisher: PublisherConfig::default(),
            collections: BTreeMap::new()
        }
    }
}

impl DatabaseConfig {
    /// Returns the configuration for a given collection
    /// by applying overrides to the base `DatabaseConfig`.
    pub fn collection_config(&self, collection_name: &str) -> CollectionConfig {
        match self.collections.get(collection_name) {
            Some(collection_config) => {
                let config = collection_config.clone();
                CollectionConfig {
                    data: match config.data {
                        Some(data_config) => DataConfig {
                            path: data_config.path.unwrap_or_else(|| self.data.path.to_owned()),
                            index_granularity: data_config.index_granularity.unwrap_or(self.data.index_granularity)
                        },
                        None => self.data.clone()
                    },
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
                data: self.data.clone(),
                scanner: self.scanner.clone(),
                publisher: self.publisher.clone()
            }
        }
    }
}

/// Exar DB's data configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = DataConfig {
///     path: "".to_owned(),
///     index_granularity: 100000
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DataConfig {
    /// Path to the data directory.
    pub path: String,
    /// Granularity of the log lines index (used by `IndexedLineReader`).
    pub index_granularity: u64
}

impl Default for DataConfig {
    fn default() -> Self {
        DataConfig {
            path: "".to_owned(),
            index_granularity: DEFAULT_INDEX_GRANULARITY
        }
    }
}

/// Exar DB's partial data configuration.
/// Holds overrides for the main database configuration.
///
/// # Examples
/// ```
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let config = PartialDataConfig {
///     path: Some("test".to_owned()),
///     index_granularity: Some(1000)
/// };
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialDataConfig {
    /// Path to the data directory.
    pub path: Option<String>,
    /// Granularity of the log lines index (used by `IndexedLineReader`).
    pub index_granularity: Option<u64>
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
    fn default() -> Self {
        ScannerConfig {
            routing_strategy: RoutingStrategy::default(),
            threads: DEFAULT_SCANNER_THREADS
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
    fn default() -> Self {
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
///     data: DataConfig {
///         path: "/path/to/logs".to_owned(),
///         index_granularity: 100000
///     },
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
    /// Data configuration.
    pub data: DataConfig,
    /// Log scanners' configuration.
    pub scanner: ScannerConfig,
    /// Real-time events publisher's configuration.
    pub publisher: PublisherConfig
}

impl Default for CollectionConfig {
    fn default() -> Self {
        let db_defaults = DatabaseConfig::default();
        CollectionConfig {
            data: db_defaults.data,
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
///     data: Some(PartialDataConfig {
///         path: Some("/path/to/logs".to_owned()),
///         index_granularity: Some(100000),
///     }),
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
    /// Data configuration.
    pub data: Option<PartialDataConfig>,
    /// Log scanners' configuration.
    pub scanner: Option<PartialScannerConfig>,
    /// Real-time events publisher's configuration.
    pub publisher: Option<PartialPublisherConfig>
}

#[cfg(test)]
mod tests {
    use testkit::*;

    #[test]
    fn test_collection_config() {
        let mut db_config = DatabaseConfig::default();

        let collection_config = db_config.collection_config("test");

        assert_eq!(collection_config.data, db_config.data);
        assert_eq!(collection_config.scanner, db_config.scanner);
        assert_eq!(collection_config.publisher, db_config.publisher);

        db_config.collections.insert("test".to_owned(), PartialCollectionConfig {
            data: Some(PartialDataConfig {
                path: Some("test".to_owned()),
                index_granularity: Some(1000)
            }),
            scanner: Some(PartialScannerConfig {
                routing_strategy: Some(RoutingStrategy::Random),
                threads: Some(3)
            }),
            publisher: Some(PartialPublisherConfig {
                buffer_size: Some(10000)
            })
        });

        let collection_config = db_config.collection_config("test");

        assert_eq!(collection_config.data, DataConfig {
            path: "test".to_owned(),
            index_granularity: 1000
        });
        assert_eq!(collection_config.scanner, ScannerConfig {
            routing_strategy: RoutingStrategy::Random,
            threads: 3
        });
        assert_eq!(collection_config.publisher, PublisherConfig {
            buffer_size: 10000
        });
    }
}
