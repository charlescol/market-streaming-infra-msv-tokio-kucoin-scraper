use serde::Deserialize;
use std::collections::{HashMap, HashSet};

use crate::common::{config_loader::ConfigLoader, error::ConfigError};

#[derive(Debug, Clone)]
pub struct Config {
    pub tokio_worker_threads: Option<usize>,
    pub tokio_queue_interval: Option<u32>,
    pub tokio_event_interval: Option<u32>,
    pub enable_scraper: bool,
    pub workflow: WorkflowConfig,
    pub kafka: KafkaConfig,
    pub port: u16,
    pub symbols: Vec<String>,
    pub symbol_entries: Vec<SymbolEntry>,
    pub monitoring: MonitoringConfig,
    pub source_tls: bool,
    pub kucoin: KucoinConfig,
}

#[derive(Debug, Clone)]
pub struct KucoinConfig {
    pub host: String,
}

#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    pub track_symbols: HashSet<String>,
    pub enable_metrics_verbose: bool,
    pub metrics_port: u16,
    pub monitor_rdkafka: bool,
    pub monitoring_ratio: f64,
    pub monitor_symbols: HashSet<String>,
    pub monitor_msg: bool,
}

#[derive(Debug, Clone)]
pub struct WorkflowConfig {
    pub stream_group_count: usize,
    pub process_group_count: usize,
    pub process_queue_capacity: usize,
    pub max_inflight_by_process_group: usize,
}

/// Configuration for the Kafka producer.
#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub client_id: String,
    pub broker_host: String,
    pub topic: String,
    pub schema_registry_url: String,
    pub producer_config: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum SymbolEntry {
    Simple(String),
    Mapping {
        symbol: String,
        #[serde(default)]
        kucoin: Option<String>,
    },
}

impl SymbolEntry {
    pub fn canonical(&self) -> String {
        match self {
            SymbolEntry::Simple(s) => s.clone(),
            SymbolEntry::Mapping { symbol, .. } => symbol.clone(),
        }
    }

    pub fn kucoin_symbol(&self) -> String {
        match self {
            SymbolEntry::Simple(s) => s.clone(),
            SymbolEntry::Mapping { symbol, kucoin } => {
                kucoin.clone().unwrap_or_else(|| symbol.clone())
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SymbolConfig {
    pub symbols: Vec<SymbolEntry>,
}

impl Config {
    /// Creates a new Config instance by reading environment variables and loading config files.
    /// # Returns
    /// - `Ok(Config)` if the configuration was successfully loaded.    
    /// - `Err(ConfigError)` if the configuration could not be loaded.
    pub fn new() -> Result<Self, ConfigError> {
        Ok(Config {
            tokio_worker_threads: ConfigLoader::optional_env_var("TOKIO_WORKER_THREADS"),
            tokio_queue_interval: ConfigLoader::optional_env_var("TOKIO_QUEUE_INTERVAL"),
            tokio_event_interval: ConfigLoader::optional_env_var("TOKIO_EVENT_INTERVAL"),
            enable_scraper: ConfigLoader::parse_or_default("ENABLE_SCRAPER", false)?,
            monitoring: Self::load_monitoring_config()?,
            kucoin: KucoinConfig {
                host: ConfigLoader::parse_or_default(
                    "KUCOIN_API_HOST",
                    "https://api.kucoin.com".to_string(),
                )?,
            },

            workflow: Self::load_workflow_config()?,
            kafka: Self::load_kafka_config()?,
            port: ConfigLoader::try_parse_env_var("PORT").or_else(|_| Ok(3000))?,
            symbols: ConfigLoader::load_yaml_config::<SymbolConfig>(&ConfigLoader::load_or_fail(
                "SYMBOL_CONFIG_PATH",
            )?)?
            .symbols
            .iter()
            .map(|s| s.canonical())
            .collect(),
            symbol_entries: ConfigLoader::load_yaml_config::<SymbolConfig>(
                &ConfigLoader::load_or_fail("SYMBOL_CONFIG_PATH")?,
            )?
            .symbols,
            source_tls: ConfigLoader::parse_or_default("SOURCE_TLS", false)?,
        })
    }

    /// Load the workflow configuration.
    ///
    /// # Returns
    /// - `Ok(WorkflowConfig)` if the configuration was successfully loaded.
    /// - `Err(ConfigError)` if the configuration could not be loaded.
    pub fn load_workflow_config() -> Result<WorkflowConfig, ConfigError> {
        Ok(WorkflowConfig {
            stream_group_count: ConfigLoader::try_parse_env_var("STREAM_GROUP_COUNT")?,
            process_group_count: ConfigLoader::try_parse_env_var("PROCESS_GROUP_COUNT")?,
            process_queue_capacity: ConfigLoader::try_parse_env_var("PROCESS_QUEUE_CAPACITY")?,
            max_inflight_by_process_group: ConfigLoader::parse_or_default(
                "MAX_INFLIGHT_BY_PROCESS_GROUP",
                20,
            )?,
        })
    }

    /// Load the monitoring configuration.
    ///
    /// # Returns
    /// - `Ok(MonitoringConfig)` if the configuration was successfully loaded.
    /// - `Err(ConfigError)` if the configuration could not be loaded.
    pub fn load_monitoring_config() -> Result<MonitoringConfig, ConfigError> {
        let monitoring_ratio = ConfigLoader::parse_or_default("MONITORING_RATIO", 0.1)?;
        if monitoring_ratio < 0.0 || monitoring_ratio > 1.0 {
            return Err(ConfigError::InvalidValue(
                "MONITORING_RATIO must be between 0 and 1".to_string(),
            ));
        }
        Ok(MonitoringConfig {
            enable_metrics_verbose: ConfigLoader::parse_or_default(
                "ENABLE_METRICS_VERBOSE",
                false,
            )?,
            track_symbols: ConfigLoader::parse_or_default("TRACKED_SYMBOLS", "".to_string())?
                .split(',')
                .map(|s| s.replace(' ', "").to_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
            metrics_port: ConfigLoader::parse_or_default("METRICS_PORT", 9000)?,
            monitor_rdkafka: ConfigLoader::parse_or_default("MONITOR_RDKAFKA", false)?,
            monitoring_ratio,
            monitor_msg: ConfigLoader::parse_or_default("MONITOR_MSG", false)?,
            monitor_symbols: ConfigLoader::parse_or_default("MONITOR_SYMBOLS", "".to_string())?
                .split(',')
                .map(|s| s.replace(' ', "").to_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
        })
    }

    /// Load the Kafka configuration.
    ///
    /// # Returns
    /// - `Ok(KafkaConfig)` if the configuration was successfully loaded.
    /// - `Err(ConfigError)` if the configuration could not be loaded.
    fn load_kafka_config() -> Result<KafkaConfig, ConfigError> {
        let config_path = ConfigLoader::load_or_fail("KAFKA_CONFIG_PATH")?;
        Ok(KafkaConfig {
            client_id: ConfigLoader::load_or_fail("KAFKA_CLIENT_ID")?,
            broker_host: ConfigLoader::load_or_fail("KAFKA_BROKER_HOST")?,
            topic: ConfigLoader::load_or_fail("KAFKA_TOPIC")?,
            schema_registry_url: ConfigLoader::load_or_fail("SCHEMA_REGISTRY_URL")?,
            producer_config: ConfigLoader::load_flat_config(&config_path)?,
        })
    }
}
