use std::{
    collections::HashMap,
    fs::{self, read_to_string},
};

use crate::common::error::ConfigError;

pub struct ConfigLoader;

impl ConfigLoader {
    /// Load an environment variable or return a ConfigError if it is not set.
    /// # Parameters
    /// - `key`: the environment variable key
    ///
    /// # Returns
    /// - `Ok(String)` if the environment variable was successfully loaded.
    /// - `Err(ConfigError)` if the environment variable could not be loaded.
    pub fn load_or_fail(key: &str) -> Result<String, ConfigError> {
        std::env::var(key)
            .map_err(|e| ConfigError::MissingRequiredEnvironmentVariable(format!("{key}: {e}")))
    }

    /// Try to parse an environment variable as the specified type.
    /// # Parameters
    /// - `key`: the environment variable key
    ///
    /// # Returns
    /// - `Ok(T)` if the environment variable was successfully loaded.
    /// - `Err(ConfigError)` if the environment variable could not be loaded.
    pub fn try_parse_env_var<T: std::str::FromStr>(key: &str) -> Result<T, ConfigError> {
        Self::load_or_fail(key).and_then(|v| {
            v.parse::<T>()
                .map_err(|_| ConfigError::ParseError(key.to_string()))
        })
    }

    /// Try to parse an environment variable as the specified type.
    /// If the environment variable is not set, return the default value.
    ///
    /// # Parameters
    /// - `key`: the environment variable key
    /// - `default`: the default value to use if the environment variable is not set
    ///
    /// # Returns
    /// - `Ok(T)` if the environment variable was successfully loaded.
    /// - `Err(ConfigError)` if the environment variable could not be loaded.
    pub fn parse_or_default<T: std::str::FromStr>(key: &str, default: T) -> Result<T, ConfigError> {
        Self::try_parse_env_var(key).or_else(|_| Ok(default))
    }

    /// Load an optional environment variable.
    ///
    /// # Arguments
    /// - `key`: the environment variable key
    ///
    /// # Returns
    /// - `Some(T)` if the environment variable was successfully loaded.
    /// - `None` if the environment variable could not be loaded.
    pub fn optional_env_var<T: std::str::FromStr>(key: &str) -> Option<T> {
        std::env::var(key).ok().and_then(|v| v.parse().ok())
    }

    /// Load a flat config file.
    /// # Parameters
    /// - `path`: the path to the config file.
    ///
    /// # Returns
    /// - `Ok(HashMap<String, String>)` if the config was successfully loaded.
    /// - `Err(ConfigError)` if the config could not be loaded.
    pub fn load_flat_config(path: &str) -> Result<HashMap<String, String>, ConfigError> {
        let content = read_to_string(path)
            .map_err(|e| ConfigError::ConfigFileNotFound(format!("{} ({e})", path)))?;

        let mut props = HashMap::new();
        for (idx, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once(':') else {
                return Err(ConfigError::ParseError(format!(
                    "Invalid line #{idx} in config file: {line}"
                )));
            };
            props.insert(key.trim().to_string(), value.trim().to_string());
        }

        Ok(props)
    }

    /// Load a YAML config file.
    /// # Parameters
    /// - `path`: the path to the config file.
    ///
    /// # Returns
    /// - `Ok(T)` if the config was successfully loaded.
    /// - `Err(ConfigError)` if the config could not be loaded.
    pub fn load_yaml_config<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, ConfigError> {
        let file = fs::read_to_string(path)
            .map_err(|_| ConfigError::ConfigFileNotFound(path.to_string()))?;
        let config: T =
            serde_yaml::from_str(&file).map_err(|_| ConfigError::ParseError(path.to_string()))?;
        Ok(config)
    }
}
