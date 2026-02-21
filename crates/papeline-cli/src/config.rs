//! Configuration loading from TOML files

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Global configuration for papeline
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub output: OutputConfig,
    pub openalex: OpenAlexConfig,
    pub s2: S2Config,
    pub workers: WorkersConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OutputConfig {
    pub default_dir: PathBuf,
    pub compression_level: i32,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            default_dir: PathBuf::from("./data"),
            compression_level: 3,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OpenAlexConfig {
    pub base_url: String,
}

impl Default for OpenAlexConfig {
    fn default() -> Self {
        Self {
            base_url: "https://openalex.s3.amazonaws.com/".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct S2Config {
    pub api_url: String,
    #[serde(deserialize_with = "deserialize_env_var")]
    pub api_key: Option<String>,
}

impl Default for S2Config {
    fn default() -> Self {
        Self {
            api_url: "https://api.semanticscholar.org/datasets/v1/".to_string(),
            api_key: std::env::var("S2_API_KEY").ok(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct WorkersConfig {
    pub default: usize,
    pub max: usize,
}

impl Default for WorkersConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            default: cpus.min(8),
            max: 16,
        }
    }
}

/// Deserialize a string that may contain environment variable reference like ${VAR}
fn deserialize_env_var<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.and_then(|s| expand_env_var(&s)))
}

/// Expand ${VAR} to environment variable value
fn expand_env_var(s: &str) -> Option<String> {
    if let Some(var_name) = s.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        std::env::var(var_name).ok()
    } else {
        Some(s.to_string())
    }
}

impl Config {
    /// Load configuration from default locations
    ///
    /// Search order:
    /// 1. ./papeline.toml (current directory)
    /// 2. ~/.config/papeline/config.toml
    ///
    /// If no config file found, returns default config.
    pub fn load() -> Result<Self> {
        // Try current directory first
        let local_config = PathBuf::from("papeline.toml");
        if local_config.exists() {
            return Self::from_file(&local_config);
        }

        // Try user config directory
        if let Some(config_dir) = directories::ProjectDirs::from("", "", "papeline") {
            let user_config = config_dir.config_dir().join("config.toml");
            if user_config.exists() {
                return Self::from_file(&user_config);
            }
        }

        // Return defaults if no config found
        log::debug!("No config file found, using defaults");
        Ok(Self::default())
    }

    /// Load configuration from a specific file
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        log::info!("Loaded config from {}", path.display());
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.output.default_dir, PathBuf::from("./data"));
        assert_eq!(config.output.compression_level, 3);
        assert!(config.workers.default >= 1);
    }

    #[test]
    fn expand_env_var_simple() {
        std::env::set_var("TEST_VAR", "test_value");
        assert_eq!(
            expand_env_var("${TEST_VAR}"),
            Some("test_value".to_string())
        );
        std::env::remove_var("TEST_VAR");
    }

    #[test]
    fn expand_env_var_literal() {
        assert_eq!(expand_env_var("literal"), Some("literal".to_string()));
    }

    #[test]
    fn expand_env_var_missing() {
        assert_eq!(expand_env_var("${NONEXISTENT_VAR_12345}"), None);
    }

    #[test]
    fn parse_config_toml() {
        let toml = r#"
[output]
default_dir = "/tmp/data"
compression_level = 5

[workers]
default = 4
max = 8
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.output.default_dir, PathBuf::from("/tmp/data"));
        assert_eq!(config.output.compression_level, 5);
        assert_eq!(config.workers.default, 4);
        assert_eq!(config.workers.max, 8);
    }
}
