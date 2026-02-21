//! OpenAlex pipeline configuration

use std::path::PathBuf;

use chrono::NaiveDate;

use crate::state::Entity;

/// Runtime configuration for OpenAlex pipeline
#[derive(Debug)]
pub struct Config {
    /// Output directory for parquet files
    pub output_dir: PathBuf,
    /// Entity type to fetch
    pub entity: Entity,
    /// Only fetch records updated since this date (inclusive)
    pub since: Option<NaiveDate>,
    /// Number of parallel workers
    pub workers: usize,
    /// Maximum shards to process (for testing)
    pub max_shards: Option<usize>,
    /// Zstd compression level for parquet output
    pub zstd_level: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("output"),
            entity: Entity::Works,
            since: None,
            workers: num_cpus(),
            max_shards: None,
            zstd_level: 3,
        }
    }
}

/// Get default worker count (number of CPUs, capped at 8)
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.output_dir, PathBuf::from("output"));
        assert_eq!(config.entity, Entity::Works);
        assert!(config.since.is_none());
        assert!(config.workers > 0 && config.workers <= 8);
        assert!(config.max_shards.is_none());
        assert_eq!(config.zstd_level, 3);
    }

    #[test]
    fn num_cpus_returns_valid_count() {
        let count = num_cpus();
        assert!(count >= 1);
        assert!(count <= 8);
    }
}
