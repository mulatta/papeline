//! PubMed pipeline configuration

use std::path::PathBuf;

/// Runtime configuration for PubMed pipeline
#[derive(Debug)]
pub struct Config {
    /// Output directory for parquet files
    pub output_dir: PathBuf,
    /// Number of parallel workers
    pub workers: usize,
    /// Maximum files to process (for testing)
    pub max_files: Option<usize>,
    /// Zstd compression level for parquet output
    pub zstd_level: i32,
    /// Base URL for PubMed FTP
    pub base_url: String,
}

impl Config {
    /// Get default worker count (number of CPUs, capped at 8)
    pub fn num_cpus() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get().min(8))
            .unwrap_or(4)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("output"),
            workers: Self::num_cpus(),
            max_files: None,
            zstd_level: 3,
            base_url: "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.output_dir, PathBuf::from("output"));
        assert!(config.workers >= 1 && config.workers <= 8);
        assert!(config.max_files.is_none());
        assert_eq!(config.zstd_level, 3);
        assert!(config.base_url.starts_with("https://"));
    }

    #[test]
    fn num_cpus_returns_valid_count() {
        let cpus = Config::num_cpus();
        assert!(cpus >= 1);
        assert!(cpus <= 8);
    }
}
