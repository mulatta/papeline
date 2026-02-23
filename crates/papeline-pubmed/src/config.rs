//! PubMed pipeline configuration

use std::path::PathBuf;

/// Runtime configuration for PubMed pipeline
#[derive(Debug)]
pub struct Config {
    /// Output directory for parquet files
    pub output_dir: PathBuf,
    /// Maximum files to process (for testing)
    pub max_files: Option<usize>,
    /// Zstd compression level for parquet output
    pub zstd_level: i32,
    /// Base URL for PubMed FTP
    pub base_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("output"),
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
        assert!(config.max_files.is_none());
        assert_eq!(config.zstd_level, 3);
        assert!(config.base_url.starts_with("https://"));
    }
}
