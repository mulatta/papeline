use std::path::PathBuf;

/// Configuration for the join pipeline.
pub struct JoinConfig {
    /// Directory containing PubMed parquet files (pubmed_*.parquet)
    pub pubmed_dir: PathBuf,
    /// Directory containing OpenAlex parquet files (works_*.parquet)
    pub openalex_dir: PathBuf,
    /// Directory containing S2 papers parquet files (papers_*.parquet)
    pub s2_dir: PathBuf,
    /// Output directory for joined parquet files
    pub output_dir: PathBuf,
    /// DuckDB memory limit (e.g. "8GB")
    pub memory_limit: String,
}
