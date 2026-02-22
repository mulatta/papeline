//! Papeline PubMed - PubMed data pipeline
//!
//! Fetches and transforms PubMed baseline data to Parquet format.
//!
//! # Features
//!
//! - Complete field coverage: MeSH terms, chemicals, grants, keywords, etc.
//! - Streaming XML parsing with quick-xml
//! - Parallel processing with rayon
//!
//! # Example
//!
//! ```ignore
//! use papeline_pubmed::{Config, run};
//!
//! let config = Config {
//!     output_dir: "output".into(),
//!     max_files: Some(1),
//!     ..Default::default()
//! };
//!
//! let summary = run(&config)?;
//! println!("Processed {} articles", summary.total_articles);
//! ```

pub mod config;
pub mod manifest;
pub mod parser;
pub mod runner;
pub mod schema;
pub mod transform;
pub mod worker;

// Re-exports
pub use config::Config;
pub use runner::{Summary, run};
