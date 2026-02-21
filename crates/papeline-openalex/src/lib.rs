//! Papeline OpenAlex - OpenAlex dataset pipeline
//!
//! This crate provides components for fetching and processing
//! OpenAlex academic data from their public S3 bucket.
//!
//! # Example
//!
//! ```no_run
//! use papeline_openalex::{Config, Entity, run};
//! use chrono::NaiveDate;
//!
//! let config = Config {
//!     entity: Entity::Works,
//!     since: Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
//!     ..Default::default()
//! };
//!
//! let summary = run(&config).expect("Pipeline failed");
//! println!("Processed {} rows", summary.total_rows);
//! ```

pub mod abstract_decode;
pub mod config;
pub mod manifest;
pub mod runner;
pub mod schema;
pub mod state;
pub mod transform;
pub mod worker;

// Re-exports for convenience
pub use config::Config;
pub use runner::{RunSummary, run};
pub use state::Entity;
