//! Papeline S2 - Semantic Scholar data pipeline
//!
//! This crate provides components for fetching and processing
//! Semantic Scholar bulk dataset.

pub mod api;
pub mod cli;
pub mod config;
pub mod coverage;
pub mod runner;
pub mod schema;
pub mod state;
pub mod stats;
pub mod transform;
pub mod url_expiry;
pub mod worker;

// Re-exports
pub use api::{cmd_dataset_list, cmd_release_list};
pub use cli::{Cli, Command, DatasetAction, FetchArgs, ReleaseAction};
pub use config::Config;
pub use runner::run;
pub use state::Dataset;
