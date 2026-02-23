//! papeline-store: Content-addressable store for pipeline outputs
//!
//! Provides blake3-based caching and integrity verification for
//! pipeline stage outputs. Each stage's output is stored under its
//! input hash, enabling deterministic cache lookups.

pub mod hash;
pub mod manifest;
pub mod run_config;
pub mod stage;
pub mod store;

pub use hash::{combine_hashes, hash_bytes, hash_file, short_hash};
pub use manifest::StageManifest;
pub use run_config::RunConfig;
pub use stage::{StageInput, StageName};
pub use store::Store;
