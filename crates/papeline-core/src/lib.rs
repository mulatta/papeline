//! Papeline Core - Common infrastructure for paper data pipelines
//!
//! This crate provides reusable components for fetching, processing,
//! and storing academic paper data from various sources.

pub mod filter;
pub mod lance_writer;
pub mod logging;
pub mod progress;
pub mod shutdown;
pub mod sink;
pub mod stream;

// Re-exports for convenience
pub use filter::{CorpusIdSet, MappedCorpusIds, load_corpus_ids, save_corpus_ids};
pub use lance_writer::LanceWriter;
pub use logging::{IndicatifLogger, init_logging};
pub use progress::{ProgressContext, SharedProgress};
pub use shutdown::{is_shutdown_requested, request_shutdown, shutdown_flag};
pub use sink::{ErrorFlag, LanceSink, ParquetSink, cleanup_tmp_files, is_valid_parquet};
pub use stream::{
    ByteCounter, GzipReader, SHARED_RUNTIME, StreamError, http_client, open_gzip_reader,
};
