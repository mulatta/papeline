//! Papeline Core - Common infrastructure for paper data pipelines
//!
//! This crate provides reusable components for fetching, processing,
//! and storing academic paper data from various sources.

pub mod accumulator;
pub mod error;
pub mod filter;
pub mod lance_writer;
pub mod logging;
pub mod progress;
pub mod retry;
pub mod semaphore;
pub mod shard_processor;
pub mod shutdown;
pub mod sink;
pub mod stream;
pub mod work_queue;

// Re-exports for convenience
pub use accumulator::{Accumulator, DEFAULT_BATCH_SIZE, LineStats, process_lines};
pub use error::ShardError;
pub use filter::{CorpusIdSet, MappedCorpusIds, load_corpus_ids, save_corpus_ids};
pub use lance_writer::LanceWriter;
pub use logging::{IndicatifLogger, init_logging};
pub use progress::{ProgressContext, SharedProgress};
pub use retry::{backoff_duration, retry_with_backoff};
pub use semaphore::{Semaphore, SemaphoreGuard};
pub use shard_processor::{GzipShardStats, ShardConfig, process_gzip_shard};
pub use shutdown::{is_shutdown_requested, request_shutdown, shutdown_flag};
pub use sink::{ErrorFlag, LanceSink, ParquetSink, cleanup_tmp_files, is_valid_parquet};
pub use stream::{
    ByteCounter, GzipReader, HttpConfig, SHARED_RUNTIME, StreamError, acquire_download_permit,
    http_client, http_config, open_gzip_reader, set_download_concurrency, set_http_config, stagger,
};
pub use work_queue::WorkQueue;
