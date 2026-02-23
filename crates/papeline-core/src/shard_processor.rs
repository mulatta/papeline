//! High-level shard processor: retry + open_gzip_reader + process_lines + ParquetSink

use std::path::Path;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use indicatif::ProgressBar;

use crate::accumulator::{Accumulator, process_lines};
use crate::error::ShardError;
use crate::retry::retry_with_backoff;
use crate::sink::ParquetSink;
use crate::stream;

/// Statistics from processing a single gzip shard end-to-end
#[derive(Debug)]
pub struct GzipShardStats {
    pub lines_scanned: usize,
    pub rows_written: usize,
    pub elapsed: Duration,
}

/// Configuration for a single gzip shard processing run
pub struct ShardConfig<'a> {
    pub url: &'a str,
    pub shard_label: &'a str,
    pub output_prefix: &'a str,
    pub shard_idx: usize,
    pub output_dir: &'a Path,
    pub schema: &'a Schema,
    pub zstd_level: i32,
    pub content_length_hint: Option<u64>,
}

/// Process a gzip-compressed shard end-to-end:
/// retry → HTTP GET → gunzip → line parse/filter → accumulate → Parquet write.
///
/// `parse_filter` must be `Clone` because retries re-create the closure.
/// Closures capturing `&CorpusIdSet` or similar shared refs are auto-Clone.
pub fn process_gzip_shard<A: Accumulator>(
    cfg: &ShardConfig<'_>,
    pb: &ProgressBar,
    new_acc: impl Fn() -> A,
    parse_filter: impl FnMut(&str) -> Option<A::Row> + Clone,
) -> Result<GzipShardStats, ShardError> {
    retry_with_backoff(cfg.shard_label, pb, || {
        attempt_gzip_shard(cfg, pb, &new_acc, parse_filter.clone())
    })
}

fn attempt_gzip_shard<A: Accumulator>(
    cfg: &ShardConfig<'_>,
    pb: &ProgressBar,
    new_acc: &impl Fn() -> A,
    parse_filter: impl FnMut(&str) -> Option<A::Row>,
) -> Result<GzipShardStats, ShardError> {
    let start = Instant::now();

    let (mut reader, counter, total_bytes) =
        stream::open_gzip_reader(cfg.url).map_err(ShardError::Stream)?;

    if let Some(total) = total_bytes.or(cfg.content_length_hint) {
        crate::progress::upgrade_to_bar(pb, total);
    }
    pb.set_message("processing...");

    let mut sink = ParquetSink::new(
        cfg.output_prefix,
        cfg.shard_idx,
        cfg.output_dir,
        cfg.schema,
        cfg.zstd_level,
    )
    .map_err(ShardError::Io)?;

    let mut acc = new_acc();
    let stats = process_lines(
        &mut reader,
        &counter,
        &mut acc,
        |b| sink.write_batch(b),
        parse_filter,
        pb,
    )
    .map_err(ShardError::Io)?;

    sink.finalize().map_err(ShardError::Io)?;

    Ok(GzipShardStats {
        lines_scanned: stats.lines_scanned,
        rows_written: stats.rows_written,
        elapsed: start.elapsed(),
    })
}
