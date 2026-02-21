//! Shard processing — download and transform OpenAlex data

use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use indicatif::ProgressBar;
use papeline_core::sink::ParquetSink;
use papeline_core::stream;

use crate::schema;
use crate::state::ShardInfo;
use crate::transform::{Accumulator, WorkAccumulator, WorkRow};

/// Maximum retry attempts for transient failures
const MAX_RETRIES: u32 = 3;

/// Initial capacity for per-line JSON read buffer
const LINE_BUF_CAPACITY: usize = 8192;

/// Progress update interval (every N lines)
const UPDATE_INTERVAL: usize = 10_000;

/// Statistics from processing a single shard
#[derive(Debug)]
pub struct ShardStats {
    pub shard_idx: usize,
    pub lines_scanned: usize,
    pub parse_errors: usize,
    pub rows_written: usize,
    pub elapsed: Duration,
}

impl ShardStats {
    /// Log stats for non-TTY output
    pub fn log(&self) {
        log::info!(
            "shard_{:04}: {} rows from {} lines ({} errors) in {:.1}s",
            self.shard_idx,
            self.rows_written,
            self.lines_scanned,
            self.parse_errors,
            self.elapsed.as_secs_f64()
        );
    }
}

/// Process a single shard with retries
pub fn process_shard(
    shard: &ShardInfo,
    output_dir: &Path,
    zstd_level: i32,
    pb: &ProgressBar,
) -> Result<ShardStats, ShardError> {
    let mut attempt = 0;
    loop {
        match attempt_shard(shard, output_dir, zstd_level, pb) {
            Ok(stats) => return Ok(stats),
            Err(e) if attempt < MAX_RETRIES && e.is_retryable() => {
                attempt += 1;
                pb.set_message(format!("retry {attempt}/{MAX_RETRIES}..."));
                log::debug!(
                    "shard_{:04}: attempt {}/{} failed: {e}, retrying...",
                    shard.shard_idx,
                    attempt,
                    MAX_RETRIES
                );
                std::thread::sleep(backoff_duration(attempt));
            }
            Err(e) => {
                log::error!("shard_{:04}: failed permanently: {e}", shard.shard_idx);
                return Err(e);
            }
        }
    }
}

/// Single attempt to process a shard
fn attempt_shard(
    shard: &ShardInfo,
    output_dir: &Path,
    zstd_level: i32,
    pb: &ProgressBar,
) -> Result<ShardStats, ShardError> {
    let start = Instant::now();

    // Open gzip stream
    let (mut reader, counter, total_bytes) =
        stream::open_gzip_reader(&shard.url).map_err(ShardError::Stream)?;

    // Upgrade spinner to progress bar if we know total size
    if let Some(total) = total_bytes.or(shard.content_length) {
        pb.set_length(total);
        pb.set_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} {prefix} [{bar:30.cyan/blue}] {bytes}/{total_bytes} {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
    }
    pb.set_message("processing...");

    // Create output sink
    let mut sink = ParquetSink::new(
        shard.entity.output_prefix(),
        shard.shard_idx,
        output_dir,
        schema::works(),
        zstd_level,
    )
    .map_err(ShardError::Io)?;

    let mut acc = WorkAccumulator::new();
    let mut buf = String::with_capacity(LINE_BUF_CAPACITY);
    let mut lines_scanned = 0usize;
    let mut parse_errors = 0usize;

    loop {
        buf.clear();
        if reader.read_line(&mut buf).map_err(ShardError::Io)? == 0 {
            break;
        }
        lines_scanned += 1;

        // Update progress periodically
        if lines_scanned.is_multiple_of(UPDATE_INTERVAL) {
            pb.set_position(counter.load(Ordering::Relaxed));
            pb.set_message(format!(
                "{} rows",
                acc.len() + (lines_scanned - parse_errors)
            ));
        }

        // Parse JSON line
        let row: WorkRow = match serde_json::from_str(&buf) {
            Ok(r) => r,
            Err(e) => {
                if parse_errors < 5 {
                    log::debug!(
                        "shard_{:04}: parse error at line {}: {e}",
                        shard.shard_idx,
                        lines_scanned
                    );
                }
                parse_errors += 1;
                continue;
            }
        };

        acc.push(row);

        // Flush when batch is full
        if acc.is_full() {
            sink.write_batch(&acc.take_batch())
                .map_err(ShardError::Io)?;
        }
    }

    // Flush remaining rows
    pb.set_message("writing...");
    if !acc.is_empty() {
        sink.write_batch(&acc.take_batch())
            .map_err(ShardError::Io)?;
    }

    let rows_written = sink.finalize().map_err(ShardError::Io)?;

    Ok(ShardStats {
        shard_idx: shard.shard_idx,
        lines_scanned,
        parse_errors,
        rows_written,
        elapsed: start.elapsed(),
    })
}

/// Exponential backoff duration
const fn backoff_duration(attempt: u32) -> Duration {
    Duration::from_secs(2u64.pow(attempt))
}

// === Error type ===

#[derive(Debug)]
pub enum ShardError {
    Stream(stream::StreamError),
    Io(std::io::Error),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "IO: {e}"),
        }
    }
}

impl std::error::Error for ShardError {}

impl ShardError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Stream(e) => e.is_retryable(),
            Self::Io(e) => e.kind() != std::io::ErrorKind::StorageFull,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn backoff_exponential() {
        assert_eq!(backoff_duration(1), Duration::from_secs(2));
        assert_eq!(backoff_duration(2), Duration::from_secs(4));
        assert_eq!(backoff_duration(3), Duration::from_secs(8));
    }

    #[test]
    fn shard_error_io_storage_full_not_retryable() {
        let err = ShardError::Io(std::io::Error::new(ErrorKind::StorageFull, "disk full"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn shard_error_io_other_retryable() {
        let err = ShardError::Io(std::io::Error::new(ErrorKind::BrokenPipe, "pipe"));
        assert!(err.is_retryable());
    }

    #[test]
    fn shard_error_display_io() {
        let err = ShardError::Io(std::io::Error::new(ErrorKind::NotFound, "not found"));
        let msg = format!("{err}");
        assert!(msg.contains("IO:"));
    }

    #[test]
    fn shard_stats_log_does_not_panic() {
        let stats = ShardStats {
            shard_idx: 0,
            lines_scanned: 100,
            parse_errors: 5,
            rows_written: 95,
            elapsed: Duration::from_secs(1),
        };
        // Just verify it doesn't panic
        stats.log();
    }
}
