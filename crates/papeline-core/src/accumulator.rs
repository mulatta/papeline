//! Batch accumulator trait and generic line processor for gzip shards

use std::io::BufRead;
use std::sync::atomic::Ordering;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use indicatif::ProgressBar;

use crate::progress::fmt_num;
use crate::stream::{ByteCounter, GzipReader};

/// Default batch size for flushing accumulated rows into a `RecordBatch`.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// Accumulator trait for batch processing of parsed rows into Arrow `RecordBatch`.
pub trait Accumulator {
    type Row;

    /// Push a row into the accumulator
    fn push(&mut self, row: Self::Row);

    /// Number of rows currently buffered
    fn len(&self) -> usize;

    /// Check if buffer is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if buffer is full and should be flushed
    fn is_full(&self) -> bool {
        self.len() >= DEFAULT_BATCH_SIZE
    }

    /// Take buffered rows as a RecordBatch, resetting internal state
    fn take_batch(&mut self) -> Result<RecordBatch, ArrowError>;
}

/// Statistics from processing shard lines
pub struct LineStats {
    pub lines_scanned: usize,
    pub rows_written: usize,
}

/// Initial capacity for per-line JSON read buffer
const LINE_BUF_CAPACITY: usize = 4096;

/// Progress update interval (every N lines to avoid overhead)
const UPDATE_INTERVAL: usize = 10_000;

/// Read lines from a gzip reader, parse+filter each, push to accumulator, flush batches.
///
/// Generic over any [`Accumulator`]. The caller provides:
/// - `write_batch`: sink callback for flushing full batches
/// - `parse_filter`: per-line parse+filter returning `Some(row)` to keep
pub fn process_lines<A: Accumulator>(
    reader: &mut GzipReader,
    counter: &ByteCounter,
    acc: &mut A,
    mut write_batch: impl FnMut(&RecordBatch) -> std::io::Result<()>,
    mut parse_filter: impl FnMut(&str) -> Option<A::Row>,
    pb: &ProgressBar,
) -> std::io::Result<LineStats> {
    let mut buf = String::with_capacity(LINE_BUF_CAPACITY);
    let mut rows_written = 0usize;
    let mut lines_scanned = 0usize;

    loop {
        buf.clear();
        if reader.read_line(&mut buf)? == 0 {
            break;
        }
        lines_scanned += 1;

        // Update progress bar periodically
        if lines_scanned.is_multiple_of(UPDATE_INTERVAL) {
            pb.set_position(counter.load(Ordering::Relaxed));
            let match_pct = if lines_scanned > 0 {
                rows_written as f64 / lines_scanned as f64 * 100.0
            } else {
                0.0
            };
            pb.set_message(format!(
                "{} rows ({:.1}%)",
                fmt_num(rows_written),
                match_pct
            ));
        }

        if let Some(row) = parse_filter(&buf) {
            acc.push(row);
            rows_written += 1;
            if acc.is_full() {
                write_batch(&acc.take_batch().map_err(std::io::Error::other)?)?;
            }
        }
    }
    if acc.len() > 0 {
        write_batch(&acc.take_batch().map_err(std::io::Error::other)?)?;
    }
    Ok(LineStats {
        lines_scanned,
        rows_written,
    })
}
