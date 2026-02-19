//! Dedicated writer thread for Lance dataset output

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;

use arrow::array::RecordBatch;
use lance::dataset::{Dataset, WriteMode, WriteParams};

use crate::sink::ErrorFlag;

/// Rows to buffer before flushing to lance
const LANCE_FLUSH_THRESHOLD: usize = 500_000;

/// Receives `RecordBatch`es from worker threads and writes them to a lance dataset.
///
/// Runs on a dedicated thread with its own tokio `current_thread` runtime
/// (lance requires async). Buffers batches and flushes when threshold is reached.
/// On error, sets the shared `error_flag` so `LanceSink` instances fast-fail.
pub struct LanceWriter {
    rx: Receiver<RecordBatch>,
    path: PathBuf,
    error_flag: ErrorFlag,
}

impl LanceWriter {
    pub fn new(rx: Receiver<RecordBatch>, path: PathBuf, error_flag: ErrorFlag) -> Self {
        Self {
            rx,
            path,
            error_flag,
        }
    }

    /// Entry point — builds a tokio runtime and runs the writer loop
    pub fn run(self) -> Result<u64, io::Error> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(self.writer_loop())
    }

    async fn writer_loop(self) -> Result<u64, io::Error> {
        // Overwrite any partial lance data from a previous failed run
        if self.path.exists() {
            fs::remove_dir_all(&self.path)?;
        }

        let mut buffer: Vec<RecordBatch> = Vec::new();
        let mut buffer_rows = 0u64;
        let mut total_rows = 0u64;
        let path_str = self.path.to_string_lossy().to_string();
        let mut dataset_initialized = false;

        for batch in self.rx.iter() {
            buffer_rows += batch.num_rows() as u64;
            buffer.push(batch);

            if buffer_rows >= LANCE_FLUSH_THRESHOLD as u64 {
                if let Err(e) = Self::flush(&path_str, &mut buffer, &mut dataset_initialized).await
                {
                    self.error_flag.store(true, Ordering::Relaxed);
                    return Err(e);
                }
                total_rows += buffer_rows;
                buffer_rows = 0;
            }
        }

        // Final flush on channel close (all senders dropped)
        if !buffer.is_empty() {
            if let Err(e) = Self::flush(&path_str, &mut buffer, &mut dataset_initialized).await {
                self.error_flag.store(true, Ordering::Relaxed);
                return Err(e);
            }
            total_rows += buffer_rows;
        }

        Ok(total_rows)
    }

    async fn flush(
        path: &str,
        buffer: &mut Vec<RecordBatch>,
        dataset_initialized: &mut bool,
    ) -> Result<(), io::Error> {
        let batches = std::mem::take(buffer);
        let schema = batches[0].schema();
        let reader = arrow::array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        if !*dataset_initialized {
            Dataset::write(reader, path, None)
                .await
                .map_err(lance_err)?;
            *dataset_initialized = true;
        } else {
            let params = WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            };
            Dataset::write(reader, path, Some(params))
                .await
                .map_err(lance_err)?;
        }
        Ok(())
    }
}

fn lance_err(e: lance::error::Error) -> io::Error {
    io::Error::other(e.to_string())
}
