//! Output sinks — Parquet file writer and Lance channel sender

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Shared error flag — `LanceWriter` sets on failure, `LanceSink` checks before send
pub type ErrorFlag = Arc<AtomicBool>;

/// Buffered parquet writer with atomic tmp→rename
pub struct ParquetSink {
    writer: ArrowWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    row_count: usize,
}

impl std::fmt::Debug for ParquetSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSink")
            .field("final_path", &self.final_path)
            .field("row_count", &self.row_count)
            .finish_non_exhaustive()
    }
}

impl ParquetSink {
    /// Create a new sink writing to a temporary file
    pub fn new(
        dataset: &str,
        shard_idx: usize,
        output_dir: &Path,
        schema: &Schema,
        zstd_level: i32,
    ) -> Result<Self, std::io::Error> {
        let filename = format!("{dataset}_{shard_idx:04}.parquet");
        let final_path = output_dir.join(&filename);
        let tmp_path = output_dir.join(format!("{filename}.tmp"));

        // Clean up stale tmp file
        if tmp_path.exists() {
            fs::remove_file(&tmp_path)?;
        }

        let file = File::create(&tmp_path)?;
        let level = ZstdLevel::try_new(zstd_level)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(level))
            .set_max_row_group_size(1024 * 1024) // 1M rows per row group
            .build();

        let writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .map_err(std::io::Error::other)?;

        Ok(Self {
            writer,
            tmp_path,
            final_path,
            row_count: 0,
        })
    }

    /// Write a record batch
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), std::io::Error> {
        self.row_count += batch.num_rows();
        self.writer.write(batch).map_err(std::io::Error::other)
    }

    /// Finalize: flush footer and atomically rename tmp → final
    pub fn finalize(self) -> Result<usize, std::io::Error> {
        let row_count = self.row_count;
        self.writer.close().map_err(std::io::Error::other)?;
        fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(row_count)
    }
}

/// Check if a completed parquet file exists and has a valid footer
pub fn is_valid_parquet(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    parquet::file::reader::SerializedFileReader::new(file).is_ok()
}

/// Remove stale .tmp files in the output directory
pub fn cleanup_tmp_files(output_dir: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "tmp") {
            log::warn!("Removing stale tmp file: {}", path.display());
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Sink that sends `RecordBatch`es to a `LanceWriter` thread via bounded channel
pub struct LanceSink {
    sender: SyncSender<RecordBatch>,
    error_flag: ErrorFlag,
    row_count: usize,
}

impl std::fmt::Debug for LanceSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceSink")
            .field("row_count", &self.row_count)
            .finish_non_exhaustive()
    }
}

impl LanceSink {
    pub fn new(sender: SyncSender<RecordBatch>, error_flag: ErrorFlag) -> Self {
        Self {
            sender,
            error_flag,
            row_count: 0,
        }
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), io::Error> {
        // Fast-fail: check if LanceWriter already errored
        if self.error_flag.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "LanceWriter failed",
            ));
        }
        self.row_count += batch.num_rows();
        self.sender
            .send(batch.clone())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "lance channel closed"))
    }

    pub fn finalize(self) -> Result<usize, io::Error> {
        Ok(self.row_count) // LanceWriter manages lifecycle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn is_valid_parquet_missing_file() {
        let dir = TempDir::new().unwrap();
        assert!(!is_valid_parquet(&dir.path().join("nope.parquet")));
    }

    #[test]
    fn is_valid_parquet_not_parquet() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.parquet");
        std::fs::write(&path, b"this is not parquet").unwrap();
        assert!(!is_valid_parquet(&path));
    }

    #[test]
    fn is_valid_parquet_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.parquet");
        std::fs::write(&path, b"").unwrap();
        assert!(!is_valid_parquet(&path));
    }

    #[test]
    fn is_valid_parquet_real_file() {
        let dir = TempDir::new().unwrap();
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let batch = arrow::array::RecordBatch::try_new(
            std::sync::Arc::new(schema.clone()),
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();

        let path = dir.path().join("valid.parquet");
        let file = File::create(&path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, std::sync::Arc::new(schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(is_valid_parquet(&path));
    }

    #[test]
    fn cleanup_tmp_files_removes_only_tmp() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.tmp"), b"stale").unwrap();
        std::fs::write(dir.path().join("b.parquet"), b"keep").unwrap();
        std::fs::write(dir.path().join("c.tmp"), b"stale2").unwrap();

        cleanup_tmp_files(dir.path()).unwrap();

        assert!(!dir.path().join("a.tmp").exists());
        assert!(dir.path().join("b.parquet").exists());
        assert!(!dir.path().join("c.tmp").exists());
    }
}
