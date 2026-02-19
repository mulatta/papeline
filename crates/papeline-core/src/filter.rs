//! Domain filtering and corpus ID management

use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;

use memmap2::Mmap;
use rustc_hash::FxHashSet;

/// O(1) corpus ID lookup using `FxHashSet`
#[derive(Debug)]
pub struct CorpusIdSet {
    set: FxHashSet<i64>,
}

impl CorpusIdSet {
    pub fn from_mmap(mmap: &MappedCorpusIds) -> Self {
        Self {
            set: mmap.iter().copied().collect(),
        }
    }

    pub fn contains(&self, id: i64) -> bool {
        self.set.contains(&id)
    }
}

/// Save sorted corpus IDs as raw little-endian i64 bytes.
///
/// Takes `&mut Vec` (not `&[i64]`) to encapsulate sort+dedup invariant — callers
/// need not remember to pre-sort. Only 2 call sites, so the API tradeoff favors safety.
pub fn save_corpus_ids(ids: &mut Vec<i64>, path: &Path) -> std::io::Result<()> {
    ids.sort_unstable();
    ids.dedup();
    let mut writer = std::io::BufWriter::new(File::create(path)?);
    for id in ids.iter() {
        writer.write_all(&id.to_le_bytes())?;
    }
    writer.flush()?;
    log::info!("Saved {} corpus IDs to {}", ids.len(), path.display());
    Ok(())
}

/// Memory-mapped corpus IDs — zero-copy access to sorted i64 array
pub struct MappedCorpusIds {
    mmap: Mmap,
    len: usize,
}

impl std::fmt::Debug for MappedCorpusIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedCorpusIds")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl MappedCorpusIds {
    pub const fn len(&self) -> usize {
        self.len
    }
}

impl Deref for MappedCorpusIds {
    type Target = [i64];

    #[allow(clippy::cast_ptr_alignment)] // mmap is page-aligned (4KB > 8B)
    fn deref(&self) -> &[i64] {
        // SAFETY: mmap is page-aligned (satisfies i64 alignment of 8).
        // File was validated to be a multiple of 8 bytes.
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr().cast::<i64>(), self.len) }
    }
}

/// Load sorted corpus IDs via mmap (zero heap allocation)
pub fn load_corpus_ids(path: &Path) -> Result<MappedCorpusIds, std::io::Error> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let file_len = usize::try_from(metadata.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "corpus_ids.bin too large")
    })?;

    if file_len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "corpus_ids.bin is empty",
        ));
    }
    if !file_len.is_multiple_of(8) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "corpus_ids.bin size not aligned to 8 bytes",
        ));
    }

    // SAFETY: file is read-only, no concurrent writers (we own the pipeline)
    let mmap = unsafe { Mmap::map(&file)? };
    let len = file_len / 8;

    Ok(MappedCorpusIds { mmap, len })
}

/// Recover corpus IDs from completed papers parquet files (crash recovery)
pub fn recover_corpus_ids(output_dir: &Path) -> Vec<i64> {
    let pattern = output_dir.join("papers_*.parquet");
    let pattern_str = pattern.to_string_lossy();

    let mut ids = Vec::new();
    for entry in glob::glob(&pattern_str).expect("invalid glob pattern for parquet recovery") {
        let path = match entry {
            Ok(p) => p,
            Err(_) => continue,
        };
        match read_corpusids_from_parquet(&path) {
            Ok(mut file_ids) => {
                log::debug!(
                    "Recovered {} corpus IDs from {}",
                    file_ids.len(),
                    path.display()
                );
                ids.append(&mut file_ids);
            }
            Err(e) => {
                log::warn!("Failed to read {}: {e}", path.display());
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Read corpusid column from a parquet file
fn read_corpusids_from_parquet(path: &Path) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    use arrow::array::Int64Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Only read the corpusid column
    let schema = builder.schema().clone();
    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "corpusid")
        .ok_or("No corpusid column")?;

    let col_name = schema.field(col_idx).name().as_str();
    let mask = parquet::arrow::ProjectionMask::columns(
        builder.parquet_schema(),
        std::iter::once(col_name),
    );
    let reader = builder.with_projection(mask).build()?;

    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("corpusid column is not Int64")?;
        ids.extend(col.values().iter());
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("corpus_ids.bin");

        let mut ids = vec![30, 10, 20, 10]; // unsorted, with dupe
        save_corpus_ids(&mut ids, &path).unwrap();

        let mmap = load_corpus_ids(&path).unwrap();
        assert_eq!(mmap.len(), 3); // deduped
        assert_eq!(&*mmap, &[10, 20, 30]); // sorted
    }

    #[test]
    fn save_empty_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("corpus_ids.bin");

        let mut ids: Vec<i64> = vec![];
        save_corpus_ids(&mut ids, &path).unwrap();

        // Empty file should fail to load (size 0)
        let err = load_corpus_ids(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn load_missing_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.bin");
        assert!(load_corpus_ids(&path).is_err());
    }

    #[test]
    fn load_misaligned_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.bin");
        // Write 7 bytes (not a multiple of 8)
        std::fs::write(&path, [0u8; 7]).unwrap();
        let err = load_corpus_ids(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn corpus_id_set_contains() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("corpus_ids.bin");

        let mut ids = vec![100, 200, 300];
        save_corpus_ids(&mut ids, &path).unwrap();
        let mmap = load_corpus_ids(&path).unwrap();
        let set = CorpusIdSet::from_mmap(&mmap);

        assert!(set.contains(100));
        assert!(set.contains(200));
        assert!(set.contains(300));
        assert!(!set.contains(150));
        assert!(!set.contains(0));
    }

    #[test]
    fn recover_corpus_ids_empty_dir() {
        let dir = TempDir::new().unwrap();
        let ids = recover_corpus_ids(dir.path());
        assert!(ids.is_empty());
    }

    #[test]
    fn recover_corpus_ids_no_matching_files() {
        let dir = TempDir::new().unwrap();
        // Create a non-matching file
        std::fs::write(dir.path().join("other.parquet"), b"dummy").unwrap();
        let ids = recover_corpus_ids(dir.path());
        assert!(ids.is_empty());
    }

    #[test]
    fn mapped_corpus_ids_debug() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("corpus_ids.bin");
        let mut ids = vec![1, 2, 3];
        save_corpus_ids(&mut ids, &path).unwrap();
        let mmap = load_corpus_ids(&path).unwrap();

        let debug_str = format!("{:?}", mmap);
        assert!(debug_str.contains("MappedCorpusIds"));
        assert!(debug_str.contains("len: 3"));
    }
}
