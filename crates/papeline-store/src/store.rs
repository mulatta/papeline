//! Content-addressable store for pipeline stage outputs
//!
//! Directory layout:
//! ```text
//! {base}/
//! ├── store/
//! │   └── {input-hash}/     # stage output (8 char hex)
//! │       ├── manifest.json
//! │       └── *.parquet
//! ├── runs/
//! │   └── {run-hash}/       # per-run symlinks
//! │       ├── run.json
//! │       ├── pubmed -> ../../store/{hash}
//! │       └── ...
//! └── latest -> runs/{run-hash}
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hash;
use crate::manifest::StageManifest;
use crate::stage::{StageInput, StageName};

/// Result of looking up a stage in the store.
#[derive(Debug)]
pub enum LookupResult {
    /// Found a valid cached result.
    Cached {
        path: PathBuf,
        manifest: StageManifest,
    },
    /// No cached result; needs execution.
    NeedsRun,
}

/// Summary of a store entry for listing.
#[derive(Debug, Serialize)]
pub struct StoreEntry {
    pub input_hash: String,
    pub stage: StageName,
    pub file_count: usize,
    pub content_hash: String,
    pub created_at: String,
    pub referenced: bool,
}

/// Verification result for a single file.
#[derive(Debug)]
pub struct VerifyResult {
    pub path: String,
    pub expected: String,
    pub actual: String,
    pub ok: bool,
}

/// Run metadata stored in runs/{run-hash}/run.json
#[derive(Debug, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_hash: String,
    pub stages: BTreeMap<String, StageMeta>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StageMeta {
    pub input_hash: String,
    pub content_hash: String,
    pub cached: bool,
}

/// Content-addressable store.
pub struct Store {
    base: PathBuf,
}

impl Store {
    /// Create a new store rooted at `base`.
    pub fn new(base: &Path) -> Result<Self> {
        let store_dir = base.join("store");
        let runs_dir = base.join("runs");
        fs::create_dir_all(&store_dir)
            .with_context(|| format!("failed to create store dir: {}", store_dir.display()))?;
        fs::create_dir_all(&runs_dir)
            .with_context(|| format!("failed to create runs dir: {}", runs_dir.display()))?;
        Ok(Self {
            base: base.to_path_buf(),
        })
    }

    pub fn store_dir(&self) -> PathBuf {
        self.base.join("store")
    }

    pub fn runs_dir(&self) -> PathBuf {
        self.base.join("runs")
    }

    /// Look up a cached stage output by input hash.
    pub fn lookup(&self, input: &StageInput) -> LookupResult {
        let short = input.short_hash();
        let dir = self.store_dir().join(&short);

        if !dir.exists() {
            return LookupResult::NeedsRun;
        }

        match StageManifest::read_from(&dir) {
            Ok(manifest) => LookupResult::Cached {
                path: dir,
                manifest,
            },
            Err(e) => {
                log::warn!("corrupt cache entry {short}: {e}");
                LookupResult::NeedsRun
            }
        }
    }

    /// Get the staging (tmp) directory for a stage output.
    pub fn stage_tmp_dir(&self, input: &StageInput) -> PathBuf {
        let short = input.short_hash();
        self.store_dir().join(format!("{short}.tmp"))
    }

    /// Get the final directory for a stage output.
    pub fn stage_dir(&self, input: &StageInput) -> PathBuf {
        let short = input.short_hash();
        self.store_dir().join(&short)
    }

    /// Commit a completed stage: compute content hash, write manifest, atomic rename.
    ///
    /// `tmp_dir` is the `.tmp` staging directory.
    /// `input` is the stage input (for hash + metadata).
    /// `recursive` controls whether to hash files recursively (for S2).
    ///
    /// Returns the manifest.
    pub fn commit_stage(
        &self,
        input: &StageInput,
        tmp_dir: &Path,
        recursive: bool,
    ) -> Result<StageManifest> {
        let (file_hashes, content_hash) = if recursive {
            StageManifest::compute_content_hashes_recursive(tmp_dir)?
        } else {
            StageManifest::compute_content_hashes(tmp_dir)?
        };

        let manifest = StageManifest {
            stage: input.stage,
            input_hash: input.short_hash(),
            config_json: input.config_json.clone(),
            file_hashes,
            content_hash: content_hash.to_hex().to_string(),
            created_at: chrono::Utc::now(),
        };

        manifest.write_to(tmp_dir)?;

        let final_dir = self.stage_dir(input);
        if final_dir.exists() {
            // Content-addressable: same hash = same content, just clean tmp
            log::info!(
                "store: {} already exists (concurrent run), removing tmp",
                final_dir.display()
            );
            fs::remove_dir_all(tmp_dir)
                .with_context(|| format!("failed to remove tmp {}", tmp_dir.display()))?;
        } else {
            fs::rename(tmp_dir, &final_dir).with_context(|| {
                format!(
                    "failed to rename {} → {}",
                    tmp_dir.display(),
                    final_dir.display()
                )
            })?;
        }

        Ok(manifest)
    }

    /// Create a run entry with symlinks to stage outputs.
    pub fn create_run(
        &self,
        stages: &[(StageName, &StageInput, &StageManifest, bool)],
    ) -> Result<PathBuf> {
        // Run hash = hash of all input hashes
        let combined = hash::combine_hashes(
            &stages
                .iter()
                .map(|(_, input, _, _)| input.input_hash())
                .collect::<Vec<_>>(),
        );
        let run_hash = hash::short_hash(&combined);

        let run_dir = self.runs_dir().join(&run_hash);
        if run_dir.exists() {
            fs::remove_dir_all(&run_dir)?;
        }
        fs::create_dir_all(&run_dir)?;

        // Create symlinks
        for (name, input, _, _) in stages {
            let target = PathBuf::from("../../store").join(input.short_hash());
            let link = run_dir.join(name.dir_name());
            std::os::unix::fs::symlink(&target, &link).with_context(|| {
                format!(
                    "failed to create symlink {} → {}",
                    link.display(),
                    target.display()
                )
            })?;
        }

        // Write run.json
        let mut stage_meta = BTreeMap::new();
        for (name, input, manifest, cached) in stages {
            stage_meta.insert(
                name.dir_name().to_string(),
                StageMeta {
                    input_hash: input.short_hash(),
                    content_hash: manifest.content_hash.clone(),
                    cached: *cached,
                },
            );
        }

        let run_meta = RunMeta {
            run_hash: run_hash.clone(),
            stages: stage_meta,
            created_at: chrono::Utc::now(),
        };

        let json = serde_json::to_string_pretty(&run_meta)?;
        fs::write(run_dir.join("run.json"), json)?;

        // Update latest symlink
        let latest = self.base.join("latest");
        let _ = fs::remove_file(&latest);
        let target = PathBuf::from("runs").join(&run_hash);
        std::os::unix::fs::symlink(&target, &latest).ok();

        Ok(run_dir)
    }

    /// List all store entries.
    pub fn list(&self) -> Result<Vec<StoreEntry>> {
        let store_dir = self.store_dir();
        let referenced = self.referenced_hashes()?;

        let mut entries = Vec::new();
        for entry in fs::read_dir(&store_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            // Skip .tmp dirs
            if name.ends_with(".tmp") {
                continue;
            }

            match StageManifest::read_from(&path) {
                Ok(manifest) => {
                    entries.push(StoreEntry {
                        input_hash: name.clone(),
                        stage: manifest.stage,
                        file_count: manifest.file_hashes.len(),
                        content_hash: manifest.content_hash[..8].to_string(),
                        created_at: manifest.created_at.format("%Y-%m-%d %H:%M").to_string(),
                        referenced: referenced.contains(&name),
                    });
                }
                Err(e) => {
                    log::warn!("skipping {name}: {e}");
                }
            }
        }

        entries.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(entries)
    }

    /// Collect all input hashes referenced by any run.
    fn referenced_hashes(&self) -> Result<std::collections::HashSet<String>> {
        let mut refs = std::collections::HashSet::new();
        let runs_dir = self.runs_dir();

        if !runs_dir.exists() {
            return Ok(refs);
        }

        for entry in fs::read_dir(&runs_dir)? {
            let entry = entry?;
            let run_json = entry.path().join("run.json");
            if let Ok(content) = fs::read_to_string(&run_json) {
                if let Ok(meta) = serde_json::from_str::<RunMeta>(&content) {
                    for stage in meta.stages.values() {
                        refs.insert(stage.input_hash.clone());
                    }
                }
            }
        }

        Ok(refs)
    }

    /// Garbage collect unreferenced store entries.
    /// Returns list of removed hashes.
    pub fn gc(&self) -> Result<Vec<String>> {
        let referenced = self.referenced_hashes()?;
        let store_dir = self.store_dir();
        let mut removed = Vec::new();

        for entry in fs::read_dir(&store_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            // Always clean .tmp dirs
            if name.ends_with(".tmp") {
                log::info!("gc: removing stale tmp {name}");
                fs::remove_dir_all(&path)?;
                removed.push(name);
                continue;
            }

            if !referenced.contains(&name) {
                log::info!("gc: removing unreferenced {name}");
                fs::remove_dir_all(&path)?;
                removed.push(name);
            }
        }

        Ok(removed)
    }

    /// Verify content hashes for a store entry.
    pub fn verify(&self, input_hash: &str) -> Result<Vec<VerifyResult>> {
        let dir = self.store_dir().join(input_hash);
        let manifest = StageManifest::read_from(&dir)
            .with_context(|| format!("no manifest for {input_hash}"))?;

        let mut results = Vec::new();
        for (filename, expected_hash) in &manifest.file_hashes {
            let file_path = dir.join(filename);
            let (actual, ok) = if file_path.exists() {
                match hash::hash_file(&file_path) {
                    Ok(h) => {
                        let hex = h.to_hex().to_string();
                        let ok = hex == *expected_hash;
                        (hex, ok)
                    }
                    Err(e) => (format!("error: {e}"), false),
                }
            } else {
                ("MISSING".to_string(), false)
            };

            results.push(VerifyResult {
                path: filename.clone(),
                expected: expected_hash.clone(),
                actual,
                ok,
            });
        }

        Ok(results)
    }

    /// Verify all store entries.
    pub fn verify_all(&self) -> Result<BTreeMap<String, Vec<VerifyResult>>> {
        let store_dir = self.store_dir();
        let mut all_results = BTreeMap::new();

        for entry in fs::read_dir(&store_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") || !entry.path().is_dir() {
                continue;
            }
            match self.verify(&name) {
                Ok(results) => {
                    all_results.insert(name, results);
                }
                Err(e) => {
                    log::warn!("verify {name}: {e}");
                }
            }
        }

        Ok(all_results)
    }

    /// Clean stale .tmp directories.
    pub fn cleanup_tmp(&self) -> Result<usize> {
        let store_dir = self.store_dir();
        let mut count = 0;

        for entry in fs::read_dir(&store_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") && entry.path().is_dir() {
                log::info!("cleaning stale tmp: {name}");
                fs::remove_dir_all(entry.path())?;
                count += 1;
            }
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::{PubmedInput, make_stage_input};

    fn test_input() -> StageInput {
        let cfg = PubmedInput {
            base_url: "https://example.com".into(),
            max_files: Some(5),
            zstd_level: 3,
        };
        make_stage_input(StageName::Pubmed, &cfg)
    }

    #[test]
    fn store_new_creates_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        assert!(store.store_dir().exists());
        assert!(store.runs_dir().exists());
    }

    #[test]
    fn lookup_miss() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        let input = test_input();
        assert!(matches!(store.lookup(&input), LookupResult::NeedsRun));
    }

    #[test]
    fn commit_and_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        let input = test_input();

        // Create tmp dir with a file
        let tmp = store.stage_tmp_dir(&input);
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("data.parquet"), b"test data").unwrap();

        // Commit
        let manifest = store.commit_stage(&input, &tmp, false).unwrap();
        assert_eq!(manifest.file_hashes.len(), 1);
        assert!(manifest.file_hashes.contains_key("data.parquet"));

        // Lookup should now hit
        match store.lookup(&input) {
            LookupResult::Cached { manifest: m, .. } => {
                assert_eq!(m.content_hash, manifest.content_hash);
            }
            LookupResult::NeedsRun => panic!("expected cache hit"),
        }
    }

    #[test]
    fn gc_removes_unreferenced() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        let input = test_input();

        // Commit a stage (no run referencing it)
        let tmp = store.stage_tmp_dir(&input);
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("data.parquet"), b"test").unwrap();
        store.commit_stage(&input, &tmp, false).unwrap();

        // GC should remove it
        let removed = store.gc().unwrap();
        assert_eq!(removed.len(), 1);
        assert!(!store.stage_dir(&input).exists());
    }

    #[test]
    fn verify_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        let input = test_input();

        let tmp = store.stage_tmp_dir(&input);
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("data.parquet"), b"original").unwrap();
        store.commit_stage(&input, &tmp, false).unwrap();

        // Corrupt the file
        let data_path = store.stage_dir(&input).join("data.parquet");
        fs::write(&data_path, b"corrupted").unwrap();

        let results = store.verify(&input.short_hash()).unwrap();
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
    }
}
