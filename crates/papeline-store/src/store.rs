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
            Ok(manifest) => {
                if manifest.format_version != crate::manifest::CURRENT_FORMAT_VERSION {
                    log::warn!(
                        "cache {short}: format_version {} != current {}, invalidating",
                        manifest.format_version,
                        crate::manifest::CURRENT_FORMAT_VERSION,
                    );
                    LookupResult::NeedsRun
                } else {
                    LookupResult::Cached {
                        path: dir,
                        manifest,
                    }
                }
            }
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
            format_version: crate::manifest::CURRENT_FORMAT_VERSION,
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
            baseline_year: 26,
            seq_end: 1334,
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

    fn commit_input(store: &Store, input: &StageInput, data: &[u8]) -> StageManifest {
        let tmp = store.stage_tmp_dir(input);
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("data.parquet"), data).unwrap();
        store.commit_stage(input, &tmp, false).unwrap()
    }

    fn make_input(base_url: &str) -> StageInput {
        let cfg = PubmedInput {
            base_url: base_url.into(),
            baseline_year: 26,
            seq_end: 1334,
            max_files: Some(5),
            zstd_level: 3,
        };
        make_stage_input(StageName::Pubmed, &cfg)
    }

    fn make_oa_input() -> StageInput {
        use crate::stage::OpenAlexInput;
        let cfg = OpenAlexInput {
            entity: "works".into(),
            since: None,
            max_shards: None,
            zstd_level: 3,
        };
        make_stage_input(StageName::Openalex, &cfg)
    }

    fn make_s2_input_for_test() -> StageInput {
        use crate::stage::S2Input;
        let cfg = S2Input {
            release_id: "2025-01-01".into(),
            datasets: vec!["papers".into()],
            domains: vec!["Biology".into()],
            max_shards: None,
            zstd_level: 3,
        };
        make_stage_input(StageName::S2, &cfg)
    }

    #[test]
    fn create_run_produces_symlinks_and_run_json() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input = test_input();
        let manifest = commit_input(&store, &input, b"pm_data");

        let run_dir = store
            .create_run(&[(StageName::Pubmed, &input, &manifest, false)])
            .unwrap();

        // run.json exists
        let run_json = run_dir.join("run.json");
        assert!(run_json.exists());
        let meta: RunMeta = serde_json::from_str(&fs::read_to_string(&run_json).unwrap()).unwrap();
        assert_eq!(meta.stages.len(), 1);
        assert!(meta.stages.contains_key("pubmed"));
        assert!(!meta.stages["pubmed"].cached);

        // Symlink exists and resolves
        let link = run_dir.join("pubmed");
        assert!(link.is_symlink());
        assert!(fs::read_dir(&link).is_ok());
    }

    #[test]
    fn create_run_updates_latest_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input = test_input();
        let manifest = commit_input(&store, &input, b"data");

        let run_dir = store
            .create_run(&[(StageName::Pubmed, &input, &manifest, true)])
            .unwrap();

        let latest = dir.path().join("latest");
        assert!(latest.is_symlink());
        // latest should resolve to the same run
        let latest_resolved = fs::canonicalize(&latest).unwrap();
        let run_resolved = fs::canonicalize(&run_dir).unwrap();
        assert_eq!(latest_resolved, run_resolved);
    }

    #[test]
    fn create_run_with_multiple_stages() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let pm_input = test_input();
        let pm_manifest = commit_input(&store, &pm_input, b"pm");

        let oa_input = make_oa_input();
        let oa_manifest = commit_input(&store, &oa_input, b"oa");

        let run_dir = store
            .create_run(&[
                (StageName::Pubmed, &pm_input, &pm_manifest, true),
                (StageName::Openalex, &oa_input, &oa_manifest, false),
            ])
            .unwrap();

        assert!(run_dir.join("pubmed").is_symlink());
        assert!(run_dir.join("openalex").is_symlink());

        let meta: RunMeta =
            serde_json::from_str(&fs::read_to_string(run_dir.join("run.json")).unwrap()).unwrap();
        assert_eq!(meta.stages.len(), 2);
        assert!(meta.stages["pubmed"].cached);
        assert!(!meta.stages["openalex"].cached);
    }

    #[test]
    fn list_returns_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input1 = make_input("https://a.com");
        let manifest1 = commit_input(&store, &input1, b"data_a");

        let input2 = make_input("https://b.com");
        let _manifest2 = commit_input(&store, &input2, b"data_b");

        // Create a run referencing only input1
        store
            .create_run(&[(StageName::Pubmed, &input1, &manifest1, false)])
            .unwrap();

        let entries = store.list().unwrap();
        assert_eq!(entries.len(), 2);

        let ref_entry = entries
            .iter()
            .find(|e| e.input_hash == input1.short_hash())
            .unwrap();
        assert!(ref_entry.referenced);

        let unref_entry = entries
            .iter()
            .find(|e| e.input_hash == input2.short_hash())
            .unwrap();
        assert!(!unref_entry.referenced);

        // Both should have correct file count
        for e in &entries {
            assert_eq!(e.file_count, 1);
        }
    }

    #[test]
    fn list_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        let entries = store.list().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn list_skips_tmp_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        // Create a .tmp dir
        let tmp = store.store_dir().join("abcd1234.tmp");
        fs::create_dir_all(&tmp).unwrap();

        let entries = store.list().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn gc_preserves_referenced() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input = test_input();
        let manifest = commit_input(&store, &input, b"keep");

        // Create run → now it's referenced
        store
            .create_run(&[(StageName::Pubmed, &input, &manifest, false)])
            .unwrap();

        let removed = store.gc().unwrap();
        assert!(removed.is_empty());
        assert!(store.stage_dir(&input).exists());
    }

    #[test]
    fn gc_cleans_tmp_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let tmp = store.store_dir().join("deadbeef.tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("junk"), b"stale").unwrap();

        let removed = store.gc().unwrap();
        assert_eq!(removed.len(), 1);
        assert!(removed[0].ends_with(".tmp"));
        assert!(!tmp.exists());
    }

    #[test]
    fn verify_all_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input1 = make_input("https://a.com");
        commit_input(&store, &input1, b"ok_data");

        let input2 = make_input("https://b.com");
        commit_input(&store, &input2, b"will_corrupt");

        // Corrupt second entry
        let corrupt_path = store.stage_dir(&input2).join("data.parquet");
        fs::write(&corrupt_path, b"bad").unwrap();

        let all = store.verify_all().unwrap();
        assert_eq!(all.len(), 2);

        let r1 = &all[&input1.short_hash()];
        assert!(r1.iter().all(|r| r.ok));

        let r2 = &all[&input2.short_hash()];
        assert!(r2.iter().any(|r| !r.ok));
    }

    #[test]
    fn verify_all_skips_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let tmp = store.store_dir().join("deadbeef.tmp");
        fs::create_dir_all(&tmp).unwrap();

        let all = store.verify_all().unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn verify_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input = test_input();
        commit_input(&store, &input, b"data");

        // Delete the data file but keep manifest
        let data_path = store.stage_dir(&input).join("data.parquet");
        fs::remove_file(&data_path).unwrap();

        let results = store.verify(&input.short_hash()).unwrap();
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(results[0].actual, "MISSING");
    }

    #[test]
    fn cleanup_tmp_removes_stale() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        fs::create_dir_all(store.store_dir().join("aaa.tmp")).unwrap();
        fs::create_dir_all(store.store_dir().join("bbb.tmp")).unwrap();

        let count = store.cleanup_tmp().unwrap();
        assert_eq!(count, 2);
        assert!(!store.store_dir().join("aaa.tmp").exists());
        assert!(!store.store_dir().join("bbb.tmp").exists());
    }

    #[test]
    fn cleanup_tmp_ignores_real_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input = test_input();
        commit_input(&store, &input, b"real");

        let count = store.cleanup_tmp().unwrap();
        assert_eq!(count, 0);
        assert!(store.stage_dir(&input).exists());
    }

    #[test]
    fn commit_recursive_s2_structure() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();

        let input = make_s2_input_for_test();
        let tmp = store.stage_tmp_dir(&input);
        let sub = tmp.join("2025-01-01");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("papers_0000.parquet"), b"papers").unwrap();
        fs::write(sub.join("corpus_ids.bin"), b"ids").unwrap();

        let manifest = store.commit_stage(&input, &tmp, true).unwrap();
        assert_eq!(manifest.file_hashes.len(), 2);
        assert!(
            manifest
                .file_hashes
                .contains_key("2025-01-01/papers_0000.parquet")
        );
        assert!(
            manifest
                .file_hashes
                .contains_key("2025-01-01/corpus_ids.bin")
        );
    }

    #[test]
    fn commit_duplicate_hash_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path()).unwrap();
        let input = test_input();

        // First commit
        let tmp1 = store.stage_tmp_dir(&input);
        fs::create_dir_all(&tmp1).unwrap();
        fs::write(tmp1.join("data.parquet"), b"data").unwrap();
        store.commit_stage(&input, &tmp1, false).unwrap();

        // Second commit with same input hash (simulates concurrent)
        let tmp2 = store
            .store_dir()
            .join(format!("{}.tmp", input.short_hash()));
        fs::create_dir_all(&tmp2).unwrap();
        fs::write(tmp2.join("data.parquet"), b"data").unwrap();
        let manifest2 = store.commit_stage(&input, &tmp2, false).unwrap();

        // tmp should be cleaned up, final dir should still exist
        assert!(!tmp2.exists());
        assert!(store.stage_dir(&input).exists());
        assert!(!manifest2.content_hash.is_empty());
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
