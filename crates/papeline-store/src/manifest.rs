//! Stage manifest: records input hash, content hashes, and metadata

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hash;
use crate::stage::StageName;

/// Manifest stored alongside stage output files.
/// Records how the output was produced and its content hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageManifest {
    /// Which stage produced this output.
    pub stage: StageName,
    /// Blake3 hash of the content-affecting config (input hash).
    pub input_hash: String,
    /// Config JSON that was hashed (for auditability).
    pub config_json: String,
    /// Per-file blake3 content hashes (filename → full hex hash).
    pub file_hashes: BTreeMap<String, String>,
    /// Combined content hash of all output files.
    pub content_hash: String,
    /// When this stage was executed.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl StageManifest {
    /// Compute content hashes for all files in a directory.
    /// Returns (file_hashes, combined_content_hash).
    pub fn compute_content_hashes(dir: &Path) -> Result<(BTreeMap<String, String>, blake3::Hash)> {
        let pattern = dir.join("*");
        let pattern_str = pattern.to_string_lossy();

        let mut entries: Vec<_> = glob::glob(&pattern_str)
            .context("invalid glob pattern")?
            .filter_map(|e| e.ok())
            .filter(|p| p.is_file() && p.file_name().is_none_or(|n| n != "manifest.json"))
            .collect();

        // Sort for deterministic hash order
        entries.sort();

        let mut file_hashes = BTreeMap::new();
        let mut all_hashes = Vec::new();

        for path in &entries {
            let h = hash::hash_file(path)
                .with_context(|| format!("failed to hash {}", path.display()))?;
            let filename = path
                .file_name()
                .expect("glob entry has filename")
                .to_string_lossy()
                .into_owned();
            file_hashes.insert(filename, h.to_hex().to_string());
            all_hashes.push(h);
        }

        let content_hash = if all_hashes.is_empty() {
            hash::hash_bytes(b"empty")
        } else {
            hash::combine_hashes(&all_hashes)
        };

        Ok((file_hashes, content_hash))
    }

    /// Compute content hashes recursively (for S2 which has subdirectories).
    pub fn compute_content_hashes_recursive(
        dir: &Path,
    ) -> Result<(BTreeMap<String, String>, blake3::Hash)> {
        let pattern = dir.join("**/*");
        let pattern_str = pattern.to_string_lossy();

        let mut entries: Vec<_> = glob::glob(&pattern_str)
            .context("invalid glob pattern")?
            .filter_map(|e| e.ok())
            .filter(|p| p.is_file() && p.file_name().is_none_or(|n| n != "manifest.json"))
            .collect();

        entries.sort();

        let mut file_hashes = BTreeMap::new();
        let mut all_hashes = Vec::new();

        for path in &entries {
            let h = hash::hash_file(path)
                .with_context(|| format!("failed to hash {}", path.display()))?;
            // Use relative path from dir as key
            let rel = path
                .strip_prefix(dir)
                .unwrap_or(path)
                .to_string_lossy()
                .into_owned();
            file_hashes.insert(rel, h.to_hex().to_string());
            all_hashes.push(h);
        }

        let content_hash = if all_hashes.is_empty() {
            hash::hash_bytes(b"empty")
        } else {
            hash::combine_hashes(&all_hashes)
        };

        Ok((file_hashes, content_hash))
    }

    /// Write manifest to dir/manifest.json
    pub fn write_to(&self, dir: &Path) -> Result<()> {
        let path = dir.join("manifest.json");
        let json = serde_json::to_string_pretty(self).context("failed to serialize manifest")?;
        std::fs::write(&path, json)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    /// Read manifest from dir/manifest.json
    pub fn read_from(dir: &Path) -> Result<Self> {
        let path = dir.join("manifest.json");
        let json = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let manifest: Self =
            serde_json::from_str(&json).with_context(|| "failed to parse manifest.json")?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_content_hashes_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let (hashes, _combined) = StageManifest::compute_content_hashes(dir.path()).unwrap();
        assert!(hashes.is_empty());
    }

    #[test]
    fn compute_content_hashes_with_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.parquet"), b"data_a").unwrap();
        std::fs::write(dir.path().join("b.parquet"), b"data_b").unwrap();

        let (hashes, _) = StageManifest::compute_content_hashes(dir.path()).unwrap();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains_key("a.parquet"));
        assert!(hashes.contains_key("b.parquet"));
    }

    #[test]
    fn manifest_excludes_itself() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("data.parquet"), b"data").unwrap();
        std::fs::write(dir.path().join("manifest.json"), b"{}").unwrap();

        let (hashes, _) = StageManifest::compute_content_hashes(dir.path()).unwrap();
        assert_eq!(hashes.len(), 1);
        assert!(!hashes.contains_key("manifest.json"));
    }

    #[test]
    fn manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        let manifest = StageManifest {
            stage: crate::stage::StageName::Pubmed,
            input_hash: "abcd1234".into(),
            config_json: r#"{"base_url":"x"}"#.into(),
            file_hashes: BTreeMap::from([("a.parquet".into(), "hash_a".into())]),
            content_hash: "deadbeef".into(),
            created_at: chrono::Utc::now(),
        };

        manifest.write_to(dir.path()).unwrap();
        let loaded = StageManifest::read_from(dir.path()).unwrap();
        assert_eq!(loaded.stage, manifest.stage);
        assert_eq!(loaded.input_hash, manifest.input_hash);
        assert_eq!(loaded.content_hash, manifest.content_hash);
    }
}
