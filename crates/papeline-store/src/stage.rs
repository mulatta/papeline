//! Stage definitions and input hashing

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::hash;

/// Pipeline stage identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StageName {
    Pubmed,
    Openalex,
    S2,
    Join,
}

impl StageName {
    /// Directory name used in runs/ symlinks.
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Pubmed => "pubmed",
            Self::Openalex => "openalex",
            Self::S2 => "s2",
            Self::Join => "joined",
        }
    }
}

impl fmt::Display for StageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.dir_name())
    }
}

/// Content-affecting configuration for a stage, serialized to compute input hash.
///
/// Fields must be in a fixed order (struct field order via serde).
/// Vec fields are sorted before serialization for determinism.
#[derive(Debug, Clone, Serialize)]
pub struct StageInput {
    pub stage: StageName,
    /// Canonical JSON of content-affecting config fields.
    /// Built differently per stage.
    pub config_json: String,
}

impl StageInput {
    /// Compute the blake3 input hash from the canonical config JSON.
    pub fn input_hash(&self) -> blake3::Hash {
        hash::hash_bytes(self.config_json.as_bytes())
    }

    /// Short (8-char hex) input hash.
    pub fn short_hash(&self) -> String {
        hash::short_hash(&self.input_hash())
    }
}

/// Content-affecting fields for PubMed stage.
#[derive(Debug, Clone, Serialize)]
pub struct PubmedInput {
    pub base_url: String,
    pub max_files: Option<usize>,
    pub zstd_level: i32,
}

/// Content-affecting fields for OpenAlex stage.
#[derive(Debug, Clone, Serialize)]
pub struct OpenAlexInput {
    pub entity: String,
    pub since: Option<String>,
    pub max_shards: Option<usize>,
    pub zstd_level: i32,
}

/// Content-affecting fields for S2 stage.
#[derive(Debug, Clone, Serialize)]
pub struct S2Input {
    pub release_id: String,
    pub datasets: Vec<String>,
    pub domains: Vec<String>,
    pub max_shards: Option<usize>,
    pub zstd_level: i32,
}

/// Content-affecting fields for Join stage.
/// Uses content hashes from upstream stages.
#[derive(Debug, Clone, Serialize)]
pub struct JoinInput {
    pub pm_content_hash: String,
    pub oa_content_hash: String,
    pub s2_content_hash: String,
}

/// Build a StageInput from typed config.
pub fn make_stage_input<T: Serialize>(stage: StageName, config: &T) -> StageInput {
    let config_json =
        serde_json::to_string(config).expect("StageInput config serialization should never fail");
    StageInput { stage, config_json }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_hash_deterministic() {
        let input = PubmedInput {
            base_url: "https://example.com".into(),
            max_files: Some(10),
            zstd_level: 3,
        };
        let si1 = make_stage_input(StageName::Pubmed, &input);
        let si2 = make_stage_input(StageName::Pubmed, &input);
        assert_eq!(si1.input_hash(), si2.input_hash());
    }

    #[test]
    fn input_hash_changes_with_config() {
        let input1 = PubmedInput {
            base_url: "https://a.com".into(),
            max_files: Some(10),
            zstd_level: 3,
        };
        let input2 = PubmedInput {
            base_url: "https://b.com".into(),
            max_files: Some(10),
            zstd_level: 3,
        };
        let si1 = make_stage_input(StageName::Pubmed, &input1);
        let si2 = make_stage_input(StageName::Pubmed, &input2);
        assert_ne!(si1.input_hash(), si2.input_hash());
    }

    #[test]
    fn s2_input_sorted_domains() {
        let mut input = S2Input {
            release_id: "2025-01-01".into(),
            datasets: vec!["papers".into(), "abstracts".into()],
            domains: vec!["Medicine".into(), "Biology".into()],
            max_shards: None,
            zstd_level: 3,
        };
        // Sort for determinism
        input.datasets.sort();
        input.domains.sort();

        let si = make_stage_input(StageName::S2, &input);
        assert_eq!(si.short_hash().len(), 8);
    }

    #[test]
    fn stage_name_display() {
        assert_eq!(StageName::Pubmed.to_string(), "pubmed");
        assert_eq!(StageName::Join.to_string(), "joined");
    }
}
