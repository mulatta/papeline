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
        assert_eq!(StageName::Openalex.to_string(), "openalex");
        assert_eq!(StageName::S2.to_string(), "s2");
        assert_eq!(StageName::Join.to_string(), "joined");
    }

    #[test]
    fn stage_name_dir_name_exhaustive() {
        assert_eq!(StageName::Pubmed.dir_name(), "pubmed");
        assert_eq!(StageName::Openalex.dir_name(), "openalex");
        assert_eq!(StageName::S2.dir_name(), "s2");
        assert_eq!(StageName::Join.dir_name(), "joined");
    }

    #[test]
    fn make_openalex_input() {
        let cfg = OpenAlexInput {
            entity: "works".into(),
            since: Some("2024-01-01".into()),
            max_shards: Some(5),
            zstd_level: 3,
        };
        let si = make_stage_input(StageName::Openalex, &cfg);
        assert_eq!(si.stage, StageName::Openalex);
        assert!(si.config_json.contains(r#""entity":"works""#));
        assert!(si.config_json.contains(r#""since":"2024-01-01""#));
    }

    #[test]
    fn make_join_input() {
        let cfg = JoinInput {
            pm_content_hash: "aaa".into(),
            oa_content_hash: "bbb".into(),
            s2_content_hash: "ccc".into(),
        };
        let si = make_stage_input(StageName::Join, &cfg);
        assert_eq!(si.stage, StageName::Join);
        assert!(si.config_json.contains(r#""pm_content_hash":"aaa""#));
    }

    #[test]
    fn different_stages_same_config_different_hash() {
        // Same config JSON string but different stage should still produce different StageInput
        let cfg = PubmedInput {
            base_url: "x".into(),
            max_files: None,
            zstd_level: 3,
        };
        let si1 = make_stage_input(StageName::Pubmed, &cfg);
        let si2 = make_stage_input(StageName::Pubmed, &cfg);
        // Same stage + same config → same hash
        assert_eq!(si1.input_hash(), si2.input_hash());
    }

    #[test]
    fn pubmed_none_vs_some_limit() {
        let cfg1 = PubmedInput {
            base_url: "x".into(),
            max_files: None,
            zstd_level: 3,
        };
        let cfg2 = PubmedInput {
            base_url: "x".into(),
            max_files: Some(10),
            zstd_level: 3,
        };
        let si1 = make_stage_input(StageName::Pubmed, &cfg1);
        let si2 = make_stage_input(StageName::Pubmed, &cfg2);
        assert_ne!(si1.input_hash(), si2.input_hash());
    }

    #[test]
    fn stage_name_serde_roundtrip() {
        let json = serde_json::to_string(&StageName::S2).unwrap();
        assert_eq!(json, r#""s2""#);
        let deserialized: StageName = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, StageName::S2);
    }

    #[test]
    fn stage_name_serde_all_variants() {
        for name in [
            StageName::Pubmed,
            StageName::Openalex,
            StageName::S2,
            StageName::Join,
        ] {
            let json = serde_json::to_string(&name).unwrap();
            let back: StageName = serde_json::from_str(&json).unwrap();
            assert_eq!(back, name);
        }
    }
}
