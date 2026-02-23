//! RunConfig: parse run.toml and generate stage inputs

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::stage::{
    JoinInput, OpenAlexInput, PubmedInput, S2Input, StageInput, StageName, make_stage_input,
};

/// Top-level run.toml structure.
#[derive(Debug, Clone, Deserialize)]
pub struct RunConfig {
    /// Output directory (store + runs live here).
    #[serde(default = "default_output")]
    pub output: PathBuf,

    /// Default zstd compression level.
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,

    /// PubMed stage config. Present = enabled.
    pub pubmed: Option<PubmedStageConfig>,

    /// OpenAlex stage config. Present = enabled.
    pub openalex: Option<OpenAlexStageConfig>,

    /// S2 stage config. Present = enabled.
    pub s2: Option<S2StageConfig>,

    /// Join stage config (override). Defaults auto-populated.
    pub join: Option<JoinStageConfig>,
}

fn default_output() -> PathBuf {
    PathBuf::from("./data")
}

fn default_zstd_level() -> i32 {
    3
}

#[derive(Debug, Clone, Deserialize)]
pub struct PubmedStageConfig {
    pub base_url: Option<String>,
    pub limit: Option<usize>,
    pub zstd_level: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAlexStageConfig {
    pub entity: Option<String>,
    pub since: Option<String>,
    pub limit: Option<usize>,
    pub zstd_level: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S2StageConfig {
    pub domains: Vec<String>,
    pub release: Option<String>,
    pub datasets: Option<Vec<String>>,
    pub limit: Option<usize>,
    pub zstd_level: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JoinStageConfig {
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
}

fn default_memory_limit() -> String {
    "8GB".into()
}

/// Defaults from papeline.toml (the global config).
pub struct Defaults {
    pub pubmed_base_url: String,
    pub openalex_base_url: String,
    pub s2_api_url: String,
    pub zstd_level: i32,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            pubmed_base_url: "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/".into(),
            openalex_base_url: "https://openalex.s3.amazonaws.com/".into(),
            s2_api_url: "https://api.semanticscholar.org/datasets/v1/".into(),
            zstd_level: 3,
        }
    }
}

impl RunConfig {
    /// Parse run.toml from a file path.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read run config: {}", path.display()))?;
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse run config: {}", path.display()))?;
        Ok(config)
    }

    /// Which fetch stages are active (section present in run.toml).
    pub fn active_fetch_stages(&self) -> Vec<StageName> {
        let mut stages = Vec::new();
        if self.pubmed.is_some() {
            stages.push(StageName::Pubmed);
        }
        if self.openalex.is_some() {
            stages.push(StageName::Openalex);
        }
        if self.s2.is_some() {
            stages.push(StageName::S2);
        }
        stages
    }

    /// Whether join should run.
    /// Auto-enabled when all 3 sources present, or explicitly via [join] section.
    pub fn should_join(&self) -> bool {
        if self.join.is_some() {
            return true;
        }
        // Auto-join when all 3 sources are present
        self.pubmed.is_some() && self.openalex.is_some() && self.s2.is_some()
    }

    /// Validate: if [join] is explicit but sources are missing, error.
    pub fn validate(&self) -> Result<()> {
        if self.join.is_some() {
            let missing: Vec<&str> = [
                (self.pubmed.is_none(), "pubmed"),
                (self.openalex.is_none(), "openalex"),
                (self.s2.is_none(), "s2"),
            ]
            .iter()
            .filter(|(is_missing, _)| *is_missing)
            .map(|(_, name)| *name)
            .collect();

            if !missing.is_empty() {
                anyhow::bail!(
                    "[join] requires all 3 sources, missing: {}",
                    missing.join(", ")
                );
            }
        }
        Ok(())
    }

    /// Get effective zstd level for a stage.
    fn effective_zstd(&self, stage_level: Option<i32>) -> i32 {
        stage_level.unwrap_or(self.zstd_level)
    }

    /// Build StageInput for PubMed.
    pub fn pubmed_input(&self, defaults: &Defaults) -> Option<StageInput> {
        let cfg = self.pubmed.as_ref()?;
        let input = PubmedInput {
            base_url: cfg
                .base_url
                .clone()
                .unwrap_or_else(|| defaults.pubmed_base_url.clone()),
            max_files: cfg.limit,
            zstd_level: self.effective_zstd(cfg.zstd_level),
        };
        Some(make_stage_input(StageName::Pubmed, &input))
    }

    /// Build StageInput for OpenAlex.
    pub fn openalex_input(&self, _defaults: &Defaults) -> Option<StageInput> {
        let cfg = self.openalex.as_ref()?;
        let input = OpenAlexInput {
            entity: cfg.entity.clone().unwrap_or_else(|| "works".into()),
            since: cfg.since.clone(),
            max_shards: cfg.limit,
            zstd_level: self.effective_zstd(cfg.zstd_level),
        };
        Some(make_stage_input(StageName::Openalex, &input))
    }

    /// Build StageInput for S2.
    /// `resolved_release_id` must be the actual ID (not "latest").
    pub fn s2_input(&self, resolved_release_id: &str) -> Option<StageInput> {
        let cfg = self.s2.as_ref()?;
        let mut datasets = cfg.datasets.clone().unwrap_or_else(|| {
            vec![
                "papers".into(),
                "abstracts".into(),
                "tldrs".into(),
                "citations".into(),
            ]
        });
        datasets.sort();
        let mut domains = cfg.domains.clone();
        domains.sort();

        let input = S2Input {
            release_id: resolved_release_id.to_string(),
            datasets,
            domains,
            max_shards: cfg.limit,
            zstd_level: self.effective_zstd(cfg.zstd_level),
        };
        Some(make_stage_input(StageName::S2, &input))
    }

    /// Build StageInput for Join.
    /// Content hashes from upstream stages.
    pub fn join_input(
        &self,
        pm_content_hash: &str,
        oa_content_hash: &str,
        s2_content_hash: &str,
    ) -> Option<StageInput> {
        if !self.should_join() {
            return None;
        }
        let input = JoinInput {
            pm_content_hash: pm_content_hash.to_string(),
            oa_content_hash: oa_content_hash.to_string(),
            s2_content_hash: s2_content_hash.to_string(),
        };
        Some(make_stage_input(StageName::Join, &input))
    }

    /// Get the join memory limit.
    pub fn join_memory_limit(&self) -> String {
        self.join
            .as_ref()
            .map(|j| j.memory_limit.clone())
            .unwrap_or_else(default_memory_limit)
    }

    /// Get the S2 release string from run.toml (may be "latest").
    pub fn s2_release(&self) -> Option<String> {
        self.s2
            .as_ref()
            .map(|cfg| cfg.release.clone().unwrap_or_else(|| "latest".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_run_config() {
        let toml = r#"
output = "./out"

[pubmed]

[openalex]

[s2]
domains = ["Biology"]
"#;
        let config: RunConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.output, PathBuf::from("./out"));
        assert!(config.pubmed.is_some());
        assert!(config.openalex.is_some());
        assert!(config.s2.is_some());
        assert!(config.should_join()); // all 3 present → auto-join
    }

    #[test]
    fn parse_partial_config() {
        let toml = r#"
[pubmed]
limit = 5
"#;
        let config: RunConfig = toml::from_str(toml).unwrap();
        assert!(config.pubmed.is_some());
        assert!(config.openalex.is_none());
        assert!(config.s2.is_none());
        assert!(!config.should_join());
    }

    #[test]
    fn validate_join_missing_sources() {
        let toml = r#"
[pubmed]

[join]
memory_limit = "4GB"
"#;
        let config: RunConfig = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("openalex"));
        assert!(err.to_string().contains("s2"));
    }

    #[test]
    fn s2_input_sorts_fields() {
        let toml = r#"
[s2]
domains = ["Medicine", "Biology"]
datasets = ["citations", "papers"]
"#;
        let config: RunConfig = toml::from_str(toml).unwrap();
        let si = config.s2_input("2025-01-01").unwrap();
        // The canonical JSON should have sorted domains and datasets
        assert!(
            si.config_json
                .contains(r#""datasets":["citations","papers"]"#)
        );
        assert!(
            si.config_json
                .contains(r#""domains":["Biology","Medicine"]"#)
        );
    }

    #[test]
    fn effective_zstd_fallback() {
        let toml = r#"
zstd_level = 5

[pubmed]
zstd_level = 10
"#;
        let config: RunConfig = toml::from_str(toml).unwrap();
        let defaults = Defaults::default();
        let si = config.pubmed_input(&defaults).unwrap();
        // Stage-level override takes precedence
        assert!(si.config_json.contains(r#""zstd_level":10"#));
    }
}
