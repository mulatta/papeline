//! OpenAlex pipeline configuration

use std::path::PathBuf;

use chrono::NaiveDate;

use crate::state::Entity;
use crate::transform::WorkRow;

/// Hierarchical topic filter for OpenAlex works.
///
/// Semantics: Hierarchical OR — a row passes if it matches ANY of:
/// - `primary_topic.domain.display_name` ∈ domains
/// - `primary_topic.field.display_name` ∈ fields
/// - `primary_topic.id` short ID ∈ topics
///
/// Empty filter (all vecs empty) = pass everything (backward compatible).
#[derive(Debug, Clone, Default)]
pub struct TopicFilter {
    pub domains: Vec<String>,
    pub fields: Vec<String>,
    pub topics: Vec<String>,
    /// Pre-computed substrings for fast pre-filtering on raw JSON lines.
    needles: Vec<String>,
}

impl TopicFilter {
    pub fn new(domains: Vec<String>, fields: Vec<String>, topics: Vec<String>) -> Self {
        let mut needles = Vec::new();
        // Domain/field names appear as quoted strings in JSON
        for name in domains.iter().chain(fields.iter()) {
            needles.push(format!("\"{name}\""));
        }
        // Topic short IDs (e.g. "T10978") appear as-is in URLs
        for tid in &topics {
            needles.push(tid.clone());
        }
        Self {
            domains,
            fields,
            topics,
            needles,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.domains.is_empty() && self.fields.is_empty() && self.topics.is_empty()
    }

    /// Fast substring pre-filter on a raw JSON line.
    /// Returns true if the line *might* match (may have false positives).
    pub fn pre_filter(&self, line: &str) -> bool {
        self.needles.iter().any(|n| line.contains(n.as_str()))
    }

    /// Full semantic match on a parsed WorkRow.
    pub fn matches(&self, row: &WorkRow) -> bool {
        if let Some(domain) = row.primary_topic_domain() {
            if self.domains.iter().any(|d| d == domain) {
                return true;
            }
        }
        if let Some(field) = row.primary_topic_field() {
            if self.fields.iter().any(|f| f == field) {
                return true;
            }
        }
        if let Some(tid) = row.primary_topic_short_id() {
            if self.topics.iter().any(|t| t == tid) {
                return true;
            }
        }
        false
    }
}

/// Runtime configuration for OpenAlex pipeline
#[derive(Debug)]
pub struct Config {
    /// Output directory for parquet files
    pub output_dir: PathBuf,
    /// Entity type to fetch
    pub entity: Entity,
    /// Only fetch records updated since this date (inclusive)
    pub since: Option<NaiveDate>,
    /// Maximum shards to process (for testing)
    pub max_shards: Option<usize>,
    /// Zstd compression level for parquet output
    pub zstd_level: i32,
    /// Topic/domain/field filter for works
    pub topic_filter: TopicFilter,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("output"),
            entity: Entity::Works,
            since: None,
            max_shards: None,
            zstd_level: 3,
            topic_filter: TopicFilter::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.output_dir, PathBuf::from("output"));
        assert_eq!(config.entity, Entity::Works);
        assert!(config.since.is_none());
        assert!(config.max_shards.is_none());
        assert_eq!(config.zstd_level, 3);
        assert!(config.topic_filter.is_empty());
    }

    #[test]
    fn topic_filter_empty_passes_all() {
        let filter = TopicFilter::default();
        assert!(filter.is_empty());
    }

    #[test]
    fn topic_filter_pre_filter_domain() {
        let filter = TopicFilter::new(vec!["Health Sciences".into()], vec![], vec![]);
        assert!(filter.pre_filter(r#""domain":{"display_name":"Health Sciences"}"#));
        assert!(!filter.pre_filter(r#""domain":{"display_name":"Physical Sciences"}"#));
    }

    #[test]
    fn topic_filter_pre_filter_topic_id() {
        let filter = TopicFilter::new(vec![], vec![], vec!["T10978".into()]);
        assert!(filter.pre_filter("https://openalex.org/T10978"));
        assert!(!filter.pre_filter("https://openalex.org/T99999"));
    }

    #[test]
    fn topic_filter_matches_domain() {
        let filter = TopicFilter::new(vec!["Physical Sciences".into()], vec![], vec![]);
        let json = r#"{
            "id": "https://openalex.org/W1",
            "primary_topic": {
                "id": "https://openalex.org/T1",
                "display_name": "ML",
                "domain": {"display_name": "Physical Sciences"}
            }
        }"#;
        let row: WorkRow = serde_json::from_str(json).unwrap();
        assert!(filter.matches(&row));
    }

    #[test]
    fn topic_filter_matches_field() {
        let filter = TopicFilter::new(vec![], vec!["Medicine".into()], vec![]);
        let json = r#"{
            "id": "https://openalex.org/W1",
            "primary_topic": {
                "id": "https://openalex.org/T1",
                "display_name": "ML",
                "field": {"display_name": "Medicine"}
            }
        }"#;
        let row: WorkRow = serde_json::from_str(json).unwrap();
        assert!(filter.matches(&row));
    }

    #[test]
    fn topic_filter_matches_topic_id() {
        let filter = TopicFilter::new(vec![], vec![], vec!["T789".into()]);
        let json = r#"{
            "id": "https://openalex.org/W1",
            "primary_topic": {"id": "https://openalex.org/T789", "display_name": "ML"}
        }"#;
        let row: WorkRow = serde_json::from_str(json).unwrap();
        assert!(filter.matches(&row));
    }

    #[test]
    fn topic_filter_no_match() {
        let filter = TopicFilter::new(
            vec!["Life Sciences".into()],
            vec!["Biology".into()],
            vec!["T999".into()],
        );
        let json = r#"{
            "id": "https://openalex.org/W1",
            "primary_topic": {
                "id": "https://openalex.org/T1",
                "display_name": "ML",
                "domain": {"display_name": "Physical Sciences"},
                "field": {"display_name": "Computer Science"}
            }
        }"#;
        let row: WorkRow = serde_json::from_str(json).unwrap();
        assert!(!filter.matches(&row));
    }

    #[test]
    fn topic_filter_no_primary_topic() {
        let filter = TopicFilter::new(vec!["Health Sciences".into()], vec![], vec![]);
        let json = r#"{"id": "https://openalex.org/W1"}"#;
        let row: WorkRow = serde_json::from_str(json).unwrap();
        assert!(!filter.matches(&row));
    }
}
