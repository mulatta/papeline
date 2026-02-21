//! OpenAlex Redshift manifest parsing
//!
//! OpenAlex provides data via S3 with Redshift-style manifest files.
//! Each manifest lists gz-compressed JSON files organized by update date.

use chrono::NaiveDate;
use serde::Deserialize;

/// S3 base URL for OpenAlex public bucket (HTTP access)
const S3_BASE_URL: &str = "https://openalex.s3.amazonaws.com/";

/// Manifest structure for OpenAlex data files
#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub entries: Vec<ManifestEntry>,
}

/// Single entry in the manifest
#[derive(Debug, Deserialize)]
pub struct ManifestEntry {
    /// S3 path (e.g., "s3://openalex/data/works/updated_date=2025-01-01/part_0000.gz")
    pub url: String,
    /// File metadata (optional in some manifests)
    #[serde(default)]
    pub meta: Option<EntryMeta>,
}

/// Metadata for a manifest entry
#[derive(Debug, Deserialize)]
pub struct EntryMeta {
    /// File size in bytes
    pub content_length: u64,
    /// Number of records in the file
    pub record_count: u64,
}

impl Manifest {
    /// Parse manifest from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Filter entries to only include those with updated_date >= since
    pub fn filter_since(&self, since: Option<NaiveDate>) -> Vec<&ManifestEntry> {
        match since {
            Some(date) => self
                .entries
                .iter()
                .filter(|e| e.updated_date().is_some_and(|d| d >= date))
                .collect(),
            None => self.entries.iter().collect(),
        }
    }

    /// Total number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if manifest is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl ManifestEntry {
    /// Convert S3 URL to HTTP URL for direct access
    ///
    /// Transforms "s3://openalex/data/..." to "https://openalex.s3.amazonaws.com/data/..."
    pub fn http_url(&self) -> String {
        if self.url.starts_with("s3://openalex/") {
            format!("{}{}", S3_BASE_URL, &self.url["s3://openalex/".len()..])
        } else {
            // Already HTTP or unknown format, return as-is
            self.url.clone()
        }
    }

    /// Extract updated_date from URL path
    ///
    /// URL format: "s3://openalex/data/works/updated_date=2025-01-01/part_0000.gz"
    pub fn updated_date(&self) -> Option<NaiveDate> {
        // Find "updated_date=" segment and parse the date
        let marker = "updated_date=";
        let start = self.url.find(marker)?;
        let date_start = start + marker.len();
        let date_str = &self.url[date_start..date_start + 10]; // "YYYY-MM-DD" = 10 chars
        NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
    }

    /// Extract part number from URL (e.g., part_0000 -> 0)
    pub fn part_number(&self) -> Option<usize> {
        let marker = "/part_";
        let start = self.url.find(marker)?;
        let num_start = start + marker.len();
        let num_end = self.url[num_start..].find('.')? + num_start;
        self.url[num_start..num_end].parse().ok()
    }
}

/// Fetch manifest from OpenAlex S3
pub fn fetch_manifest(entity: &str) -> anyhow::Result<Manifest> {
    use papeline_core::stream::{SHARED_RUNTIME, http_client};

    let url = format!("{}data/{}/manifest", S3_BASE_URL, entity);

    let json = SHARED_RUNTIME.handle().block_on(async {
        let response = http_client()
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| anyhow::anyhow!("Failed to fetch manifest: {e}"))?;
        response
            .text()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read manifest: {e}"))
    })?;

    Manifest::from_json(&json).map_err(|e| anyhow::anyhow!("Failed to parse manifest: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_MANIFEST: &str = r#"{
        "entries": [
            {
                "url": "s3://openalex/data/works/updated_date=2025-01-15/part_0000.gz",
                "meta": {"content_length": 123456, "record_count": 1000}
            },
            {
                "url": "s3://openalex/data/works/updated_date=2025-01-14/part_0001.gz",
                "meta": {"content_length": 234567, "record_count": 2000}
            }
        ]
    }"#;

    #[test]
    fn parse_manifest() {
        let m = Manifest::from_json(SAMPLE_MANIFEST).unwrap();
        assert_eq!(m.entries.len(), 2);
    }

    #[test]
    fn http_url_conversion() {
        let m = Manifest::from_json(SAMPLE_MANIFEST).unwrap();
        assert_eq!(
            m.entries[0].http_url(),
            "https://openalex.s3.amazonaws.com/data/works/updated_date=2025-01-15/part_0000.gz"
        );
    }

    #[test]
    fn extract_updated_date() {
        let m = Manifest::from_json(SAMPLE_MANIFEST).unwrap();
        assert_eq!(
            m.entries[0].updated_date(),
            Some(NaiveDate::from_ymd_opt(2025, 1, 15).unwrap())
        );
    }

    #[test]
    fn extract_part_number() {
        let m = Manifest::from_json(SAMPLE_MANIFEST).unwrap();
        assert_eq!(m.entries[0].part_number(), Some(0));
        assert_eq!(m.entries[1].part_number(), Some(1));
    }

    #[test]
    fn filter_since() {
        let m = Manifest::from_json(SAMPLE_MANIFEST).unwrap();
        let since = NaiveDate::from_ymd_opt(2025, 1, 15);
        let filtered = m.filter_since(since);
        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].url.contains("2025-01-15"));
    }

    #[test]
    fn filter_none_returns_all() {
        let m = Manifest::from_json(SAMPLE_MANIFEST).unwrap();
        let filtered = m.filter_since(None);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn manifest_without_meta() {
        let json = r#"{
            "entries": [{"url": "s3://openalex/data/works/updated_date=2025-01-15/part_0000.gz"}]
        }"#;
        let m = Manifest::from_json(json).unwrap();
        assert!(m.entries[0].meta.is_none());
    }
}
