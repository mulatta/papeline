//! S2 fetcher configuration

use std::path::PathBuf;

use anyhow::Context;

use crate::api::{fetch_all_dataset_urls, resolve_release};
use crate::state::Dataset;

/// CLI-facing arguments for the fetch command (plain struct, no clap derive).
#[derive(Debug)]
pub struct FetchArgs {
    pub release: Option<String>,
    pub url_dir: Option<PathBuf>,
    pub domains: Vec<String>,
    pub datasets: Vec<String>,
    pub output_dir: PathBuf,
    pub max_shards: Option<usize>,
    pub zstd_level: i32,
}

/// Runtime configuration for s2-fetcher
#[derive(Debug)]
pub struct Config {
    pub url_dir: PathBuf,
    pub output_dir: PathBuf,
    pub release_id: String,
    pub datasets: Vec<Dataset>,
    pub domains: Vec<String>,
    pub max_shards: Option<usize>,
    pub zstd_level: i32,
}

impl TryFrom<FetchArgs> for Config {
    type Error = anyhow::Error;

    fn try_from(args: FetchArgs) -> Result<Self, Self::Error> {
        let datasets: Vec<Dataset> = args
            .datasets
            .iter()
            .map(|s| Dataset::from_name(s).ok_or_else(|| anyhow::anyhow!("Unknown dataset: {s}")))
            .collect::<anyhow::Result<_>>()?;

        let (url_dir, release_id) = if let Some(ref url_dir) = args.url_dir {
            anyhow::ensure!(
                url_dir.exists(),
                "URL directory does not exist: {}",
                url_dir.display()
            );
            let release_id = detect_release_id(url_dir)?;
            (url_dir.clone(), release_id)
        } else if let Some(ref release) = args.release {
            let api_key = std::env::var("S2_API_KEY")
                .context("S2_API_KEY environment variable required for --release")?;
            let release_id = resolve_release(release, &api_key)?;
            let url_dir = PathBuf::from(format!(".cache/urls/{release_id}"));
            fetch_all_dataset_urls(&release_id, &api_key, &datasets, &url_dir)?;
            (url_dir, release_id)
        } else {
            anyhow::bail!("Either --release or --url-dir must be provided");
        };

        Ok(Self {
            url_dir,
            output_dir: args.output_dir,
            release_id,
            datasets,
            domains: args.domains,
            max_shards: args.max_shards,
            zstd_level: args.zstd_level,
        })
    }
}

/// Detect release ID from URL file contents (S3 path contains date-like release ID)
fn detect_release_id(url_dir: &std::path::Path) -> anyhow::Result<String> {
    let entries = std::fs::read_dir(url_dir).context("Cannot read URL dir")?;
    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "txt"))
        .collect();
    // Prefer papers.txt for stability
    paths.sort_by(|a, b| {
        let a_is_papers = a.file_name().is_some_and(|n| n == "papers.txt");
        let b_is_papers = b.file_name().is_some_and(|n| n == "papers.txt");
        b_is_papers.cmp(&a_is_papers)
    });
    for path in paths {
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Some(first_line) = content.lines().next() {
                if let Some(id) = extract_release_id_from_url(first_line) {
                    return Ok(id);
                }
            }
        }
    }
    anyhow::bail!("Cannot detect release ID from URLs. Use --release to specify.")
}

/// Extract release ID (date) from S2 dataset URL
fn extract_release_id_from_url(url: &str) -> Option<String> {
    for segment in url.split('/') {
        if segment.len() == 10 && segment.chars().nth(4) == Some('-') {
            return Some(segment.to_string());
        }
        if segment.len() == 8 && segment.chars().all(|c| c.is_ascii_digit()) {
            let formatted = format!("{}-{}-{}", &segment[..4], &segment[4..6], &segment[6..8]);
            return Some(formatted);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_id_from_yyyy_mm_dd() {
        let url = "https://s3-us-west-2.amazonaws.com/ai2-s2ag/2025-01-14/papers/part-000.gz";
        assert_eq!(
            extract_release_id_from_url(url),
            Some("2025-01-14".to_string())
        );
    }

    #[test]
    fn release_id_from_yyyymmdd() {
        let url = "https://example.com/datasets/20250114/papers.gz";
        assert_eq!(
            extract_release_id_from_url(url),
            Some("2025-01-14".to_string())
        );
    }

    #[test]
    fn release_id_no_match() {
        assert_eq!(
            extract_release_id_from_url("https://example.com/foo/bar"),
            None
        );
        assert_eq!(extract_release_id_from_url(""), None);
    }
}
