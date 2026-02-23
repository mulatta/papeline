//! Semantic Scholar API client

use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use papeline_core::{SHARED_RUNTIME, http_client};

use crate::state::Dataset;

const API_MAX_RETRIES: u32 = 5;
const API_BASE_DELAY: Duration = Duration::from_secs(2);

/// HTTP GET with retry for rate limit (429) and server errors (5xx)
pub fn api_get_with_retry(url: &str, api_key: &str) -> anyhow::Result<String> {
    for attempt in 0..API_MAX_RETRIES {
        let result: Result<String, reqwest::Error> = SHARED_RUNTIME.handle().block_on(async {
            let resp = http_client()
                .get(url)
                .header("x-api-key", api_key)
                .send()
                .await?
                .error_for_status()?;
            resp.text().await
        });

        match result {
            Ok(text) => {
                return Ok(text);
            }
            Err(e) => {
                let status = e.status().map(|s| s.as_u16());
                let is_retryable = matches!(status, Some(429) | Some(500..=599));

                if is_retryable && attempt < API_MAX_RETRIES - 1 {
                    let delay = API_BASE_DELAY * 2u32.pow(attempt);
                    log::warn!(
                        "API request failed (status {}), retry {}/{} in {:?}",
                        status.map_or("?".to_string(), |s| s.to_string()),
                        attempt + 1,
                        API_MAX_RETRIES,
                        delay
                    );
                    std::thread::sleep(delay);
                } else {
                    // Strip URL from error to avoid leaking API endpoints in logs
                    let msg = papeline_core::stream::StreamError::from_reqwest(&e);
                    anyhow::bail!("API request failed: {msg}");
                }
            }
        }
    }
    anyhow::bail!("API request failed after {API_MAX_RETRIES} retries")
}

/// Resolve release ID — "latest" fetches from API, otherwise pass through
pub fn resolve_release(release: &str, api_key: &str) -> anyhow::Result<String> {
    if release != "latest" {
        return Ok(release.to_string());
    }
    log::debug!("Resolving latest release ID...");
    let body = api_get_with_retry(
        "https://api.semanticscholar.org/datasets/v1/release/",
        api_key,
    )
    .context("Failed to fetch releases")?;
    let releases: Vec<String> = serde_json::from_str(&body).context("Invalid release list JSON")?;
    let id = releases
        .into_iter()
        .last()
        .context("Empty release list from S2 API")?;
    log::debug!("Latest release: {id}");
    Ok(id)
}

/// Fetch URL lists for all requested datasets, caching in url_dir
pub fn fetch_all_dataset_urls(
    release_id: &str,
    api_key: &str,
    datasets: &[Dataset],
    url_dir: &Path,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(url_dir).context("Cannot create URL cache dir")?;

    let mut fetched_count = 0;
    for &ds in datasets {
        let api_name = ds.api_name();
        let cache_path = url_dir.join(format!("{api_name}.txt"));

        if cache_path.exists() {
            log::debug!("{api_name}: using cached URLs");
            continue;
        }

        // Rate limit: delay between consecutive API requests
        if fetched_count > 0 {
            std::thread::sleep(Duration::from_millis(500));
        }
        fetch_dataset_urls(release_id, api_key, ds, url_dir)?;
        fetched_count += 1;
    }
    Ok(())
}

/// Fetch URLs for a single dataset
pub fn fetch_dataset_urls(
    release_id: &str,
    api_key: &str,
    ds: Dataset,
    url_dir: &Path,
) -> anyhow::Result<()> {
    let api_name = ds.api_name();
    let cache_path = url_dir.join(format!("{api_name}.txt"));

    log::info!("Fetching URLs for {api_name}...");
    let url = format!(
        "https://api.semanticscholar.org/datasets/v1/release/{release_id}/dataset/{api_name}"
    );
    let body = api_get_with_retry(&url, api_key)
        .with_context(|| format!("Failed to fetch {api_name} URLs"))?;

    let parsed: serde_json::Value =
        serde_json::from_str(&body).with_context(|| format!("Invalid JSON for {api_name}"))?;
    let files = parsed["files"]
        .as_array()
        .with_context(|| format!("No 'files' array in {api_name} response"))?;

    let urls: Vec<&str> = files.iter().filter_map(|v| v.as_str()).collect();
    anyhow::ensure!(!urls.is_empty(), "No URLs in {api_name} response");

    let content = urls.join("\n") + "\n";
    std::fs::write(&cache_path, &content)
        .with_context(|| format!("Cannot write {}", cache_path.display()))?;
    log::info!("{api_name}: {} shards cached", urls.len());

    Ok(())
}

/// Load URLs from a dataset file, optionally limiting shard count
pub fn load_urls(
    url_dir: &Path,
    dataset: &str,
    max_shards: Option<usize>,
) -> anyhow::Result<Vec<String>> {
    let path = url_dir.join(format!("{dataset}.txt"));
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Cannot read {}", path.display()))?;
    let mut urls: Vec<String> = content
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();
    if let Some(max) = max_shards {
        urls.truncate(max);
    }
    anyhow::ensure!(!urls.is_empty(), "No URLs found in {}", path.display());
    log::info!("{dataset}: {} shards to process", urls.len());
    Ok(urls)
}
