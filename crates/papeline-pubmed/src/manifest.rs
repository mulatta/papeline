//! PubMed FTP manifest parsing
//!
//! Parses the directory listing from NCBI FTP to get baseline file URLs.

use anyhow::{Context, Result};
use papeline_core::SHARED_RUNTIME;

/// Entry in the PubMed baseline manifest
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub filename: String,
    pub url: String,
    pub size_bytes: Option<u64>,
}

/// Fetch and parse the PubMed baseline manifest
pub fn fetch_manifest(base_url: &str) -> Result<Vec<ManifestEntry>> {
    SHARED_RUNTIME
        .handle()
        .block_on(async { fetch_manifest_async(base_url).await })
}

async fn fetch_manifest_async(base_url: &str) -> Result<Vec<ManifestEntry>> {
    let client = papeline_core::http_client();

    let mut last_err = None;
    for attempt in 0..3 {
        if attempt > 0 {
            let delay = std::time::Duration::from_secs(2u64 << (attempt - 1));
            log::info!(
                "Retrying manifest fetch (attempt {}/3) after {delay:?}",
                attempt + 1
            );
            tokio::time::sleep(delay).await;
        }

        match tokio::time::timeout(std::time::Duration::from_secs(60), async {
            client
                .get(base_url)
                .send()
                .await
                .context("Failed to fetch manifest")?
                .text()
                .await
                .context("Failed to read manifest body")
        })
        .await
        {
            Ok(Ok(html)) => return parse_html_listing(&html, base_url),
            Ok(Err(e)) => {
                log::warn!("Manifest fetch failed: {e:#}");
                last_err = Some(e);
            }
            Err(_) => {
                log::warn!("Manifest fetch timed out (60s)");
                last_err = Some(anyhow::anyhow!("manifest fetch timed out after 60s"));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("manifest fetch failed")))
}

/// Extract baseline year from manifest entries.
///
/// Parses filenames like `pubmed26n0001.xml.gz` → 26.
/// Returns `None` if no entries or pattern unrecognized.
pub fn extract_baseline_year(entries: &[ManifestEntry]) -> Option<u16> {
    entries.first().and_then(|e| {
        let name = e.filename.strip_prefix("pubmed")?;
        let n_pos = name.find('n')?;
        name[..n_pos].parse::<u16>().ok()
    })
}

/// Fetch manifest and extract baseline year in one call.
pub fn fetch_baseline_year(base_url: &str) -> Result<u16> {
    let entries = fetch_manifest(base_url)?;
    extract_baseline_year(&entries)
        .with_context(|| "failed to extract baseline year from PubMed manifest filenames")
}

/// Parse HTML directory listing for .xml.gz files
fn parse_html_listing(html: &str, base_url: &str) -> Result<Vec<ManifestEntry>> {
    let mut entries = Vec::new();

    // Parse lines like: <a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a>     2026-01-29 14:48   19M
    for line in html.lines() {
        // Look for xml.gz links
        if let Some(start) = line.find("href=\"") {
            let rest = &line[start + 6..];
            if let Some(end) = rest.find('"') {
                let filename = &rest[..end];

                // Only process .xml.gz files (not .md5)
                if filename.ends_with(".xml.gz") && !filename.ends_with(".md5") {
                    let url = format!("{}/{}", base_url.trim_end_matches('/'), filename);

                    // Try to parse size from the line
                    let size_bytes = parse_size_from_line(line);

                    entries.push(ManifestEntry {
                        filename: filename.to_string(),
                        url,
                        size_bytes,
                    });
                }
            }
        }
    }

    // Sort by filename for consistent ordering
    entries.sort_by(|a, b| a.filename.cmp(&b.filename));

    Ok(entries)
}

/// Parse size from HTML line (e.g., "19M" or "4.5K")
fn parse_size_from_line(line: &str) -> Option<u64> {
    // Look for patterns like "19M" or "4.5K" at the end
    let parts: Vec<&str> = line.split_whitespace().collect();

    for part in parts.iter().rev() {
        if let Some(size) = parse_size_string(part) {
            return Some(size);
        }
    }

    None
}

fn parse_size_string(s: &str) -> Option<u64> {
    let s = s.trim();

    if s.is_empty() {
        return None;
    }

    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('K') {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix('M') {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('G') {
        (n, 1024 * 1024 * 1024)
    } else if s.chars().all(|c| c.is_ascii_digit()) {
        (s, 1)
    } else {
        return None;
    };

    num_str
        .parse::<f64>()
        .ok()
        .map(|n| (n * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_HTML: &str = r#"
<html>
<head><title>Index of /pubmed/baseline</title></head>
<body>
<h1>Index of /pubmed/baseline</h1>
<pre>Name                     Last modified      Size
<a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a>     2026-01-29 14:48   19M
<a href="pubmed26n0001.xml.gz.md5">pubmed26n0001.xml.gz.md5</a> 2026-01-29 14:48   60
<a href="pubmed26n0002.xml.gz">pubmed26n0002.xml.gz</a>     2026-01-29 14:48   17M
</pre>
</body>
</html>"#;

    #[test]
    fn parse_listing() {
        let entries =
            parse_html_listing(SAMPLE_HTML, "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/")
                .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].filename, "pubmed26n0001.xml.gz");
        assert_eq!(entries[1].filename, "pubmed26n0002.xml.gz");
    }

    #[test]
    fn parse_size() {
        assert_eq!(parse_size_string("19M"), Some(19 * 1024 * 1024));
        assert_eq!(parse_size_string("4.5K"), Some((4.5 * 1024.0) as u64));
        assert_eq!(parse_size_string("100"), Some(100));
    }

    #[test]
    fn excludes_md5_files() {
        let entries = parse_html_listing(SAMPLE_HTML, "https://example.com/").unwrap();
        assert!(entries.iter().all(|e| !e.filename.ends_with(".md5")));
    }

    #[test]
    fn url_construction() {
        let entries =
            parse_html_listing(SAMPLE_HTML, "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/")
                .unwrap();

        assert_eq!(
            entries[0].url,
            "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed26n0001.xml.gz"
        );
    }

    #[test]
    fn url_construction_trailing_slash() {
        let entries = parse_html_listing(SAMPLE_HTML, "https://example.com/path").unwrap();
        assert!(entries[0].url.starts_with("https://example.com/path/"));
    }

    #[test]
    fn parse_size_gigabytes() {
        assert_eq!(parse_size_string("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(
            parse_size_string("2.5G"),
            Some((2.5 * 1024.0 * 1024.0 * 1024.0) as u64)
        );
    }

    #[test]
    fn parse_size_invalid() {
        assert!(parse_size_string("").is_none());
        assert!(parse_size_string("abc").is_none());
        assert!(parse_size_string("12X").is_none());
    }

    #[test]
    fn entries_sorted_by_filename() {
        let html = r#"
<a href="pubmed26n0003.xml.gz">pubmed26n0003.xml.gz</a>
<a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a>
<a href="pubmed26n0002.xml.gz">pubmed26n0002.xml.gz</a>
"#;
        let entries = parse_html_listing(html, "https://example.com/").unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].filename, "pubmed26n0001.xml.gz");
        assert_eq!(entries[1].filename, "pubmed26n0002.xml.gz");
        assert_eq!(entries[2].filename, "pubmed26n0003.xml.gz");
    }

    #[test]
    fn manifest_entry_size_bytes() {
        let entries = parse_html_listing(SAMPLE_HTML, "https://example.com/").unwrap();

        assert_eq!(entries[0].size_bytes, Some(19 * 1024 * 1024));
        assert_eq!(entries[1].size_bytes, Some(17 * 1024 * 1024));
    }

    #[test]
    fn extract_baseline_year_from_entries() {
        let entries = parse_html_listing(SAMPLE_HTML, "https://example.com/").unwrap();
        assert_eq!(extract_baseline_year(&entries), Some(26));
    }

    #[test]
    fn extract_baseline_year_empty() {
        assert_eq!(extract_baseline_year(&[]), None);
    }

    #[test]
    fn extract_baseline_year_different_year() {
        let html = r#"<a href="pubmed25n0001.xml.gz">pubmed25n0001.xml.gz</a>"#;
        let entries = parse_html_listing(html, "https://example.com/").unwrap();
        assert_eq!(extract_baseline_year(&entries), Some(25));
    }
}
