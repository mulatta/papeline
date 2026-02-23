//! Worker for processing PubMed XML files

use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use indicatif::ProgressBar;
use papeline_core::progress::upgrade_to_bar;
use papeline_core::{ParquetSink, open_gzip_reader};

use crate::config::Config;
use crate::manifest::ManifestEntry;
use crate::parser::parse_pubmed_xml;
use crate::transform::ArticleAccumulator;

use papeline_core::stream::http_config;

/// Read chunk size for progress updates (64KB decompressed)
const READ_CHUNK: usize = 64 * 1024;

/// Process a single PubMed XML file
pub fn process_file(
    entry: &ManifestEntry,
    output_dir: &Path,
    config: &Config,
    pb: ProgressBar,
    rows_counter: &AtomicUsize,
) -> Result<usize> {
    pb.set_message(entry.filename.clone());

    // Download with semaphore — permit released after download so other
    // workers can start downloading while this one parses XML on CPU.
    let xml_content = {
        let mut last_err = None;
        let mut content = String::new();
        let max_retries = http_config().max_retries as usize;

        for attempt in 0..max_retries {
            if attempt > 0 {
                let delay = std::time::Duration::from_secs(2u64 << (attempt - 1));
                log::info!(
                    "{}: retry {}/{max_retries} after {delay:?}",
                    entry.filename,
                    attempt + 1
                );
                std::thread::sleep(delay);
                pb.set_message(format!("{} (retry {})", entry.filename, attempt + 1));
            }

            let _permit = papeline_core::acquire_download_permit();
            match download_xml(entry, &pb) {
                Ok(c) => {
                    content = c;
                    last_err = None;
                    break;
                }
                Err(e) => {
                    log::warn!("{}: download failed: {e:#}", entry.filename);
                    last_err = Some(e);
                }
            }
        }

        if let Some(e) = last_err {
            pb.finish_and_clear();
            return Err(e).with_context(|| format!("Failed after {max_retries} attempts"));
        }
        content
    }; // permit released here → CPU-only parse below

    // Parse articles (and deleted PMIDs from updatefiles)
    let parse_result = parse_pubmed_xml(&xml_content)
        .with_context(|| format!("Failed to parse {}", entry.filename))?;

    if !parse_result.deleted_pmids.is_empty() {
        log::info!(
            "{}: {} deleted PMIDs (will be excluded at join stage)",
            entry.filename,
            parse_result.deleted_pmids.len()
        );
    }

    let articles = parse_result.articles;
    let article_count = articles.len();

    if article_count == 0 {
        pb.finish_and_clear();
        return Ok(0);
    }

    // Extract shard index from filename (e.g., "pubmed26n0001.xml.gz" -> 1)
    let shard_idx = extract_shard_index(&entry.filename);

    // Create parquet sink
    let mut sink = ParquetSink::new(
        "pubmed",
        shard_idx,
        output_dir,
        crate::schema::articles(),
        config.zstd_level,
    )
    .context("Failed to create parquet sink")?;

    // Accumulate and write
    let mut acc = ArticleAccumulator::new();

    for article in articles {
        acc.push(article);

        // Flush batch when full
        if acc.len() >= 10_000 {
            let batch = acc.take_batch()?;
            sink.write_batch(&batch)?;
        }
    }

    // Write remaining
    if !acc.is_empty() {
        let batch = acc.take_batch()?;
        sink.write_batch(&batch)?;
    }

    sink.finalize()?;

    rows_counter.fetch_add(article_count, Ordering::Relaxed);
    pb.finish_and_clear();

    Ok(article_count)
}

/// Download and decompress a PubMed XML file with progress tracking
fn download_xml(entry: &ManifestEntry, pb: &ProgressBar) -> Result<String> {
    let (mut reader, byte_counter, total_bytes) =
        open_gzip_reader(&entry.url).with_context(|| format!("Failed to open {}", entry.url))?;

    if let Some(total) = total_bytes {
        upgrade_to_bar(pb, total);
    }

    let mut bytes = Vec::new();
    let mut chunk = vec![0u8; READ_CHUNK];

    loop {
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                bytes.extend_from_slice(&chunk[..n]);
                pb.set_position(byte_counter.load(Ordering::Relaxed));
            }
            Err(e) => return Err(e).context("Failed to read XML content"),
        }
    }

    pb.set_position(byte_counter.load(Ordering::Relaxed));
    String::from_utf8(bytes).context("Invalid UTF-8 in XML content")
}

/// Statistics from processing
#[derive(Debug, Default)]
pub struct ProcessStats {
    pub total_files: usize,
    pub completed_files: usize,
    pub failed_files: usize,
    pub total_articles: usize,
}

/// Extract shard index from filename (e.g., "pubmed26n0001.xml.gz" -> 1)
fn extract_shard_index(filename: &str) -> usize {
    // Find the numeric suffix before .xml.gz
    // Format: pubmedYYnNNNN.xml.gz where NNNN is the shard number
    filename
        .strip_suffix(".xml.gz")
        .and_then(|s| {
            // Find last 'n' and extract the number after it
            s.rfind('n').map(|pos| &s[pos + 1..])
        })
        .and_then(|num_str| num_str.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_shard_index_standard() {
        assert_eq!(extract_shard_index("pubmed26n0001.xml.gz"), 1);
        assert_eq!(extract_shard_index("pubmed26n0100.xml.gz"), 100);
        assert_eq!(extract_shard_index("pubmed26n1234.xml.gz"), 1234);
    }

    #[test]
    fn extract_shard_index_different_year() {
        assert_eq!(extract_shard_index("pubmed24n0001.xml.gz"), 1);
        assert_eq!(extract_shard_index("pubmed25n0500.xml.gz"), 500);
    }

    #[test]
    fn extract_shard_index_invalid() {
        assert_eq!(extract_shard_index("invalid.xml.gz"), 0);
        assert_eq!(extract_shard_index("pubmed26.xml.gz"), 0);
        assert_eq!(extract_shard_index(""), 0);
    }

    #[test]
    fn extract_shard_index_no_suffix() {
        assert_eq!(extract_shard_index("pubmed26n0001"), 0);
    }

    #[test]
    fn process_stats_default() {
        let stats = ProcessStats::default();
        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.completed_files, 0);
        assert_eq!(stats.failed_files, 0);
        assert_eq!(stats.total_articles, 0);
    }
}
