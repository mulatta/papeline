//! Main runner for PubMed pipeline

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use papeline_core::SharedProgress;
use rayon::prelude::*;

use crate::config::Config;
use crate::manifest::{ManifestEntry, fetch_manifest};
use crate::worker;

/// Pipeline execution summary
#[derive(Debug)]
pub struct Summary {
    pub total_files: usize,
    pub completed_files: usize,
    pub failed_files: usize,
    pub total_articles: usize,
    pub elapsed: std::time::Duration,
}

/// Run the PubMed pipeline with pre-fetched manifest entries.
/// Use this when entries are already available (e.g. from pre-fetch for cache key computation).
pub fn run_with_entries(
    config: &Config,
    entries: Vec<ManifestEntry>,
    progress: SharedProgress,
) -> Result<Summary> {
    let start = Instant::now();
    std::fs::create_dir_all(&config.output_dir).context("Failed to create output directory")?;

    // Apply limit
    let entries: Vec<ManifestEntry> = if let Some(limit) = config.max_files {
        entries.into_iter().take(limit).collect()
    } else {
        entries
    };

    run_entries(config, entries, progress, start)
}

/// Run the PubMed pipeline (fetches manifest internally).
pub fn run(config: &Config, progress: SharedProgress) -> Result<Summary> {
    let start = Instant::now();

    // Create output directory
    std::fs::create_dir_all(&config.output_dir).context("Failed to create output directory")?;

    // Fetch manifest
    log::info!("Fetching PubMed baseline manifest...");
    let entries = fetch_manifest(&config.base_url)?;
    log::info!("Found {} files in manifest", entries.len());

    // Apply limit
    let entries: Vec<ManifestEntry> = if let Some(limit) = config.max_files {
        entries.into_iter().take(limit).collect()
    } else {
        entries
    };

    run_entries(config, entries, progress, start)
}

fn run_entries(
    config: &Config,
    entries: Vec<ManifestEntry>,
    progress: SharedProgress,
    start: Instant,
) -> Result<Summary> {
    let total_files = entries.len();
    log::info!("Processing {} files", total_files);

    let rows_counter = AtomicUsize::new(0);
    let completed_counter = AtomicUsize::new(0);
    let failed_counter = AtomicUsize::new(0);

    // Process files in parallel using the global rayon pool
    entries.par_iter().for_each(|entry| {
        let pb = progress.shard_bar(&entry.filename);

        match worker::process_file(entry, &config.output_dir, config, pb, &rows_counter) {
            Ok(count) => {
                completed_counter.fetch_add(1, Ordering::Relaxed);
                log::debug!("{}: {} articles", entry.filename, count);
            }
            Err(e) => {
                failed_counter.fetch_add(1, Ordering::Relaxed);
                log::error!("{}: {}", entry.filename, e);
            }
        }
    });

    let elapsed = start.elapsed();
    let completed = completed_counter.load(Ordering::Relaxed);
    let failed = failed_counter.load(Ordering::Relaxed);
    let total_articles = rows_counter.load(Ordering::Relaxed);

    let summary = Summary {
        total_files,
        completed_files: completed,
        failed_files: failed,
        total_articles,
        elapsed,
    };

    Ok(summary)
}
