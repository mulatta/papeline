//! Main runner for PubMed pipeline

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use indicatif::MultiProgress;
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

/// Run the PubMed pipeline
pub fn run(config: &Config) -> Result<Summary> {
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

    let total_files = entries.len();
    log::info!(
        "Processing {} files with {} workers",
        total_files,
        config.workers
    );

    // Setup progress
    let multi = MultiProgress::new();
    let rows_counter = AtomicUsize::new(0);
    let completed_counter = AtomicUsize::new(0);
    let failed_counter = AtomicUsize::new(0);

    // Build thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .build()
        .context("Failed to create thread pool")?;

    // Process files in parallel
    pool.install(|| {
        entries.par_iter().for_each(|entry| {
            let pb = multi.add(indicatif::ProgressBar::new_spinner());
            pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.green} {prefix:>20.cyan} {bar:20} {bytes}/{total_bytes} {wide_msg}")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.set_prefix(entry.filename.clone());

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

    // Log summary
    log::info!("=== PubMed Pipeline Summary ===");
    log::info!(
        "Files: {}/{} completed ({} failed)",
        summary.completed_files,
        summary.total_files,
        summary.failed_files
    );
    log::info!("Articles: {}", summary.total_articles);
    log::info!("Time: {:.1}s", summary.elapsed.as_secs_f64());

    if summary.total_articles > 0 {
        let rate = summary.total_articles as f64 / summary.elapsed.as_secs_f64();
        log::info!("Throughput: {:.0} articles/sec", rate);
    }

    Ok(summary)
}
