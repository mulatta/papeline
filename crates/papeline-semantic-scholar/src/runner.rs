//! Main execution logic for S2 fetcher

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::Context;
use papeline_core::{
    CorpusIdSet, LanceWriter, SharedProgress, cleanup_tmp_files, is_shutdown_requested,
};

use crate::api::{fetch_dataset_urls, load_urls};
use crate::config::Config;
use crate::state::Dataset;
use crate::url_expiry;
use crate::{coverage, stats, worker};

/// Main entry point for fetch command
pub fn run(config: &Config, progress: SharedProgress) -> anyhow::Result<ExitCode> {
    let release_dir = config.output_dir.join(&config.release_id);
    std::fs::create_dir_all(&release_dir).context("Cannot create output directory")?;

    log::info!(
        "s2-dataset-fetcher starting: release={}, workers={}, domains={:?}, datasets={:?}",
        config.release_id,
        config.workers,
        config.domains,
        config.datasets
    );

    // Check URL expiry and refresh if needed
    let api_key = std::env::var("S2_API_KEY").ok();
    if refresh_urls_if_expiring(
        &config.url_dir,
        &config.release_id,
        api_key.as_deref(),
        &config.datasets,
    )? {
        log::info!("URLs refreshed successfully");
    }

    cleanup_tmp_files(&release_dir).context("Failed to clean stale tmp files")?;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .build()
        .context("Failed to create thread pool")?;

    let has_papers = config.datasets.contains(&Dataset::Papers);
    let filter_datasets: Vec<Dataset> = config
        .datasets
        .iter()
        .copied()
        .filter(|d| *d != Dataset::Papers)
        .collect();

    let mut paper_summary: Option<stats::PaperSummary> = None;
    let mut filter_summary: Option<stats::FilterSummary> = None;
    let is_tty = progress.is_tty();

    // Phase 1: papers → corpus_ids
    if has_papers {
        log::info!("Phase 1: processing papers");
        let paper_urls = load_urls(&config.url_dir, "papers", config.max_shards)?;
        let (failed, summary) = pool
            .install(|| worker::process_paper_shards(&paper_urls, &release_dir, config, &progress));
        if is_tty {
            summary.print();
        } else {
            summary.log();
        }
        if is_shutdown_requested() {
            log::warn!("Shutdown requested, exiting after Phase 1");
            return Ok(ExitCode::from(130));
        }
        if failed > 0 {
            log::error!("Phase 1: {failed} shards failed, aborting");
            return Ok(ExitCode::from(1));
        }
        paper_summary = Some(summary);
    }

    // Phase 2: filter other datasets using corpus_ids
    if !filter_datasets.is_empty() {
        // Re-check URL expiry before Phase 2 (Phase 1 may have taken a while)
        if refresh_urls_if_expiring(
            &config.url_dir,
            &config.release_id,
            api_key.as_deref(),
            &filter_datasets,
        )? {
            log::info!("URLs refreshed before Phase 2");
        }

        let (failed, summary) =
            run_filter_datasets(config, &release_dir, &pool, &filter_datasets, &progress)?;
        if is_tty {
            summary.print();
        } else {
            summary.log();
        }
        if is_shutdown_requested() {
            log::warn!("Shutdown requested, exiting during Phase 2");
            return Ok(ExitCode::from(130));
        }
        if failed > 0 {
            log::error!("Phase 2: {failed} shards failed");
            return Ok(ExitCode::from(1));
        }
        filter_summary = Some(summary);
    }

    // Final summary
    if let (Some(paper), Some(filter)) = (paper_summary, filter_summary) {
        let final_summary = stats::FinalSummary { paper, filter };
        if is_tty {
            final_summary.print();
        } else {
            final_summary.log();
        }
    }

    // Phase 3: Coverage analysis
    log::info!("Phase 3: analyzing coverage");
    match coverage::CoverageStats::compute(&release_dir) {
        Ok(cov) => {
            if is_tty {
                cov.print();
            } else {
                cov.log();
            }
        }
        Err(e) => log::warn!("Coverage analysis skipped: {e}"),
    }

    match coverage::FieldCompleteness::compute(&release_dir) {
        Ok(fields) => {
            if is_tty {
                fields.print();
            } else {
                fields.log();
            }
        }
        Err(e) => log::warn!("Field completeness skipped: {e}"),
    }

    log::info!("s2-dataset-fetcher completed successfully");
    Ok(ExitCode::SUCCESS)
}

/// Phase 2: load corpus IDs, filter other datasets, write lance embeddings
fn run_filter_datasets(
    config: &Config,
    release_dir: &Path,
    pool: &rayon::ThreadPool,
    filter_datasets: &[Dataset],
    progress: &SharedProgress,
) -> anyhow::Result<(usize, stats::FilterSummary)> {
    log::info!("Phase 2: processing filtered datasets");

    let corpus_ids_path = release_dir.join("corpus_ids.bin");
    let corpus_ids_mmap = papeline_core::load_corpus_ids(&corpus_ids_path)
        .context("Cannot load corpus_ids.bin. Run papers first.")?;
    log::info!("Loaded {} corpus IDs for filtering", corpus_ids_mmap.len());
    let corpus_ids = CorpusIdSet::from_mmap(&corpus_ids_mmap);

    let mut all_shards: Vec<(Dataset, usize, String)> = Vec::new();
    let mut shard_counts: Vec<(Dataset, usize)> = Vec::new();
    for &ds in filter_datasets {
        let urls = load_urls(&config.url_dir, ds.api_name(), config.max_shards)?;
        shard_counts.push((ds, urls.len()));
        for (idx, url) in urls.into_iter().enumerate() {
            all_shards.push((ds, idx, url));
        }
    }

    // Set up lance writer thread if embeddings are requested
    let has_embeddings = filter_datasets.contains(&Dataset::Embeddings);
    let (lance_channel, lance_handle) = if has_embeddings {
        let lance_path = release_dir.join("embeddings.lance");
        let (tx, rx) = std::sync::mpsc::sync_channel(32);
        let error_flag = Arc::new(AtomicBool::new(false));
        let writer_flag = error_flag.clone();

        let handle = std::thread::Builder::new()
            .name("lance-writer".into())
            .spawn(move || LanceWriter::new(rx, lance_path, writer_flag).run())
            .context("Failed to spawn lance writer")?;

        let channel = worker::LanceWriterHandle {
            sender: tx,
            error_flag,
        };
        (Some(channel), Some(handle))
    } else {
        (None, None)
    };

    let (mut failed, filter_summary) = pool.install(|| {
        worker::process_filtered_shards(
            all_shards,
            &corpus_ids,
            release_dir,
            config,
            lance_channel.as_ref(),
            &shard_counts,
            progress,
        )
    });

    // Drop sender → channel closes → LanceWriter flushes remaining
    drop(lance_channel);

    if let Some(handle) = lance_handle {
        match handle.join() {
            Ok(Ok(total)) => {
                log::info!("embeddings.lance: {total} rows");
                let _ =
                    std::fs::write(release_dir.join("embeddings.lance.done"), total.to_string());
            }
            Ok(Err(e)) => {
                log::error!("LanceWriter failed: {e}");
                failed += 1;
            }
            Err(_) => {
                log::error!("LanceWriter panicked");
                failed += 1;
            }
        }
    }

    Ok((failed, filter_summary))
}

/// Check if cached URLs are expiring soon and refresh if needed.
fn refresh_urls_if_expiring(
    url_dir: &Path,
    release_id: &str,
    api_key: Option<&str>,
    datasets: &[Dataset],
) -> anyhow::Result<bool> {
    let mut refreshed = false;

    for &ds in datasets {
        let api_name = ds.api_name();
        let cache_path = url_dir.join(format!("{api_name}.txt"));

        if !cache_path.exists() {
            continue;
        }

        // Check first URL's expiry
        let content = std::fs::read_to_string(&cache_path)?;
        let first_url = match content.lines().next() {
            Some(url) if !url.is_empty() => url,
            _ => continue,
        };

        // Stage 1: Check URL signature expiry (fast)
        let needs_refresh = if url_expiry::is_expiring_soon(first_url, url_expiry::EXPIRY_MARGIN) {
            let remaining = url_expiry::time_until_expiry(first_url);
            let remaining_str = remaining
                .map(|d| format!("{}s", d.as_secs()))
                .unwrap_or_else(|| "expired".to_string());
            log::warn!("{api_name}: URL signature expiring ({remaining_str})");
            true
        } else {
            // Stage 2: Test actual validity (catches STS token expiry)
            if !url_expiry::test_url_validity(first_url) {
                log::warn!("{api_name}: URL invalid (likely STS token expired)");
                true
            } else {
                false
            }
        };

        if needs_refresh {
            log::info!("{api_name}: refreshing URLs...");

            // Rate limit: delay between consecutive API requests
            if refreshed {
                std::thread::sleep(Duration::from_millis(500));
            }

            // Delete cache to force re-fetch
            std::fs::remove_file(&cache_path)?;

            // Re-fetch if we have API key
            if let Some(key) = api_key {
                fetch_dataset_urls(release_id, key, ds, url_dir)?;
                refreshed = true;
            } else {
                anyhow::bail!(
                    "{api_name} URLs expired but no S2_API_KEY available for refresh. \
                     Set S2_API_KEY or provide fresh --url-dir."
                );
            }
        }
    }

    Ok(refreshed)
}
