//! Pipeline orchestration for OpenAlex data fetching

use std::fs;
use std::sync::Mutex;
use std::time::Instant;

use rayon::prelude::*;

use papeline_core::SharedProgress;

use crate::config::Config;
use crate::manifest::{ManifestEntry, fetch_manifest};
use crate::state::ShardInfo;
use crate::worker::{ShardStats, process_shard};

/// Run the OpenAlex data pipeline
pub fn run(config: &Config, progress: SharedProgress) -> anyhow::Result<RunSummary> {
    let start = Instant::now();

    // Create output directory
    fs::create_dir_all(&config.output_dir)?;

    // Fetch manifest
    log::info!("Fetching manifest for {}...", config.entity.manifest_path());
    let manifest = fetch_manifest(config.entity.manifest_path())?;
    log::info!("Manifest contains {} entries", manifest.len());

    // Filter by date
    let entries = manifest.filter_since(config.since);
    log::info!(
        "After date filter (since {:?}): {} entries",
        config.since,
        entries.len()
    );

    // Apply max_shards limit
    let entries: Vec<&ManifestEntry> = if let Some(max) = config.max_shards {
        entries.into_iter().take(max).collect()
    } else {
        entries
    };

    if entries.is_empty() {
        log::warn!("No shards to process");
        return Ok(RunSummary::empty());
    }

    log::info!("Processing {} shards", entries.len());

    // Build shard info list
    let shards: Vec<ShardInfo> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| ShardInfo {
            entity: config.entity,
            shard_idx: idx,
            url: entry.http_url(),
            content_length: entry.meta.as_ref().map(|m| m.content_length),
        })
        .collect();

    let total_shards = shards.len();
    let stats: Mutex<Vec<ShardStats>> = Mutex::new(Vec::new());
    let failed: Mutex<usize> = Mutex::new(0);

    // Process shards in parallel using the global rayon pool
    shards.par_iter().for_each(|shard| {
        let pb = progress.shard_bar(&format!("shard_{:04}", shard.shard_idx));
        pb.set_message("connecting...");

        match process_shard(shard, &config.output_dir, config.zstd_level, &pb) {
            Ok(s) => {
                pb.finish_and_clear();
                stats.lock().unwrap().push(s);
            }
            Err(_) => {
                pb.finish_and_clear();
                *failed.lock().unwrap() += 1;
            }
        }
    });

    // Collect results
    let stats = stats.into_inner().unwrap();
    let failed_count = failed.into_inner().unwrap();

    let summary = RunSummary {
        total_shards,
        completed_shards: stats.len(),
        failed_shards: failed_count,
        total_rows: stats.iter().map(|s| s.rows_written).sum(),
        total_lines: stats.iter().map(|s| s.lines_scanned).sum(),
        parse_errors: stats.iter().map(|s| s.parse_errors).sum(),
        elapsed: start.elapsed(),
    };

    Ok(summary)
}

/// Summary of pipeline run
#[derive(Debug)]
pub struct RunSummary {
    pub total_shards: usize,
    pub completed_shards: usize,
    pub failed_shards: usize,
    pub total_rows: usize,
    pub total_lines: usize,
    pub parse_errors: usize,
    pub elapsed: std::time::Duration,
}

impl RunSummary {
    pub fn empty() -> Self {
        Self {
            total_shards: 0,
            completed_shards: 0,
            failed_shards: 0,
            total_rows: 0,
            total_lines: 0,
            parse_errors: 0,
            elapsed: std::time::Duration::ZERO,
        }
    }

    pub fn log(&self) {
        log::info!("=== Pipeline Summary ===");
        log::info!(
            "Shards: {}/{} completed ({} failed)",
            self.completed_shards,
            self.total_shards,
            self.failed_shards
        );
        log::info!(
            "Rows: {} from {} lines ({} parse errors)",
            self.total_rows,
            self.total_lines,
            self.parse_errors
        );
        log::info!("Time: {:.1}s", self.elapsed.as_secs_f64());
        if self.total_rows > 0 {
            let rows_per_sec = self.total_rows as f64 / self.elapsed.as_secs_f64();
            log::info!("Throughput: {:.0} rows/sec", rows_per_sec);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_summary_empty() {
        let summary = RunSummary::empty();
        assert_eq!(summary.total_shards, 0);
        assert_eq!(summary.completed_shards, 0);
        assert_eq!(summary.failed_shards, 0);
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.elapsed, std::time::Duration::ZERO);
    }

    #[test]
    fn run_summary_log_does_not_panic() {
        let summary = RunSummary {
            total_shards: 10,
            completed_shards: 8,
            failed_shards: 2,
            total_rows: 1000,
            total_lines: 1200,
            parse_errors: 200,
            elapsed: std::time::Duration::from_secs(5),
        };
        // Just verify it doesn't panic
        summary.log();
    }

    #[test]
    fn run_summary_log_zero_rows() {
        let summary = RunSummary::empty();
        // Should not panic even with zero elapsed time
        summary.log();
    }
}
