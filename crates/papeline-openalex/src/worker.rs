//! Shard processing — download and transform OpenAlex data

use std::path::Path;
use std::time::Duration;

use indicatif::ProgressBar;
use papeline_core::ShardError;
use papeline_core::shard_processor::{ShardConfig, process_gzip_shard};

use crate::config::TopicFilter;
use crate::schema;
use crate::state::ShardInfo;
use crate::transform::{WorkAccumulator, WorkRow};

/// Statistics from processing a single shard
#[derive(Debug)]
pub struct ShardStats {
    pub shard_idx: usize,
    pub lines_scanned: usize,
    pub parse_errors: usize,
    pub rows_written: usize,
    pub elapsed: Duration,
}

impl ShardStats {
    /// Log stats for non-TTY output
    pub fn log(&self) {
        log::info!(
            "shard_{:04}: {} rows from {} lines ({} errors) in {:.1}s",
            self.shard_idx,
            self.rows_written,
            self.lines_scanned,
            self.parse_errors,
            self.elapsed.as_secs_f64()
        );
    }
}

/// Process a single shard with retries
pub fn process_shard(
    shard: &ShardInfo,
    output_dir: &Path,
    zstd_level: i32,
    topic_filter: &TopicFilter,
    pb: &ProgressBar,
) -> Result<ShardStats, ShardError> {
    let label = format!("shard_{:04}", shard.shard_idx);
    let cfg = ShardConfig {
        url: &shard.url,
        shard_label: &label,
        output_prefix: shard.entity.output_prefix(),
        shard_idx: shard.shard_idx,
        output_dir,
        schema: schema::works(),
        zstd_level,
        content_length_hint: shard.content_length,
    };
    let filter = topic_filter;
    let stats = process_gzip_shard(&cfg, pb, WorkAccumulator::new, |line| {
        if !filter.is_empty() && !filter.pre_filter(line) {
            return None;
        }
        let row = sonic_rs::from_str::<WorkRow>(line).ok()?;
        if !filter.is_empty() && !filter.matches(&row) {
            return None;
        }
        Some(row)
    })?;

    Ok(ShardStats {
        shard_idx: shard.shard_idx,
        lines_scanned: stats.lines_scanned,
        parse_errors: stats.lines_scanned.saturating_sub(stats.rows_written),
        rows_written: stats.rows_written,
        elapsed: stats.elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_stats_log_does_not_panic() {
        let stats = ShardStats {
            shard_idx: 0,
            lines_scanned: 100,
            parse_errors: 5,
            rows_written: 95,
            elapsed: Duration::from_secs(1),
        };
        // Just verify it doesn't panic
        stats.log();
    }
}
