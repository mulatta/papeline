//! Statistics collection and reporting for dataset processing.
//!
//! Processing flow:
//! 1. Paper Extraction: papers.json.gz → papers/authors/fields parquet
//! 2. Corpus Filtering: abstracts/tldrs/citations/embeddings filtered by corpus_ids
//!
//! Statistics hierarchy:
//! - Shard-level: `PaperShardStats`, `FilterShardStats`
//! - Summary-level: `PaperSummary`, `FilterSummary`
//! - Final: `FinalSummary`

use std::time::Duration;

use comfy_table::{Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use crate::state::Dataset;

// =============================================================================
// Paper Extraction — Shard-level
// =============================================================================

/// Per-shard statistics for paper extraction.
///
/// Collected during `attempt_paper_shard` for each papers_NNNN.json.gz file.
#[derive(Debug, Clone, Default)]
pub struct PaperShardStats {
    pub shard_idx: usize,
    /// Total lines read from the gzipped JSON file
    pub lines_scanned: usize,
    /// Lines skipped by cheap substring pre-filter (no domain keyword)
    pub pre_filtered: usize,
    /// JSON parsing failures
    pub parse_errors: usize,
    /// Parsed but FoS domain mismatch
    pub fos_excluded: usize,
    /// Papers matching target domains
    pub papers_matched: usize,
    /// Author rows written (denormalized)
    pub authors_rows: usize,
    /// Field rows written (denormalized)
    pub fields_rows: usize,
    /// Processing time for this shard
    pub elapsed: Duration,
}

impl PaperShardStats {
    /// Log shard completion (non-TTY mode only).
    pub fn log(&self) {
        let pct = if self.lines_scanned > 0 {
            self.papers_matched as f64 / self.lines_scanned as f64 * 100.0
        } else {
            0.0
        };
        log::info!(
            "papers_{:04}: {} / {} ({:.1}%) [{:.1}s]",
            self.shard_idx,
            fmt_num(self.papers_matched),
            fmt_num(self.lines_scanned),
            pct,
            self.elapsed.as_secs_f64()
        );
    }
}

// =============================================================================
// Paper Extraction — Summary
// =============================================================================

/// Aggregated statistics for paper extraction.
///
/// Computed from all `PaperShardStats` after paper processing completes.
#[derive(Debug, Default)]
pub struct PaperSummary {
    pub total_shards: usize,
    pub completed_shards: usize,
    pub failed_shards: usize,
    pub lines_scanned: usize,
    pub pre_filtered: usize,
    pub parse_errors: usize,
    pub fos_excluded: usize,
    pub papers_matched: usize,
    pub authors_rows: usize,
    pub fields_rows: usize,
    pub elapsed: Duration,
}

impl PaperSummary {
    /// Aggregate from individual shard stats.
    pub fn from_shards(shards: &[PaperShardStats], total: usize, failed: usize) -> Self {
        let mut stats = Self {
            total_shards: total,
            completed_shards: shards.len(),
            failed_shards: failed,
            ..Default::default()
        };
        for s in shards {
            stats.lines_scanned += s.lines_scanned;
            stats.pre_filtered += s.pre_filtered;
            stats.parse_errors += s.parse_errors;
            stats.fos_excluded += s.fos_excluded;
            stats.papers_matched += s.papers_matched;
            stats.authors_rows += s.authors_rows;
            stats.fields_rows += s.fields_rows;
            stats.elapsed = stats.elapsed.max(s.elapsed); // wall-clock approx
        }
        stats
    }

    /// Format summary table as a string.
    pub fn format_table(&self) -> String {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_header(vec![
                Cell::new("Phase 1: Paper Extraction")
                    .fg(Color::Cyan)
                    .add_attribute(comfy_table::Attribute::Bold),
                Cell::new("Value").fg(Color::Cyan),
                Cell::new("%").fg(Color::Cyan),
            ]);

        table.add_row(vec![
            Cell::new("Shards"),
            Cell::new(format!(
                "{}/{} ({} failed)",
                self.completed_shards, self.total_shards, self.failed_shards
            )),
            Cell::new(""),
        ]);
        table.add_row(vec![
            Cell::new("Lines scanned"),
            Cell::new(fmt_num(self.lines_scanned)),
            Cell::new(""),
        ]);
        table.add_row(vec![
            Cell::new("Pre-filtered"),
            Cell::new(fmt_num(self.pre_filtered)),
            Cell::new(format!("{:.1}", pct(self.pre_filtered, self.lines_scanned))),
        ]);
        table.add_row(vec![
            Cell::new("Parse errors"),
            Cell::new(fmt_num(self.parse_errors)),
            Cell::new(format!("{:.3}", pct(self.parse_errors, self.lines_scanned))),
        ]);
        table.add_row(vec![
            Cell::new("FoS excluded"),
            Cell::new(fmt_num(self.fos_excluded)),
            Cell::new(format!("{:.1}", pct(self.fos_excluded, self.lines_scanned))),
        ]);
        table.add_row(vec![
            Cell::new("Papers matched").fg(Color::Green),
            Cell::new(fmt_num(self.papers_matched)).fg(Color::Green),
            Cell::new(format!(
                "{:.1}",
                pct(self.papers_matched, self.lines_scanned)
            ))
            .fg(Color::Green),
        ]);
        table.add_row(vec![
            Cell::new("Authors"),
            Cell::new(fmt_num(self.authors_rows)),
            Cell::new(""),
        ]);
        table.add_row(vec![
            Cell::new("Fields"),
            Cell::new(fmt_num(self.fields_rows)),
            Cell::new(""),
        ]);

        format!("\n{table}")
    }

    /// Log minimal summary (non-TTY mode).
    pub fn log(&self) {
        log::info!(
            "Phase 1 complete: {} papers ({}/{} shards)",
            fmt_num(self.papers_matched),
            self.completed_shards,
            self.total_shards
        );
    }
}

// =============================================================================
// Corpus Filtering — Shard-level
// =============================================================================

/// Per-shard statistics for corpus-filtered dataset processing.
///
/// Collected during `attempt_filtered_shard` for abstracts/tldrs/citations/embeddings.
#[derive(Debug, Clone)]
pub struct FilterShardStats {
    pub dataset: Dataset,
    pub shard_idx: usize,
    /// Total lines read from the gzipped JSON file
    pub lines_scanned: usize,
    /// Lines excluded (corpus_id not in filter set)
    pub corpus_excluded: usize,
    /// Rows written to output
    pub rows_written: usize,
    /// Processing time for this shard
    pub elapsed: Duration,
}

impl FilterShardStats {
    /// Log shard completion (non-TTY mode only).
    pub fn log(&self) {
        let pct = if self.lines_scanned > 0 {
            self.rows_written as f64 / self.lines_scanned as f64 * 100.0
        } else {
            0.0
        };
        log::info!(
            "{}_{:04}: {} rows / {} lines ({:.1}%) [{:.1}s]",
            self.dataset,
            self.shard_idx,
            fmt_num(self.rows_written),
            fmt_num(self.lines_scanned),
            pct,
            self.elapsed.as_secs_f64()
        );
    }
}

// =============================================================================
// Corpus Filtering — Summary
// =============================================================================

/// Per-dataset aggregated statistics.
#[derive(Debug, Default, Clone)]
pub struct DatasetStats {
    pub total_shards: usize,
    pub completed_shards: usize,
    pub failed_shards: usize,
    pub lines_scanned: usize,
    pub corpus_excluded: usize,
    pub rows_written: usize,
    pub elapsed: Duration,
}

impl DatasetStats {
    fn add_shard(&mut self, shard: &FilterShardStats) {
        self.completed_shards += 1;
        self.lines_scanned += shard.lines_scanned;
        self.corpus_excluded += shard.corpus_excluded;
        self.rows_written += shard.rows_written;
        self.elapsed = self.elapsed.max(shard.elapsed);
    }
}

/// Aggregated statistics for corpus-filtered datasets.
///
/// Computed from all `FilterShardStats` after filtering completes.
#[derive(Debug, Default)]
pub struct FilterSummary {
    pub abstracts: DatasetStats,
    pub tldrs: DatasetStats,
    pub citations: DatasetStats,
    pub embeddings: DatasetStats,
}

impl FilterSummary {
    /// Aggregate from individual shard stats.
    pub fn from_shards(
        shards: &[FilterShardStats],
        shard_counts: &[(Dataset, usize, usize)], // (dataset, total, failed)
    ) -> Self {
        let mut stats = Self::default();

        // Set totals and failed counts
        for (ds, total, failed) in shard_counts {
            let ds_stats = stats.get_mut(*ds);
            ds_stats.total_shards = *total;
            ds_stats.failed_shards = *failed;
        }

        // Aggregate shard results
        for shard in shards {
            stats.get_mut(shard.dataset).add_shard(shard);
        }

        stats
    }

    fn get_mut(&mut self, dataset: Dataset) -> &mut DatasetStats {
        match dataset {
            Dataset::Abstracts => &mut self.abstracts,
            Dataset::Tldrs => &mut self.tldrs,
            Dataset::Citations => &mut self.citations,
            Dataset::Embeddings => &mut self.embeddings,
            Dataset::Papers => unreachable!("papers handled in Phase 1"),
        }
    }

    /// Format summary table as a string.
    pub fn format_table(&self) -> String {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_header(vec![
                Cell::new("Phase 2: Filtered Datasets")
                    .fg(Color::Cyan)
                    .add_attribute(comfy_table::Attribute::Bold),
                Cell::new("Rows").fg(Color::Cyan),
                Cell::new("Scanned").fg(Color::Cyan),
                Cell::new("Coverage").fg(Color::Cyan),
                Cell::new("Shards").fg(Color::Cyan),
            ]);

        self.add_dataset_row(&mut table, "abstracts", &self.abstracts);
        self.add_dataset_row(&mut table, "tldrs", &self.tldrs);
        self.add_dataset_row(&mut table, "citations", &self.citations);
        self.add_dataset_row(&mut table, "embeddings", &self.embeddings);

        format!("\n{table}")
    }

    fn add_dataset_row(&self, table: &mut Table, name: &str, ds: &DatasetStats) {
        if ds.total_shards == 0 {
            return;
        }
        table.add_row(vec![
            Cell::new(name),
            Cell::new(fmt_num(ds.rows_written)),
            Cell::new(fmt_num(ds.lines_scanned)),
            Cell::new(format!("{:.1}%", pct(ds.rows_written, ds.lines_scanned))),
            Cell::new(format!("{}/{}", ds.completed_shards, ds.total_shards)),
        ]);
    }

    /// Log minimal summary (non-TTY mode).
    pub fn log(&self) {
        let mut parts = Vec::new();
        if self.abstracts.total_shards > 0 {
            parts.push(format!(
                "abstracts={}",
                fmt_num(self.abstracts.rows_written)
            ));
        }
        if self.tldrs.total_shards > 0 {
            parts.push(format!("tldrs={}", fmt_num(self.tldrs.rows_written)));
        }
        if self.citations.total_shards > 0 {
            parts.push(format!(
                "citations={}",
                fmt_num(self.citations.rows_written)
            ));
        }
        if self.embeddings.total_shards > 0 {
            parts.push(format!(
                "embeddings={}",
                fmt_num(self.embeddings.rows_written)
            ));
        }
        log::info!("Phase 2 complete: {}", parts.join(" "));
    }
}

// =============================================================================
// Final Summary
// =============================================================================

/// Final summary combining all processing with coverage metrics.
#[derive(Debug)]
pub struct FinalSummary {
    pub paper: PaperSummary,
    pub filter: FilterSummary,
}

impl FinalSummary {
    /// Format final summary table as a string.
    pub fn format_table(&self) -> String {
        let p = &self.paper;
        let f = &self.filter;

        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_header(vec![
                Cell::new("Final Summary")
                    .fg(Color::Cyan)
                    .add_attribute(comfy_table::Attribute::Bold),
                Cell::new("Original").fg(Color::Cyan),
                Cell::new("Filtered").fg(Color::Cyan),
                Cell::new("Coverage").fg(Color::Cyan),
            ]);

        // Phase 1 outputs
        table.add_row(vec![
            Cell::new("papers").fg(Color::Green),
            Cell::new(fmt_num(p.lines_scanned)),
            Cell::new(fmt_num(p.papers_matched)).fg(Color::Green),
            Cell::new(format!("{:.1}%", pct(p.papers_matched, p.lines_scanned))),
        ]);
        table.add_row(vec![
            Cell::new("authors"),
            Cell::new("-"),
            Cell::new(fmt_num(p.authors_rows)),
            Cell::new("-"),
        ]);
        table.add_row(vec![
            Cell::new("fields"),
            Cell::new("-"),
            Cell::new(fmt_num(p.fields_rows)),
            Cell::new("-"),
        ]);

        // Phase 2 outputs
        self.add_filtered_row(&mut table, "abstracts", &f.abstracts);
        self.add_filtered_row(&mut table, "tldrs", &f.tldrs);
        self.add_filtered_row(&mut table, "citations", &f.citations);
        self.add_filtered_row(&mut table, "embeddings", &f.embeddings);

        let mut out = format!("\n{table}");

        // Data quality
        let total_failed = p.failed_shards
            + f.abstracts.failed_shards
            + f.tldrs.failed_shards
            + f.citations.failed_shards
            + f.embeddings.failed_shards;

        if p.parse_errors > 0 || total_failed > 0 {
            out.push_str(&format!(
                "\n  Parse errors: {}  |  Failed shards: {}",
                fmt_num(p.parse_errors),
                total_failed
            ));
        }

        out
    }

    fn add_filtered_row(&self, table: &mut Table, name: &str, ds: &DatasetStats) {
        if ds.total_shards == 0 {
            return;
        }
        table.add_row(vec![
            Cell::new(name),
            Cell::new(fmt_num(ds.lines_scanned)),
            Cell::new(fmt_num(ds.rows_written)),
            Cell::new(format!("{:.1}%", pct(ds.rows_written, ds.lines_scanned))),
        ]);
    }

    /// Log minimal summary (non-TTY mode).
    pub fn log(&self) {
        log::info!("s2-dataset-fetcher completed successfully");
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Format number with thousand separators.
fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Calculate percentage safely.
fn pct(part: usize, total: usize) -> f64 {
    if total > 0 {
        part as f64 / total as f64 * 100.0
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_num_thousands() {
        assert_eq!(fmt_num(0), "0");
        assert_eq!(fmt_num(999), "999");
        assert_eq!(fmt_num(1000), "1,000");
        assert_eq!(fmt_num(1234567), "1,234,567");
    }

    #[test]
    fn pct_zero_total() {
        assert_eq!(pct(100, 0), 0.0);
    }

    #[test]
    fn pct_normal() {
        assert!((pct(25, 100) - 25.0).abs() < 0.001);
    }

    #[test]
    fn paper_summary_from_shards_empty() {
        let stats = PaperSummary::from_shards(&[], 10, 2);
        assert_eq!(stats.total_shards, 10);
        assert_eq!(stats.completed_shards, 0);
        assert_eq!(stats.failed_shards, 2);
        assert_eq!(stats.lines_scanned, 0);
        assert_eq!(stats.papers_matched, 0);
    }

    #[test]
    fn paper_summary_from_shards_aggregates() {
        let shards = vec![
            PaperShardStats {
                shard_idx: 0,
                lines_scanned: 1000,
                pre_filtered: 800,
                parse_errors: 5,
                fos_excluded: 100,
                papers_matched: 95,
                authors_rows: 200,
                fields_rows: 150,
                elapsed: Duration::from_secs(10),
            },
            PaperShardStats {
                shard_idx: 1,
                lines_scanned: 2000,
                pre_filtered: 1500,
                parse_errors: 10,
                fos_excluded: 300,
                papers_matched: 190,
                authors_rows: 400,
                fields_rows: 300,
                elapsed: Duration::from_secs(15),
            },
        ];

        let stats = PaperSummary::from_shards(&shards, 5, 1);

        assert_eq!(stats.total_shards, 5);
        assert_eq!(stats.completed_shards, 2);
        assert_eq!(stats.failed_shards, 1);
        assert_eq!(stats.lines_scanned, 3000);
        assert_eq!(stats.pre_filtered, 2300);
        assert_eq!(stats.parse_errors, 15);
        assert_eq!(stats.fos_excluded, 400);
        assert_eq!(stats.papers_matched, 285);
        assert_eq!(stats.authors_rows, 600);
        assert_eq!(stats.fields_rows, 450);
        // elapsed is max of all shards
        assert_eq!(stats.elapsed, Duration::from_secs(15));
    }

    #[test]
    fn filter_summary_from_shards_aggregates() {
        let shards = vec![
            FilterShardStats {
                dataset: Dataset::Abstracts,
                shard_idx: 0,
                lines_scanned: 1000,
                corpus_excluded: 900,
                rows_written: 100,
                elapsed: Duration::from_secs(5),
            },
            FilterShardStats {
                dataset: Dataset::Abstracts,
                shard_idx: 1,
                lines_scanned: 2000,
                corpus_excluded: 1800,
                rows_written: 200,
                elapsed: Duration::from_secs(8),
            },
            FilterShardStats {
                dataset: Dataset::Tldrs,
                shard_idx: 0,
                lines_scanned: 500,
                corpus_excluded: 400,
                rows_written: 100,
                elapsed: Duration::from_secs(3),
            },
        ];

        let shard_counts = vec![(Dataset::Abstracts, 5, 1), (Dataset::Tldrs, 3, 0)];

        let stats = FilterSummary::from_shards(&shards, &shard_counts);

        // Abstracts
        assert_eq!(stats.abstracts.total_shards, 5);
        assert_eq!(stats.abstracts.completed_shards, 2);
        assert_eq!(stats.abstracts.failed_shards, 1);
        assert_eq!(stats.abstracts.lines_scanned, 3000);
        assert_eq!(stats.abstracts.corpus_excluded, 2700);
        assert_eq!(stats.abstracts.rows_written, 300);
        assert_eq!(stats.abstracts.elapsed, Duration::from_secs(8));

        // Tldrs
        assert_eq!(stats.tldrs.total_shards, 3);
        assert_eq!(stats.tldrs.completed_shards, 1);
        assert_eq!(stats.tldrs.failed_shards, 0);
        assert_eq!(stats.tldrs.lines_scanned, 500);
        assert_eq!(stats.tldrs.rows_written, 100);

        // Citations/Embeddings not in shard_counts → 0
        assert_eq!(stats.citations.total_shards, 0);
        assert_eq!(stats.embeddings.total_shards, 0);
    }

    #[test]
    fn dataset_stats_add_shard() {
        let mut ds = DatasetStats::default();
        let shard = FilterShardStats {
            dataset: Dataset::Abstracts,
            shard_idx: 0,
            lines_scanned: 1000,
            corpus_excluded: 900,
            rows_written: 100,
            elapsed: Duration::from_secs(5),
        };

        ds.add_shard(&shard);

        assert_eq!(ds.completed_shards, 1);
        assert_eq!(ds.lines_scanned, 1000);
        assert_eq!(ds.corpus_excluded, 900);
        assert_eq!(ds.rows_written, 100);
        assert_eq!(ds.elapsed, Duration::from_secs(5));
    }
}
