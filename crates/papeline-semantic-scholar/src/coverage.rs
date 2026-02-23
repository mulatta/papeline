//! Coverage analysis for processed datasets.
//!
//! Phase 3: After all workers complete, compute:
//! 1. Cross-dataset coverage: papers `corpus_id` vs other datasets
//! 2. Field completeness: papers nullable field fill rates

use std::collections::HashSet;
use std::path::Path;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use comfy_table::{Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use papeline_core::progress::fmt_num;

/// Coverage statistics for cross-dataset analysis
#[derive(Debug, Default)]
pub struct CoverageStats {
    pub papers_count: usize,
    pub abstracts: Option<DatasetCoverage>,
    pub tldrs: Option<DatasetCoverage>,
    pub citations: Option<DatasetCoverage>,
    pub embeddings: Option<DatasetCoverage>,
}

/// Coverage for a single dataset against papers
#[derive(Debug, Clone)]
pub struct DatasetCoverage {
    /// Number of `corpus_id`s matching papers
    pub matched: usize,
    /// Total unique `corpus_id`s in this dataset
    pub total: usize,
    /// Coverage percentage (`matched` / `papers_count` * 100)
    pub coverage_pct: f64,
}

/// Field completeness statistics for papers
#[derive(Debug, Default)]
pub struct FieldCompleteness {
    pub total_rows: usize,
    pub fields: Vec<FieldStat>,
}

/// Individual field statistics
#[derive(Debug, Clone)]
pub struct FieldStat {
    pub name: &'static str,
    pub present: usize,
    pub pct: f64,
}

impl CoverageStats {
    /// Compute coverage statistics from output directory
    ///
    /// Files are stored as `{dataset}_{shard}.parquet` directly in output_dir.
    pub fn compute(output_dir: &Path) -> anyhow::Result<Self> {
        // Check if any papers_*.parquet files exist
        let papers_pattern = output_dir.join("papers_*.parquet");
        let pattern_str = papers_pattern
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("non-UTF8 path: {:?}", output_dir))?;
        let papers_files: Vec<_> = glob::glob(pattern_str)?.filter_map(Result::ok).collect();

        if papers_files.is_empty() {
            anyhow::bail!("no papers_*.parquet files found");
        }

        log::debug!(
            "Loading papers corpus_ids from {} files...",
            papers_files.len()
        );
        let papers_ids = read_corpus_ids_from_files(&papers_files, "corpusid")?;
        let papers_count = papers_ids.len();
        log::debug!("papers: {} unique corpus_ids", papers_count);

        // Load other datasets
        let abstracts = load_dataset_coverage(output_dir, "abstracts", "corpusid", &papers_ids);
        let tldrs = load_dataset_coverage(output_dir, "tldrs", "corpusid", &papers_ids);
        let citations =
            load_dataset_coverage(output_dir, "citations", "citingcorpusid", &papers_ids);

        // For embeddings in lance format, read count from .done file
        let embeddings_done = output_dir.join("embeddings.lance.done");
        let embeddings = if embeddings_done.exists() {
            let count_str = std::fs::read_to_string(&embeddings_done).unwrap_or_default();
            let total: usize = count_str.trim().parse().unwrap_or(0);
            // Lance doesn't give us corpus_ids easily, so we approximate
            // Assume all embeddings match papers (they're filtered by corpus_id)
            Some(DatasetCoverage {
                matched: total.min(papers_count),
                total,
                coverage_pct: if papers_count > 0 {
                    total.min(papers_count) as f64 / papers_count as f64 * 100.0
                } else {
                    0.0
                },
            })
        } else {
            None
        };

        Ok(Self {
            papers_count,
            abstracts,
            tldrs,
            citations,
            embeddings,
        })
    }

    /// Format coverage table as a string.
    pub fn format_table(&self) -> String {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_header(vec![
                Cell::new("Coverage Analysis")
                    .fg(Color::Cyan)
                    .add_attribute(comfy_table::Attribute::Bold),
                Cell::new("Matched").fg(Color::Cyan),
                Cell::new("Total").fg(Color::Cyan),
                Cell::new("Coverage").fg(Color::Cyan),
            ]);

        table.add_row(vec![
            Cell::new("papers (base)"),
            Cell::new(fmt_num(self.papers_count)),
            Cell::new("-"),
            Cell::new("100%"),
        ]);

        if let Some(ref c) = self.abstracts {
            self.add_coverage_row(&mut table, "abstracts", c);
        }
        if let Some(ref c) = self.tldrs {
            self.add_coverage_row(&mut table, "tldrs", c);
        }
        if let Some(ref c) = self.citations {
            self.add_coverage_row(&mut table, "citations (citing)", c);
        }
        if let Some(ref c) = self.embeddings {
            self.add_coverage_row(&mut table, "embeddings", c);
        }

        format!("\n{table}")
    }

    fn add_coverage_row(&self, table: &mut Table, name: &str, c: &DatasetCoverage) {
        let color = if c.coverage_pct >= 90.0 {
            Color::Green
        } else if c.coverage_pct >= 70.0 {
            Color::Yellow
        } else {
            Color::Red
        };

        table.add_row(vec![
            Cell::new(name),
            Cell::new(fmt_num(c.matched)),
            Cell::new(fmt_num(c.total)),
            Cell::new(format!("{:.1}%", c.coverage_pct)).fg(color),
        ]);
    }

    /// Log coverage summary (non-TTY mode)
    pub fn log(&self) {
        let mut parts = vec![format!("papers={}", fmt_num(self.papers_count))];

        if let Some(ref c) = self.abstracts {
            parts.push(format!("abstracts={:.1}%", c.coverage_pct));
        }
        if let Some(ref c) = self.tldrs {
            parts.push(format!("tldrs={:.1}%", c.coverage_pct));
        }
        if let Some(ref c) = self.citations {
            parts.push(format!("citations={:.1}%", c.coverage_pct));
        }
        if let Some(ref c) = self.embeddings {
            parts.push(format!("embeddings={:.1}%", c.coverage_pct));
        }

        log::info!("Coverage: {}", parts.join(" "));
    }
}

impl FieldCompleteness {
    /// Compute field completeness from papers parquet files
    ///
    /// Files are stored as `papers_*.parquet` directly in output_dir.
    pub fn compute(output_dir: &Path) -> anyhow::Result<Self> {
        let mut total = 0usize;
        let mut year_present = 0usize;
        let mut date_present = 0usize;
        let mut venue_nonempty = 0usize;
        let mut venueid_present = 0usize;
        let mut journal_present = 0usize;
        let mut doi_present = 0usize;
        let mut arxiv_present = 0usize;
        let mut pubmed_present = 0usize;
        let mut openaccess_true = 0usize;

        let pattern = output_dir.join("papers_*.parquet");
        let pattern_str = pattern
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("non-UTF8 path: {:?}", output_dir))?;
        let files: Vec<_> = glob::glob(pattern_str)?.filter_map(Result::ok).collect();

        if files.is_empty() {
            anyhow::bail!("no papers_*.parquet files found");
        }

        for path in &files {
            let file = std::fs::File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;

            for batch in reader {
                let batch = batch?;
                let rows = batch.num_rows();
                total += rows;

                // year: Option<i32> - count non-null
                if let Some(col) = batch.column_by_name("year") {
                    year_present += rows - col.null_count();
                }

                // publicationdate: Option<String> - count non-null
                if let Some(col) = batch.column_by_name("publicationdate") {
                    date_present += rows - col.null_count();
                }

                // venue: String - count non-empty
                if let Some(col) = batch.column_by_name("venue") {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        venue_nonempty += arr
                            .iter()
                            .filter(|s| s.is_some_and(|v| !v.is_empty()))
                            .count();
                    }
                }

                // publicationvenueid: Option<String> - count non-null
                if let Some(col) = batch.column_by_name("publicationvenueid") {
                    venueid_present += rows - col.null_count();
                }

                // journal_name: Option<String> - count non-null
                if let Some(col) = batch.column_by_name("journal_name") {
                    journal_present += rows - col.null_count();
                }

                // doi: Option<String> - count non-null
                if let Some(col) = batch.column_by_name("doi") {
                    doi_present += rows - col.null_count();
                }

                // arxiv: Option<String> - count non-null
                if let Some(col) = batch.column_by_name("arxiv") {
                    arxiv_present += rows - col.null_count();
                }

                // pubmed: Option<String> - count non-null
                if let Some(col) = batch.column_by_name("pubmed") {
                    pubmed_present += rows - col.null_count();
                }

                // isopenaccess: bool - count true
                if let Some(col) = batch.column_by_name("isopenaccess") {
                    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        openaccess_true += arr.iter().filter(|v| v == &Some(true)).count();
                    }
                }
            }
        }

        let pct = |n: usize| -> f64 {
            if total > 0 {
                n as f64 / total as f64 * 100.0
            } else {
                0.0
            }
        };

        Ok(Self {
            total_rows: total,
            fields: vec![
                FieldStat {
                    name: "year",
                    present: year_present,
                    pct: pct(year_present),
                },
                FieldStat {
                    name: "publicationdate",
                    present: date_present,
                    pct: pct(date_present),
                },
                FieldStat {
                    name: "venue",
                    present: venue_nonempty,
                    pct: pct(venue_nonempty),
                },
                FieldStat {
                    name: "publicationvenueid",
                    present: venueid_present,
                    pct: pct(venueid_present),
                },
                FieldStat {
                    name: "journal_name",
                    present: journal_present,
                    pct: pct(journal_present),
                },
                FieldStat {
                    name: "doi",
                    present: doi_present,
                    pct: pct(doi_present),
                },
                FieldStat {
                    name: "arxiv",
                    present: arxiv_present,
                    pct: pct(arxiv_present),
                },
                FieldStat {
                    name: "pubmed",
                    present: pubmed_present,
                    pct: pct(pubmed_present),
                },
                FieldStat {
                    name: "isopenaccess",
                    present: openaccess_true,
                    pct: pct(openaccess_true),
                },
            ],
        })
    }

    /// Format field completeness table as a string.
    pub fn format_table(&self) -> String {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_header(vec![
                Cell::new(format!(
                    "Field Completeness (n={})",
                    fmt_num(self.total_rows)
                ))
                .fg(Color::Cyan)
                .add_attribute(comfy_table::Attribute::Bold),
                Cell::new("Present").fg(Color::Cyan),
                Cell::new("Coverage").fg(Color::Cyan),
            ]);

        for f in &self.fields {
            let color = if f.pct >= 90.0 {
                Color::Green
            } else if f.pct >= 50.0 {
                Color::Yellow
            } else {
                Color::Red
            };

            table.add_row(vec![
                Cell::new(f.name),
                Cell::new(fmt_num(f.present)),
                Cell::new(format!("{:.1}%", f.pct)).fg(color),
            ]);
        }

        format!("\n{table}")
    }

    /// Log field completeness (non-TTY mode)
    pub fn log(&self) {
        let parts: Vec<String> = self
            .fields
            .iter()
            .filter(|f| f.pct >= 10.0) // Only log fields with >10% coverage
            .map(|f| format!("{}={:.0}%", f.name, f.pct))
            .collect();

        log::info!("Fields: {}", parts.join(" "));
    }
}

/// Load dataset coverage by glob pattern
fn load_dataset_coverage(
    output_dir: &Path,
    dataset: &str,
    column_name: &str,
    papers_ids: &HashSet<i64>,
) -> Option<DatasetCoverage> {
    let pattern = output_dir.join(format!("{dataset}_*.parquet"));
    let files: Vec<_> = glob::glob(pattern.to_str()?)
        .ok()?
        .filter_map(Result::ok)
        .collect();

    if files.is_empty() {
        return None;
    }

    log::debug!("Loading {dataset} corpus_ids from {} files...", files.len());
    match read_corpus_ids_from_files(&files, column_name) {
        Ok(ids) => Some(compute_coverage(papers_ids, &ids)),
        Err(e) => {
            log::warn!("{dataset}: failed to read corpus_ids: {e}");
            None
        }
    }
}

/// Read corpus_ids from a list of parquet files
fn read_corpus_ids_from_files(
    files: &[std::path::PathBuf],
    column_name: &str,
) -> anyhow::Result<HashSet<i64>> {
    let mut ids = HashSet::new();

    for path in files {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch in reader {
            let batch = batch?;
            if let Some(col) = batch.column_by_name(column_name) {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    ids.extend(arr.values().iter().copied());
                }
            }
        }
    }

    Ok(ids)
}

/// Compute coverage of other dataset against papers
fn compute_coverage(papers: &HashSet<i64>, other: &HashSet<i64>) -> DatasetCoverage {
    let matched = papers.intersection(other).count();
    DatasetCoverage {
        matched,
        total: other.len(),
        coverage_pct: if papers.is_empty() {
            0.0
        } else {
            matched as f64 / papers.len() as f64 * 100.0
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_coverage_empty() {
        let papers = HashSet::new();
        let other = HashSet::from([1, 2, 3]);
        let c = compute_coverage(&papers, &other);
        assert_eq!(c.matched, 0);
        assert_eq!(c.coverage_pct, 0.0);
    }

    #[test]
    fn compute_coverage_full() {
        let papers = HashSet::from([1, 2, 3]);
        let other = HashSet::from([1, 2, 3]);
        let c = compute_coverage(&papers, &other);
        assert_eq!(c.matched, 3);
        assert!((c.coverage_pct - 100.0).abs() < 0.01);
    }

    #[test]
    fn compute_coverage_partial() {
        let papers = HashSet::from([1, 2, 3, 4]);
        let other = HashSet::from([1, 2, 5, 6]);
        let c = compute_coverage(&papers, &other);
        assert_eq!(c.matched, 2);
        assert!((c.coverage_pct - 50.0).abs() < 0.01);
    }
}
