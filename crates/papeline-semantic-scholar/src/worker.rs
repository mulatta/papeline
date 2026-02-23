//! Shard processing — paper extraction and filtered dataset generation

use std::io::BufRead;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use arrow::array::RecordBatch;
use indicatif::ProgressBar;
use papeline_core::ShardError;
use papeline_core::accumulator::process_lines;
use papeline_core::filter;
use papeline_core::progress::{SharedProgress, fmt_num, upgrade_to_bar};
use papeline_core::retry::retry_with_backoff;
use papeline_core::shutdown_flag;
use papeline_core::sink::{ErrorFlag, LanceSink, ParquetSink};
use papeline_core::stream;

use crate::config::Config;
use crate::schema;
use papeline_core::WorkQueue;

use crate::state::{Dataset, ShardInfo};
use crate::stats::{FilterShardStats, FilterSummary, PaperShardStats, PaperSummary};
use crate::transform::{
    AbstractRow, AuthorsAccumulator, CitationRow, CitationsAccumulator, EmbeddingRow,
    EmbeddingsAccumulator, FieldsAccumulator, PaperRow, PapersAccumulator, TextAccumulator,
    TldrRow,
};

/// Lightweight probe for early corpus ID filtering (avoids full deserialization)
#[derive(serde::Deserialize)]
struct CorpusIdProbe {
    #[serde(default)]
    corpusid: i64,
}

/// Lightweight probe for citation corpus ID pair filtering
#[derive(serde::Deserialize)]
struct CitationIdProbe {
    #[serde(default)]
    citingcorpusid: i64,
    #[serde(default)]
    citedcorpusid: i64,
}

/// Lance channel info passed to Phase 2 workers
pub struct LanceWriterHandle {
    pub sender: SyncSender<RecordBatch>,
    pub error_flag: ErrorFlag,
}

impl std::fmt::Debug for LanceWriterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceWriterHandle").finish_non_exhaustive()
    }
}

/// Initial capacity for per-line JSON read buffer (typical S2 JSON line: 1–5KB)
const LINE_BUF_CAPACITY: usize = 4096;

/// Phase 1: process papers, collect corpus IDs, return (failed_count, stats)
pub fn process_paper_shards(
    paper_urls: &[String],
    release_dir: &Path,
    config: &Config,
    progress: &SharedProgress,
) -> (usize, PaperSummary) {
    let total_shards = paper_urls.len();
    let shards: Vec<ShardInfo> = paper_urls
        .iter()
        .enumerate()
        .map(|(idx, url)| ShardInfo {
            dataset: Dataset::Papers,
            shard_idx: idx,
            url: url.clone(),
        })
        .collect();

    let queue = WorkQueue::filtered(shards, |s| {
        let filename = format!("{}_{:04}.parquet", s.dataset, s.shard_idx);
        let path = release_dir.join(&filename);
        if papeline_core::sink::is_valid_parquet(&path) {
            log::debug!(
                "Shard {}_{:04} already completed, skipping",
                s.dataset,
                s.shard_idx
            );
            false
        } else {
            true
        }
    });
    if queue.total() == 0 {
        log::info!("All paper shards already completed");
        // Ensure corpus_ids.bin exists (may need recovery)
        let corpus_ids_path = release_dir.join("corpus_ids.bin");
        if !corpus_ids_path.exists() {
            log::debug!("Recovering corpus IDs from completed parquet files...");
            let mut ids = filter::recover_corpus_ids(release_dir);
            if let Err(e) = filter::save_corpus_ids(&mut ids, &corpus_ids_path) {
                log::error!("Failed to save recovered corpus IDs: {e}");
            }
        }
        return (0, PaperSummary::default());
    }

    let corpus_ids: Mutex<Vec<i64>> = Mutex::new(Vec::new());
    let shard_stats: Mutex<Vec<PaperShardStats>> = Mutex::new(Vec::new());
    let failed = Mutex::new(0usize);
    let is_tty = progress.is_tty();
    let stagger_slot = AtomicUsize::new(0);

    rayon::scope(|s| {
        for _ in 0..config.workers {
            s.spawn(|_| {
                papeline_core::stream::stagger(stagger_slot.fetch_add(1, Ordering::Relaxed));
                while let Some(shard) = queue.next() {
                    if shutdown_flag().load(Ordering::Relaxed) {
                        break;
                    }
                    let shard_name = format!("papers_{:04}", shard.shard_idx);
                    let pb = progress.shard_bar(&shard_name);
                    pb.set_message("connecting...");

                    match process_paper_shard(
                        shard,
                        release_dir,
                        &config.domains,
                        config.zstd_level,
                        &pb,
                    ) {
                        Ok((ids, stats)) => {
                            pb.finish_and_clear();
                            if !is_tty {
                                stats.log();
                            }
                            corpus_ids
                                .lock()
                                .expect("worker thread panicked")
                                .extend(ids);
                            shard_stats
                                .lock()
                                .expect("worker thread panicked")
                                .push(stats);
                        }
                        Err(()) => {
                            pb.finish_and_clear();
                            *failed.lock().expect("worker thread panicked") += 1;
                        }
                    }
                }
            });
        }
    });

    // Save corpus_ids.bin
    let mut ids = corpus_ids.into_inner().unwrap();

    // Also recover from any previously completed shards (in case of partial restart)
    let mut recovered = filter::recover_corpus_ids(release_dir);
    ids.append(&mut recovered);

    let corpus_ids_path = release_dir.join("corpus_ids.bin");
    if let Err(e) = filter::save_corpus_ids(&mut ids, &corpus_ids_path) {
        log::error!("Failed to save corpus IDs: {e}");
    }

    let failed_count = failed.into_inner().unwrap();
    let stats_vec = shard_stats.into_inner().unwrap();
    let summary = PaperSummary::from_shards(&stats_vec, total_shards, failed_count);

    (failed_count, summary)
}

/// Phase 2: process filtered datasets, return (failed_count, stats)
pub fn process_filtered_shards(
    shards: Vec<(Dataset, usize, String)>,
    corpus_ids: &filter::CorpusIdSet,
    release_dir: &Path,
    config: &Config,
    lance: Option<&LanceWriterHandle>,
    shard_counts: &[(Dataset, usize)], // (dataset, total_shards)
    progress: &SharedProgress,
) -> (usize, FilterSummary) {
    let shard_infos: Vec<ShardInfo> = shards
        .into_iter()
        .map(|(ds, idx, url)| ShardInfo {
            dataset: ds,
            shard_idx: idx,
            url,
        })
        .collect();

    let queue = WorkQueue::filtered(shard_infos, |s| {
        if s.dataset == Dataset::Embeddings {
            let done = release_dir.join("embeddings.lance.done");
            if done.exists() {
                log::debug!("embeddings already completed (lance), skipping");
                return false;
            }
            return true; // always reprocess (overwrite mode)
        }
        let filename = format!("{}_{:04}.parquet", s.dataset, s.shard_idx);
        let path = release_dir.join(&filename);
        if papeline_core::sink::is_valid_parquet(&path) {
            log::debug!(
                "Shard {}_{:04} already completed, skipping",
                s.dataset,
                s.shard_idx
            );
            false
        } else {
            true
        }
    });
    if queue.total() == 0 {
        log::info!("All phase 2 shards already completed");
        return (0, FilterSummary::default());
    }

    let failed = Mutex::new(0usize);
    let shard_stats: Mutex<Vec<FilterShardStats>> = Mutex::new(Vec::new());
    let is_tty = progress.is_tty();
    let stagger_slot2 = AtomicUsize::new(0);

    rayon::scope(|s| {
        for _ in 0..config.workers {
            s.spawn(|_| {
                papeline_core::stream::stagger(stagger_slot2.fetch_add(1, Ordering::Relaxed));
                while let Some(shard) = queue.next() {
                    if shutdown_flag().load(Ordering::Relaxed) {
                        break;
                    }
                    let shard_name = format!("{}_{:04}", shard.dataset, shard.shard_idx);
                    let pb = progress.shard_bar(&shard_name);
                    pb.set_message("connecting...");

                    match process_filtered_shard(
                        shard,
                        corpus_ids,
                        release_dir,
                        lance,
                        config.zstd_level,
                        &pb,
                    ) {
                        Ok(stats) => {
                            pb.finish_and_clear();
                            if !is_tty {
                                stats.log();
                            }
                            shard_stats
                                .lock()
                                .expect("worker thread panicked")
                                .push(stats);
                        }
                        Err(()) => {
                            pb.finish_and_clear();
                            *failed.lock().expect("worker thread panicked") += 1;
                        }
                    }
                }
            });
        }
    });

    let failed_count = failed.into_inner().unwrap();
    let stats_vec = shard_stats.into_inner().unwrap();

    // Build shard_counts with failed counts per dataset
    let shard_counts_with_failed: Vec<(Dataset, usize, usize)> = shard_counts
        .iter()
        .map(|(ds, total)| {
            let completed = stats_vec.iter().filter(|s| s.dataset == *ds).count();
            let ds_failed = total.saturating_sub(completed);
            (*ds, *total, ds_failed)
        })
        .collect();

    let summary = FilterSummary::from_shards(&stats_vec, &shard_counts_with_failed);

    (failed_count, summary)
}

// === Shard processing with retries ===

/// Process a single papers shard: filter by FoS domains, decompose into 3 parquet files
fn process_paper_shard(
    shard: &ShardInfo,
    output_dir: &Path,
    domains: &[String],
    zstd_level: i32,
    pb: &ProgressBar,
) -> Result<(Vec<i64>, PaperShardStats), ()> {
    let label = format!("papers_{:04}", shard.shard_idx);
    retry_with_backoff(&label, pb, || {
        attempt_paper_shard(shard, output_dir, domains, zstd_level, pb)
    })
    .map_err(|e| {
        if e.is_url_expired() {
            log::error!("URL expired. Delete .cache/urls and re-run to refresh.");
        }
    })
}

// === PaperPipeline: composite struct for Phase 1 multi-sink processing ===

/// Manages 3 accumulators + 3 sinks for Phase 1 paper processing.
/// Papers, authors, and fields are written to separate Parquet files from a single pass.
struct PaperPipeline {
    papers: (PapersAccumulator, ParquetSink),
    authors: (AuthorsAccumulator, ParquetSink),
    fields: (FieldsAccumulator, ParquetSink),
}

impl PaperPipeline {
    fn new(shard_idx: usize, output_dir: &Path, zstd_level: i32) -> Result<Self, ShardError> {
        Ok(Self {
            papers: (
                PapersAccumulator::new(),
                ParquetSink::new(
                    "papers",
                    shard_idx,
                    output_dir,
                    schema::papers(),
                    zstd_level,
                )
                .map_err(ShardError::Io)?,
            ),
            authors: (
                AuthorsAccumulator::new(),
                ParquetSink::new(
                    "paper_authors",
                    shard_idx,
                    output_dir,
                    schema::paper_authors(),
                    zstd_level,
                )
                .map_err(ShardError::Io)?,
            ),
            fields: (
                FieldsAccumulator::new(),
                ParquetSink::new(
                    "paper_fields",
                    shard_idx,
                    output_dir,
                    schema::paper_fields(),
                    zstd_level,
                )
                .map_err(ShardError::Io)?,
            ),
        })
    }

    /// Push a row into all 3 accumulators, flushing full batches.
    /// Returns (authors_count, fields_count) for the row.
    fn push(&mut self, row: PaperRow) -> Result<(usize, usize), ShardError> {
        let authors_count = row.authors.len();
        let fields_count = row.s2fieldsofstudy.len();

        // authors/fields borrow, papers takes ownership (no String clones)
        self.authors.0.push(&row);
        self.fields.0.push(&row);
        self.papers.0.push(row);

        Self::flush_if_full(&mut self.papers.0, &mut self.papers.1)?;
        Self::flush_if_full(&mut self.authors.0, &mut self.authors.1)?;
        Self::flush_if_full(&mut self.fields.0, &mut self.fields.1)?;

        Ok((authors_count, fields_count))
    }

    fn flush_if_full(
        acc: &mut impl AccumulatorFlush,
        sink: &mut ParquetSink,
    ) -> Result<(), ShardError> {
        if acc.should_flush() {
            let batch = acc
                .flush()
                .map_err(|e| ShardError::Io(std::io::Error::other(e)))?;
            sink.write_batch(&batch).map_err(ShardError::Io)?;
        }
        Ok(())
    }

    /// Flush remaining rows and finalize all 3 sinks.
    fn finalize(mut self) -> Result<(), ShardError> {
        Self::flush_remaining(&mut self.papers.0, &mut self.papers.1)?;
        Self::flush_remaining(&mut self.authors.0, &mut self.authors.1)?;
        Self::flush_remaining(&mut self.fields.0, &mut self.fields.1)?;
        self.papers.1.finalize().map_err(ShardError::Io)?;
        self.authors.1.finalize().map_err(ShardError::Io)?;
        self.fields.1.finalize().map_err(ShardError::Io)?;
        Ok(())
    }

    fn flush_remaining(
        acc: &mut impl AccumulatorFlush,
        sink: &mut ParquetSink,
    ) -> Result<(), ShardError> {
        if !acc.is_flush_empty() {
            let batch = acc
                .flush()
                .map_err(|e| ShardError::Io(std::io::Error::other(e)))?;
            sink.write_batch(&batch).map_err(ShardError::Io)?;
        }
        Ok(())
    }
}

/// Helper trait to unify flush operations across different accumulator types.
/// Phase 1 accumulators don't implement core Accumulator trait (borrow vs owned push).
trait AccumulatorFlush {
    fn should_flush(&self) -> bool;
    fn is_flush_empty(&self) -> bool;
    fn flush(&mut self) -> Result<RecordBatch, arrow::error::ArrowError>;
}

impl AccumulatorFlush for PapersAccumulator {
    fn should_flush(&self) -> bool {
        self.is_full()
    }
    fn is_flush_empty(&self) -> bool {
        self.is_empty()
    }
    fn flush(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        self.take_batch()
    }
}

impl AccumulatorFlush for AuthorsAccumulator {
    fn should_flush(&self) -> bool {
        self.is_full()
    }
    fn is_flush_empty(&self) -> bool {
        self.is_empty()
    }
    fn flush(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        self.take_batch()
    }
}

impl AccumulatorFlush for FieldsAccumulator {
    fn should_flush(&self) -> bool {
        self.is_full()
    }
    fn is_flush_empty(&self) -> bool {
        self.is_empty()
    }
    fn flush(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        self.take_batch()
    }
}

fn attempt_paper_shard(
    shard: &ShardInfo,
    output_dir: &Path,
    domains: &[String],
    zstd_level: i32,
    pb: &ProgressBar,
) -> Result<(Vec<i64>, PaperShardStats), ShardError> {
    let start = Instant::now();
    let (mut reader, counter, total_bytes) =
        stream::open_gzip_reader(&shard.url).map_err(ShardError::Stream)?;

    if let Some(total) = total_bytes {
        upgrade_to_bar(pb, total);
    }
    pb.set_message("filtering...");

    let mut pipeline = PaperPipeline::new(shard.shard_idx, output_dir, zstd_level)?;
    let mut corpus_ids = Vec::new();

    // Pre-compute quoted needles for cheap substring pre-filter
    let needles: Vec<String> = domains.iter().map(|d| format!("\"{d}\"")).collect();

    let mut buf = String::with_capacity(LINE_BUF_CAPACITY);
    let mut lines_scanned = 0usize;
    let mut pre_filtered = 0usize;
    let mut parse_errors = 0usize;
    let mut fos_excluded = 0usize;
    let mut authors_rows = 0usize;
    let mut fields_rows = 0usize;

    const UPDATE_INTERVAL: usize = 10_000;

    loop {
        buf.clear();
        if reader.read_line(&mut buf).map_err(ShardError::Io)? == 0 {
            break;
        }
        lines_scanned += 1;

        if lines_scanned.is_multiple_of(UPDATE_INTERVAL) {
            pb.set_position(counter.load(std::sync::atomic::Ordering::Relaxed));
            let match_pct = if lines_scanned > 0 {
                corpus_ids.len() as f64 / lines_scanned as f64 * 100.0
            } else {
                0.0
            };
            pb.set_message(format!(
                "{} matched ({:.1}%)",
                fmt_num(corpus_ids.len()),
                match_pct
            ));
        }

        // Cheap substring pre-filter
        if !needles.iter().any(|n| buf.contains(n.as_str())) {
            pre_filtered += 1;
            continue;
        }

        let row: PaperRow = match sonic_rs::from_str(&buf) {
            Ok(v) => v,
            Err(e) => {
                if parse_errors < 5 {
                    log::debug!(
                        "papers_{:04}: JSON parse error: {e}\n  line[..200]: {}",
                        shard.shard_idx,
                        &buf[..buf.len().min(200)]
                    );
                }
                parse_errors += 1;
                continue;
            }
        };

        if !row.matches_domains(domains) {
            fos_excluded += 1;
            continue;
        }

        corpus_ids.push(row.corpusid);
        let (ac, fc) = pipeline.push(row)?;
        authors_rows += ac;
        fields_rows += fc;
    }

    pb.set_message("writing...");
    pipeline.finalize()?;

    let stats = PaperShardStats {
        shard_idx: shard.shard_idx,
        lines_scanned,
        pre_filtered,
        parse_errors,
        fos_excluded,
        papers_matched: corpus_ids.len(),
        authors_rows,
        fields_rows,
        elapsed: start.elapsed(),
    };

    Ok((corpus_ids, stats))
}

/// Process a filtered shard (abstracts, tldrs, citations, embeddings)
fn process_filtered_shard(
    shard: &ShardInfo,
    corpus_ids: &filter::CorpusIdSet,
    output_dir: &Path,
    lance: Option<&LanceWriterHandle>,
    zstd_level: i32,
    pb: &ProgressBar,
) -> Result<FilterShardStats, ()> {
    let label = format!("{}_{:04}", shard.dataset, shard.shard_idx);
    retry_with_backoff(&label, pb, || {
        attempt_filtered_shard(shard, corpus_ids, output_dir, lance, zstd_level, pb)
    })
    .map_err(|e| {
        if e.is_url_expired() {
            log::error!("URL expired. Delete .cache/urls and re-run to refresh.");
        }
    })
}

fn attempt_filtered_shard(
    shard: &ShardInfo,
    corpus_ids: &filter::CorpusIdSet,
    output_dir: &Path,
    lance: Option<&LanceWriterHandle>,
    zstd_level: i32,
    pb: &ProgressBar,
) -> Result<FilterShardStats, ShardError> {
    let start = Instant::now();
    let (mut reader, counter, total_bytes) =
        stream::open_gzip_reader(&shard.url).map_err(ShardError::Stream)?;

    // Upgrade spinner to progress bar with total size
    if let Some(total) = total_bytes {
        upgrade_to_bar(pb, total);
    }
    pb.set_message("filtering...");

    let line_stats = match shard.dataset {
        Dataset::Papers => unreachable!("papers are processed in Phase 1"),

        Dataset::Abstracts => {
            let mut sink = ParquetSink::new(
                "abstracts",
                shard.shard_idx,
                output_dir,
                schema::abstracts(),
                zstd_level,
            )
            .map_err(ShardError::Io)?;
            let mut acc = TextAccumulator::new(schema::abstracts().clone());
            let stats = process_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let probe: CorpusIdProbe = sonic_rs::from_str(line).ok()?;
                    if !corpus_ids.contains(probe.corpusid) {
                        return None;
                    }
                    let row: AbstractRow = sonic_rs::from_str(line).ok()?;
                    Some((row.corpusid, row.text))
                },
                pb,
            )
            .map_err(ShardError::Io)?;
            sink.finalize().map_err(ShardError::Io)?;
            stats
        }

        Dataset::Tldrs => {
            let mut sink = ParquetSink::new(
                "tldrs",
                shard.shard_idx,
                output_dir,
                schema::tldrs(),
                zstd_level,
            )
            .map_err(ShardError::Io)?;
            let mut acc = TextAccumulator::new(schema::tldrs().clone());
            let stats = process_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let probe: CorpusIdProbe = sonic_rs::from_str(line).ok()?;
                    if !corpus_ids.contains(probe.corpusid) {
                        return None;
                    }
                    let row: TldrRow = sonic_rs::from_str(line).ok()?;
                    Some((row.corpusid, row.text))
                },
                pb,
            )
            .map_err(ShardError::Io)?;
            sink.finalize().map_err(ShardError::Io)?;
            stats
        }

        Dataset::Citations => {
            let mut sink = ParquetSink::new(
                "citations",
                shard.shard_idx,
                output_dir,
                schema::citations(),
                zstd_level,
            )
            .map_err(ShardError::Io)?;
            let mut acc = CitationsAccumulator::new();
            let stats = process_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let probe: CitationIdProbe = sonic_rs::from_str(line).ok()?;
                    if !corpus_ids.contains(probe.citingcorpusid)
                        || !corpus_ids.contains(probe.citedcorpusid)
                    {
                        return None;
                    }
                    sonic_rs::from_str::<CitationRow>(line).ok()
                },
                pb,
            )
            .map_err(ShardError::Io)?;
            sink.finalize().map_err(ShardError::Io)?;
            stats
        }

        Dataset::Embeddings => {
            let lance = lance.expect("embeddings requires lance channel");
            let mut sink = LanceSink::new(lance.sender.clone(), lance.error_flag.clone());
            let mut acc = EmbeddingsAccumulator::new();
            let stats = process_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let probe: CorpusIdProbe = sonic_rs::from_str(line).ok()?;
                    if !corpus_ids.contains(probe.corpusid) {
                        return None;
                    }
                    sonic_rs::from_str::<EmbeddingRow>(line).ok()
                },
                pb,
            )
            .map_err(ShardError::Io)?;
            sink.finalize().map_err(ShardError::Io)?;
            stats
        }
    };

    pb.set_message("done");
    Ok(FilterShardStats {
        dataset: shard.dataset,
        shard_idx: shard.shard_idx,
        lines_scanned: line_stats.lines_scanned,
        corpus_excluded: line_stats
            .lines_scanned
            .saturating_sub(line_stats.rows_written),
        rows_written: line_stats.rows_written,
        elapsed: start.elapsed(),
    })
}
