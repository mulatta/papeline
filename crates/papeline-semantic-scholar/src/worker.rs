//! Shard processing — paper extraction and filtered dataset generation

use std::io::BufRead;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use arrow::array::RecordBatch;
use indicatif::ProgressBar;
use papeline_core::filter;
use papeline_core::progress::{SharedProgress, fmt_num, upgrade_to_bar};
use papeline_core::shutdown_flag;
use papeline_core::sink::{ErrorFlag, LanceSink, ParquetSink};
use papeline_core::stream::{self, GzipReader};

use crate::config::Config;
use crate::schema;
use crate::state::{Dataset, ShardInfo, WorkQueue};
use crate::stats::{FilterShardStats, FilterSummary, PaperShardStats, PaperSummary};
use crate::transform::{
    AbstractRow, Accumulator, AuthorsAccumulator, CitationRow, CitationsAccumulator, EmbeddingRow,
    EmbeddingsAccumulator, FieldsAccumulator, PaperRow, PapersAccumulator, TextAccumulator,
    TldrRow,
};

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

const MAX_RETRIES: u32 = 3;

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

    let queue = WorkQueue::new(shards, release_dir);
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

    rayon::scope(|s| {
        for _ in 0..config.workers {
            s.spawn(|_| {
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

    let queue = WorkQueue::new(shard_infos, release_dir);
    if queue.total() == 0 {
        log::info!("All phase 2 shards already completed");
        return (0, FilterSummary::default());
    }

    let failed = Mutex::new(0usize);
    let shard_stats: Mutex<Vec<FilterShardStats>> = Mutex::new(Vec::new());
    let is_tty = progress.is_tty();

    rayon::scope(|s| {
        for _ in 0..config.workers {
            s.spawn(|_| {
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
    let mut attempt = 0;
    loop {
        match attempt_paper_shard(shard, output_dir, domains, zstd_level, pb) {
            Ok(result) => return Ok(result),
            Err(e) if attempt < MAX_RETRIES && e.is_retryable() => {
                attempt += 1;
                pb.set_message(format!("retry {attempt}/{MAX_RETRIES}..."));
                log::debug!(
                    "papers_{:04}: attempt {}/{} failed: {e}, retrying...",
                    shard.shard_idx,
                    attempt,
                    MAX_RETRIES
                );
                std::thread::sleep(backoff_duration(attempt));
            }
            Err(e) => {
                log::error!("papers_{:04}: failed permanently: {e}", shard.shard_idx);
                if e.is_url_expired() {
                    log::error!("URL expired. Delete .cache/urls and re-run to refresh.");
                }
                return Err(());
            }
        }
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

    // Upgrade spinner to progress bar with total size
    if let Some(total) = total_bytes {
        upgrade_to_bar(pb, total);
    }
    pb.set_message("filtering...");

    let mut papers_sink = ParquetSink::new(
        "papers",
        shard.shard_idx,
        output_dir,
        schema::papers(),
        zstd_level,
    )
    .map_err(ShardError::Io)?;
    let mut authors_sink = ParquetSink::new(
        "paper_authors",
        shard.shard_idx,
        output_dir,
        schema::paper_authors(),
        zstd_level,
    )
    .map_err(ShardError::Io)?;
    let mut fields_sink = ParquetSink::new(
        "paper_fields",
        shard.shard_idx,
        output_dir,
        schema::paper_fields(),
        zstd_level,
    )
    .map_err(ShardError::Io)?;

    let mut papers_acc = PapersAccumulator::new();
    let mut authors_acc = AuthorsAccumulator::new();
    let mut fields_acc = FieldsAccumulator::new();
    let mut corpus_ids = Vec::new();

    // Pre-compute quoted needles for cheap substring pre-filter.
    // Avoids per-line `format!` in the hot path.
    let needles: Vec<String> = domains.iter().map(|d| format!("\"{d}\"")).collect();

    let mut buf = String::with_capacity(LINE_BUF_CAPACITY);

    // Statistics counters
    let mut lines_scanned = 0usize;
    let mut pre_filtered = 0usize;
    let mut parse_errors = 0usize;
    let mut fos_excluded = 0usize;
    let mut authors_rows = 0usize;
    let mut fields_rows = 0usize;

    // Progress update interval (every 10K lines to avoid overhead)
    const UPDATE_INTERVAL: usize = 10_000;

    loop {
        buf.clear();
        if reader.read_line(&mut buf).map_err(ShardError::Io)? == 0 {
            break;
        }
        lines_scanned += 1;

        // Update progress bar periodically
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

        // Cheap substring pre-filter: skip rows without any target domain string.
        // False positives (domain name appearing in other fields) are caught by matches_domains().
        if !needles.iter().any(|n| buf.contains(n.as_str())) {
            pre_filtered += 1;
            continue;
        }

        let row: PaperRow = match serde_json::from_str(&buf) {
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

        // Track denormalized row counts
        authors_rows += row.authors.len();
        fields_rows += row.s2fieldsofstudy.len();

        // authors/fields borrow, papers takes ownership (no String clones)
        authors_acc.push(&row);
        fields_acc.push(&row);
        papers_acc.push(row);

        if papers_acc.is_full() {
            papers_sink
                .write_batch(&papers_acc.take_batch())
                .map_err(ShardError::Io)?;
        }
        if authors_acc.is_full() {
            authors_sink
                .write_batch(&authors_acc.take_batch())
                .map_err(ShardError::Io)?;
        }
        if fields_acc.is_full() {
            fields_sink
                .write_batch(&fields_acc.take_batch())
                .map_err(ShardError::Io)?;
        }
    }

    // Flush remaining rows
    pb.set_message("writing...");
    if !papers_acc.is_empty() {
        papers_sink
            .write_batch(&papers_acc.take_batch())
            .map_err(ShardError::Io)?;
    }
    if !authors_acc.is_empty() {
        authors_sink
            .write_batch(&authors_acc.take_batch())
            .map_err(ShardError::Io)?;
    }
    if !fields_acc.is_empty() {
        fields_sink
            .write_batch(&fields_acc.take_batch())
            .map_err(ShardError::Io)?;
    }

    papers_sink.finalize().map_err(ShardError::Io)?;
    authors_sink.finalize().map_err(ShardError::Io)?;
    fields_sink.finalize().map_err(ShardError::Io)?;

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
    let mut attempt = 0;
    loop {
        match attempt_filtered_shard(shard, corpus_ids, output_dir, lance, zstd_level, pb) {
            Ok(stats) => return Ok(stats),
            Err(e) if attempt < MAX_RETRIES && e.is_retryable() => {
                attempt += 1;
                pb.set_message(format!("retry {attempt}/{MAX_RETRIES}..."));
                log::debug!(
                    "{}_{:04}: attempt {}/{} failed: {e}, retrying...",
                    shard.dataset,
                    shard.shard_idx,
                    attempt,
                    MAX_RETRIES
                );
                std::thread::sleep(backoff_duration(attempt));
            }
            Err(e) => {
                log::error!(
                    "{}_{:04}: failed permanently: {e}",
                    shard.dataset,
                    shard.shard_idx
                );
                if e.is_url_expired() {
                    log::error!("URL expired. Delete .cache/urls and re-run to refresh.");
                }
                return Err(());
            }
        }
    }
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
            let stats = process_shard_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let row: AbstractRow = serde_json::from_str(line).ok()?;
                    corpus_ids
                        .contains(row.corpusid)
                        .then_some((row.corpusid, row.text))
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
            let stats = process_shard_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let row: TldrRow = serde_json::from_str(line).ok()?;
                    corpus_ids
                        .contains(row.corpusid)
                        .then_some((row.corpusid, row.text))
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
            let stats = process_shard_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let row: CitationRow = serde_json::from_str(line).ok()?;
                    (corpus_ids.contains(row.citingcorpusid)
                        && corpus_ids.contains(row.citedcorpusid))
                    .then_some(row)
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
            let stats = process_shard_lines(
                &mut reader,
                &counter,
                &mut acc,
                |b| sink.write_batch(b),
                |line| {
                    let row: EmbeddingRow = serde_json::from_str(line).ok()?;
                    corpus_ids.contains(row.corpusid).then_some(row)
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

// === Generic shard line processor ===

/// Statistics from processing shard lines
struct LineProcessStats {
    rows_written: usize,
    lines_scanned: usize,
}

/// Read lines from a gzip reader, parse+filter each, push to accumulator, flush batches
fn process_shard_lines<A: Accumulator>(
    reader: &mut GzipReader,
    counter: &stream::ByteCounter,
    acc: &mut A,
    mut write_batch: impl FnMut(&RecordBatch) -> std::io::Result<()>,
    mut parse_filter: impl FnMut(&str) -> Option<A::Row>,
    pb: &ProgressBar,
) -> std::io::Result<LineProcessStats> {
    let mut buf = String::with_capacity(LINE_BUF_CAPACITY);
    let mut rows_written = 0usize;
    let mut lines_scanned = 0usize;

    // Progress update interval (every 10K lines to avoid overhead)
    const UPDATE_INTERVAL: usize = 10_000;

    loop {
        buf.clear();
        if reader.read_line(&mut buf)? == 0 {
            break;
        }
        lines_scanned += 1;

        // Update progress bar periodically
        if lines_scanned.is_multiple_of(UPDATE_INTERVAL) {
            pb.set_position(counter.load(std::sync::atomic::Ordering::Relaxed));
            let match_pct = if lines_scanned > 0 {
                rows_written as f64 / lines_scanned as f64 * 100.0
            } else {
                0.0
            };
            pb.set_message(format!(
                "{} rows ({:.1}%)",
                fmt_num(rows_written),
                match_pct
            ));
        }

        if let Some(row) = parse_filter(&buf) {
            acc.push(row);
            rows_written += 1;
            if acc.is_full() {
                write_batch(&acc.take_batch())?;
            }
        }
    }
    if acc.len() > 0 {
        write_batch(&acc.take_batch())?;
    }
    Ok(LineProcessStats {
        rows_written,
        lines_scanned,
    })
}

// === Error types ===

#[derive(Debug)]
enum ShardError {
    Stream(stream::StreamError),
    Io(std::io::Error),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "IO: {e}"),
        }
    }
}

impl std::error::Error for ShardError {}

impl ShardError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Stream(e) => e.is_retryable(),
            Self::Io(e) => e.kind() != std::io::ErrorKind::StorageFull,
        }
    }

    fn is_url_expired(&self) -> bool {
        // 403/410 = URL signature expired
        // 400 = AWS STS token expired (pre-signed URL with expired temporary credentials)
        matches!(
            self,
            Self::Stream(stream::StreamError::Http {
                status: Some(400 | 403 | 410),
                ..
            })
        )
    }
}

const fn backoff_duration(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_secs(2u64.pow(attempt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use papeline_core::stream::StreamError;

    fn http_err(status: u16) -> StreamError {
        StreamError::Http {
            status: Some(status),
            message: "test".to_string(),
        }
    }

    #[test]
    fn backoff_exponential() {
        assert_eq!(backoff_duration(1), std::time::Duration::from_secs(2));
        assert_eq!(backoff_duration(2), std::time::Duration::from_secs(4));
        assert_eq!(backoff_duration(3), std::time::Duration::from_secs(8));
    }

    #[test]
    fn shard_error_stream_403_not_retryable() {
        let err = ShardError::Stream(http_err(403));
        assert!(!err.is_retryable());
    }

    #[test]
    fn shard_error_stream_500_retryable() {
        let err = ShardError::Stream(http_err(500));
        assert!(err.is_retryable());
    }

    #[test]
    fn shard_error_io_storage_full_not_retryable() {
        let err = ShardError::Io(std::io::Error::new(std::io::ErrorKind::StorageFull, "full"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn shard_error_io_other_retryable() {
        let err = ShardError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
        assert!(err.is_retryable());
    }

    #[test]
    fn is_url_expired_403() {
        let err = ShardError::Stream(http_err(403));
        assert!(err.is_url_expired());
    }

    #[test]
    fn is_url_expired_410() {
        let err = ShardError::Stream(http_err(410));
        assert!(err.is_url_expired());
    }

    #[test]
    fn is_url_expired_400() {
        let err = ShardError::Stream(http_err(400));
        assert!(err.is_url_expired());
    }

    #[test]
    fn is_url_expired_false_for_500() {
        let err = ShardError::Stream(http_err(500));
        assert!(!err.is_url_expired());
    }

    #[test]
    fn is_url_expired_false_for_io() {
        let err = ShardError::Io(std::io::Error::other("test"));
        assert!(!err.is_url_expired());
    }
}
