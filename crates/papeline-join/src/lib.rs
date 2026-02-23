//! papeline-join: Multi-source academic paper join pipeline
//!
//! Joins PubMed, OpenAlex, and Semantic Scholar parquet datasets
//! using DuckDB with narrow key match + wide enrich strategy.

mod config;
mod sql;

pub use config::JoinConfig;

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use duckdb::Connection;

/// Summary statistics from the join operation.
#[derive(Debug)]
pub struct JoinSummary {
    pub total_nodes: u64,
    pub openalex_matched: u64,
    /// OA matched via DOI (pass 1)
    pub openalex_doi: u64,
    /// OA matched via PMID fallback (pass 2)
    pub openalex_pmid: u64,
    pub s2_matched: u64,
    /// S2 matched via DOI (pass 3)
    pub s2_doi: u64,
    /// S2 matched via PMID fallback (pass 4)
    pub s2_pmid: u64,
    /// Number of exported citation edges
    pub citations_exported: u64,
    /// Total wall-clock time
    pub elapsed: Duration,
}

/// Run the join pipeline (no progress callback).
pub fn run(config: &JoinConfig) -> Result<JoinSummary> {
    run_with_progress(config, |_, _| {})
}

/// Run the join pipeline with a step progress callback.
///
/// `on_step(current, description)` is called before each of the 9 steps
/// (1-indexed, total=9).
pub fn run_with_progress(
    config: &JoinConfig,
    on_step: impl Fn(usize, &str),
) -> Result<JoinSummary> {
    let start = Instant::now();

    std::fs::create_dir_all(&config.output_dir).with_context(|| {
        format!(
            "Failed to create output dir: {}",
            config.output_dir.display()
        )
    })?;

    let conn =
        Connection::open_in_memory().context("Failed to open DuckDB in-memory connection")?;

    // Configure DuckDB resource limits
    conn.execute_batch(&format!(
        "SET memory_limit = '{}';
         SET temp_directory = '/tmp/duckdb_papeline';
         SET threads = {};",
        config.memory_limit,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8),
    ))
    .context("Failed to configure DuckDB")?;

    // Create normalize_doi macro
    conn.execute_batch(sql::create_normalize_doi_macro())
        .context("Failed to create normalize_doi macro")?;

    // Create views over source parquet files
    for stmt in sql::create_source_views(&config.pubmed_dir, &config.openalex_dir, &config.s2_dir) {
        conn.execute_batch(&stmt)
            .with_context(|| format!("Failed to create view: {stmt}"))?;
    }

    // Step 1: Narrow key tables (OA + S2)
    on_step(1, "Key tables");
    conn.execute_batch(sql::create_key_tables())
        .context("Failed: key tables")?;

    // Step 2: PubMed + OpenAlex (DOI match via key table)
    on_step(2, "PubMed + OpenAlex (DOI)");
    conn.execute_batch(sql::join_pubmed_openalex_pass1())
        .context("Failed: PubMed+OA DOI join")?;

    let oa_doi: u64 = conn
        .query_row(sql::count_oa_doi(), [], |row| row.get::<_, i64>(0))
        .context("Failed to count OA DOI matches")? as u64;

    // Step 3: PubMed + OpenAlex (PMID fallback via key table)
    on_step(3, "PubMed + OpenAlex (PMID)");
    conn.execute_batch(sql::join_pubmed_openalex_pass2())
        .context("Failed: PubMed+OA PMID fallback join")?;

    // Step 4: OpenAlex enrich (string key join)
    on_step(4, "OpenAlex enrich");
    conn.execute_batch(sql::enrich_openalex())
        .context("Failed: OA enrichment")?;

    // Step 5: S2 match (DOI)
    on_step(5, "S2 match (DOI)");
    conn.execute_batch(sql::join_s2_doi())
        .context("Failed: S2 DOI join")?;

    let s2_doi: u64 = conn
        .query_row(sql::count_s2_doi(), [], |row| row.get::<_, i64>(0))
        .context("Failed to count S2 DOI matches")? as u64;

    // Step 6: S2 match (PMID)
    on_step(6, "S2 match (PMID)");
    conn.execute_batch(sql::join_s2_pmid())
        .context("Failed: S2 PMID fallback join")?;

    // Collect summary (from nodes table, before enrichment)
    let (total_nodes, openalex_matched, s2_matched) = conn
        .query_row(sql::summary_query(), [], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
            ))
        })
        .context("Failed to query join summary")?;

    // Step 7: S2 enrich (wide integer key joins)
    on_step(7, "S2 enrich");
    conn.execute_batch(sql::enrich_s2())
        .context("Failed: S2 enrichment")?;

    // Step 8: Export nodes
    on_step(8, "Export nodes");
    conn.execute_batch(&sql::export_nodes(&config.output_dir))
        .context("Failed to export nodes.parquet")?;

    // Step 9: Export filtered citations
    on_step(9, "Export citations");
    conn.execute_batch(&sql::export_citations(&config.output_dir))
        .context("Failed to export citations.parquet")?;

    let citations_exported: u64 = conn
        .query_row(&sql::count_citations(&config.output_dir), [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap_or(0) as u64;

    let elapsed = start.elapsed();

    log::info!(
        "Join complete: {} total, {} OA ({:.1}%), {} S2 ({:.1}%), {} citations in {:.1}s",
        total_nodes,
        openalex_matched,
        if total_nodes > 0 {
            openalex_matched as f64 / total_nodes as f64 * 100.0
        } else {
            0.0
        },
        s2_matched,
        if total_nodes > 0 {
            s2_matched as f64 / total_nodes as f64 * 100.0
        } else {
            0.0
        },
        citations_exported,
        elapsed.as_secs_f64(),
    );

    Ok(JoinSummary {
        total_nodes,
        openalex_matched,
        openalex_doi: oa_doi,
        openalex_pmid: openalex_matched.saturating_sub(oa_doi),
        s2_matched,
        s2_doi,
        s2_pmid: s2_matched.saturating_sub(s2_doi),
        citations_exported,
        elapsed,
    })
}
