//! papeline-join: Multi-source academic paper join pipeline
//!
//! Joins PubMed, OpenAlex, and Semantic Scholar parquet datasets
//! using DuckDB with a 2-pass hash join strategy.

mod config;
mod sql;

pub use config::JoinConfig;

use anyhow::{Context, Result};
use duckdb::Connection;

/// Summary statistics from the join operation.
#[derive(Debug)]
pub struct JoinSummary {
    pub total_nodes: u64,
    pub openalex_matched: u64,
    pub s2_matched: u64,
}

/// Run the join pipeline.
pub fn run(config: &JoinConfig) -> Result<JoinSummary> {
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
    log::info!("Creating normalize_doi macro");
    conn.execute_batch(sql::create_normalize_doi_macro())
        .context("Failed to create normalize_doi macro")?;

    // Create views over source parquet files
    log::info!("Creating source views");
    for stmt in sql::create_source_views(&config.pubmed_dir, &config.openalex_dir, &config.s2_dir) {
        conn.execute_batch(&stmt)
            .with_context(|| format!("Failed to create view: {stmt}"))?;
    }

    // Pass 1: PubMed + OpenAlex (DOI match)
    log::info!("Pass 1/4: Joining PubMed + OpenAlex on DOI");
    conn.execute_batch(sql::join_pubmed_openalex_pass1())
        .context("Failed: PubMed+OA DOI join")?;

    // Pass 2: PubMed + OpenAlex (PMID fallback)
    log::info!("Pass 2/4: Joining PubMed + OpenAlex on PMID (fallback)");
    conn.execute_batch(sql::join_pubmed_openalex_pass2())
        .context("Failed: PubMed+OA PMID fallback join")?;

    // Pass 3: + S2 (DOI match)
    log::info!("Pass 3/4: Joining with S2 on DOI");
    conn.execute_batch(sql::join_s2_pass1())
        .context("Failed: S2 DOI join")?;

    // Pass 4: + S2 (PMID fallback)
    log::info!("Pass 4/4: Joining with S2 on PMID (fallback)");
    conn.execute_batch(sql::join_s2_pass2())
        .context("Failed: S2 PMID fallback join")?;

    // Collect summary
    log::info!("Collecting join summary");
    let summary = conn
        .query_row(sql::summary_query(), [], |row| {
            Ok(JoinSummary {
                total_nodes: row.get::<_, i64>(0)? as u64,
                openalex_matched: row.get::<_, i64>(1)? as u64,
                s2_matched: row.get::<_, i64>(2)? as u64,
            })
        })
        .context("Failed to query join summary")?;

    log::info!(
        "Join complete: {} total, {} OA matched ({:.1}%), {} S2 matched ({:.1}%)",
        summary.total_nodes,
        summary.openalex_matched,
        if summary.total_nodes > 0 {
            summary.openalex_matched as f64 / summary.total_nodes as f64 * 100.0
        } else {
            0.0
        },
        summary.s2_matched,
        if summary.total_nodes > 0 {
            summary.s2_matched as f64 / summary.total_nodes as f64 * 100.0
        } else {
            0.0
        },
    );

    // Export nodes
    log::info!("Exporting nodes.parquet");
    conn.execute_batch(&sql::export_nodes(&config.output_dir))
        .context("Failed to export nodes.parquet")?;

    // Export filtered citations
    log::info!("Exporting citations.parquet");
    conn.execute_batch(&sql::export_citations(&config.output_dir))
        .context("Failed to export citations.parquet")?;

    log::info!("Done. Output: {}", config.output_dir.display());
    Ok(summary)
}
