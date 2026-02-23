//! Join subcommand - merge PubMed, OpenAlex, and S2 parquet datasets

use std::path::PathBuf;

use anyhow::Result;
use clap::Args;

#[derive(Args, Debug)]
pub struct JoinArgs {
    /// PubMed parquet directory (contains articles_*.parquet)
    #[arg(long)]
    pub pubmed_dir: PathBuf,

    /// OpenAlex works parquet directory (contains works_*.parquet)
    #[arg(long)]
    pub openalex_dir: PathBuf,

    /// S2 parquet directory (contains papers_*.parquet, citations_*.parquet)
    #[arg(long)]
    pub s2_dir: PathBuf,

    /// Output directory for joined parquet files
    #[arg(short, long)]
    pub output: PathBuf,

    /// DuckDB memory limit (e.g. "8GB")
    #[arg(long, default_value = "8GB")]
    pub memory_limit: String,
}

pub fn run(args: JoinArgs) -> Result<()> {
    let config = papeline_join::JoinConfig {
        pubmed_dir: args.pubmed_dir,
        openalex_dir: args.openalex_dir,
        s2_dir: args.s2_dir,
        output_dir: args.output,
        memory_limit: args.memory_limit,
    };

    let summary = papeline_join::run(&config)?;

    println!();
    println!("=== Join Summary ===");
    println!("Total nodes: {}", summary.total_nodes);
    println!(
        "OpenAlex matched: {} ({:.1}%)",
        summary.openalex_matched,
        if summary.total_nodes > 0 {
            summary.openalex_matched as f64 / summary.total_nodes as f64 * 100.0
        } else {
            0.0
        }
    );
    println!(
        "S2 matched: {} ({:.1}%)",
        summary.s2_matched,
        if summary.total_nodes > 0 {
            summary.s2_matched as f64 / summary.total_nodes as f64 * 100.0
        } else {
            0.0
        }
    );

    Ok(())
}
