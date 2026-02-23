//! Join subcommand - merge PubMed, OpenAlex, and S2 parquet datasets

use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use comfy_table::{Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use papeline_core::SharedProgress;
use papeline_core::progress::fmt_num;

#[derive(Args, Debug)]
pub struct JoinArgs {
    /// PubMed parquet directory (contains pubmed_*.parquet)
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

pub fn run(args: JoinArgs, progress: &SharedProgress) -> Result<()> {
    let config = papeline_join::JoinConfig {
        pubmed_dir: args.pubmed_dir,
        openalex_dir: args.openalex_dir,
        s2_dir: args.s2_dir,
        output_dir: args.output,
        memory_limit: args.memory_limit,
    };

    let pb = progress.stage_line("join");
    let summary = papeline_join::run_with_progress(&config, |step, desc| {
        pb.set_message(format!("step {step}/6: {desc}"));
    })?;
    pb.finish_and_clear();

    print_join_summary(&summary);
    Ok(())
}

fn print_join_summary(s: &papeline_join::JoinSummary) {
    let pct = |n: u64, total: u64| -> f64 {
        if total > 0 {
            n as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    };

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Join").fg(Color::Cyan),
            Cell::new("Value").fg(Color::Cyan),
            Cell::new("%").fg(Color::Cyan),
        ]);
    table.add_row(vec![
        Cell::new("Total nodes"),
        Cell::new(fmt_num(s.total_nodes as usize)),
        Cell::new(""),
    ]);
    table.add_row(vec![
        Cell::new("OA matched (DOI)"),
        Cell::new(fmt_num(s.openalex_doi as usize)),
        Cell::new(format!("{:.1}", pct(s.openalex_doi, s.total_nodes))),
    ]);
    table.add_row(vec![
        Cell::new("OA matched (PMID)"),
        Cell::new(fmt_num(s.openalex_pmid as usize)),
        Cell::new(format!("{:.1}", pct(s.openalex_pmid, s.total_nodes))),
    ]);
    table.add_row(vec![
        Cell::new("OA total").fg(Color::Green),
        Cell::new(fmt_num(s.openalex_matched as usize)).fg(Color::Green),
        Cell::new(format!("{:.1}", pct(s.openalex_matched, s.total_nodes))).fg(Color::Green),
    ]);
    table.add_row(vec![
        Cell::new("S2 matched (DOI)"),
        Cell::new(fmt_num(s.s2_doi as usize)),
        Cell::new(format!("{:.1}", pct(s.s2_doi, s.total_nodes))),
    ]);
    table.add_row(vec![
        Cell::new("S2 matched (PMID)"),
        Cell::new(fmt_num(s.s2_pmid as usize)),
        Cell::new(format!("{:.1}", pct(s.s2_pmid, s.total_nodes))),
    ]);
    table.add_row(vec![
        Cell::new("S2 total").fg(Color::Green),
        Cell::new(fmt_num(s.s2_matched as usize)).fg(Color::Green),
        Cell::new(format!("{:.1}", pct(s.s2_matched, s.total_nodes))).fg(Color::Green),
    ]);
    table.add_row(vec![
        Cell::new("Citations"),
        Cell::new(fmt_num(s.citations_exported as usize)),
        Cell::new(""),
    ]);
    table.add_row(vec![
        Cell::new("Time"),
        Cell::new(format!("{:.1}s", s.elapsed.as_secs_f64())),
        Cell::new(""),
    ]);

    eprintln!("\n{table}");
}
