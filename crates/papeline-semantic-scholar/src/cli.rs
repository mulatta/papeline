//! CLI argument definitions (clap derive)

use std::path::PathBuf;

use clap::{ArgGroup, Parser, Subcommand};

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[derive(Parser)]
#[command(
    name = "s2-dataset-fetcher",
    about = "Fetch and filter Semantic Scholar dataset shards"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Suppress info logs (only warnings and errors)
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Enable debug logging (includes parse errors)
    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage releases
    Release {
        #[command(subcommand)]
        action: ReleaseAction,
    },
    /// Manage datasets
    Dataset {
        #[command(subcommand)]
        action: DatasetAction,
    },
    /// Fetch and filter dataset shards
    Fetch(FetchArgs),
}

#[derive(Subcommand)]
pub enum ReleaseAction {
    /// List available releases
    List,
}

#[derive(Subcommand)]
pub enum DatasetAction {
    /// List datasets in a release
    List {
        /// Release ID or "latest"
        #[arg(long, default_value = "latest")]
        release: String,
    },
}

#[derive(Parser)]
#[command(group(ArgGroup::new("source").required(true).args(["release", "url_dir"])))]
pub struct FetchArgs {
    /// S2 release ID or "latest" (fetches URLs via API)
    #[arg(long)]
    pub release: Option<String>,

    /// Pre-fetched URL directory (mutually exclusive with --release)
    #[arg(long)]
    pub url_dir: Option<PathBuf>,

    /// `FoS` domain filter (required, comma-separated)
    #[arg(long, required = true, value_delimiter = ',')]
    pub domains: Vec<String>,

    /// Comma-separated datasets to process
    #[arg(
        long,
        default_value = "papers,abstracts,tldrs,citations,embeddings",
        value_delimiter = ','
    )]
    pub datasets: Vec<String>,

    /// Output directory
    #[arg(long, default_value = "./outputs")]
    pub output_dir: PathBuf,

    /// Number of worker threads
    #[arg(long, default_value_t = num_cpus())]
    pub workers: usize,

    /// Max shards per dataset (for testing)
    #[arg(long)]
    pub max_shards: Option<usize>,

    /// ZSTD compression level (1-22, default: 3)
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i32).range(1..=22))]
    pub zstd_level: i32,
}
