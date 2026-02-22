//! papeline - Unified CLI for academic dataset pipelines
//!
//! Fetches and transforms datasets from OpenAlex, Semantic Scholar,
//! and other academic data sources into Parquet format.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod cmd;
mod config;

use config::Config;

#[derive(Parser)]
#[command(name = "papeline")]
#[command(about = "Unified CLI for academic dataset pipelines")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Config file path (default: ./papeline.toml or ~/.config/papeline/config.toml)
    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch datasets from various sources
    Fetch(cmd::fetch::FetchArgs),
    /// Show pipeline status
    Status(cmd::status::StatusArgs),
    /// Show current configuration
    Config,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    // Load configuration
    let config = if let Some(path) = cli.config {
        Config::from_file(&path)?
    } else {
        Config::load()?
    };

    match cli.command {
        Command::Fetch(args) => cmd::fetch::run(args, &config),
        Command::Status(args) => cmd::status::run(args),
        Command::Config => {
            println!("Configuration:");
            println!(
                "  Output directory: {}",
                config.output.default_dir.display()
            );
            println!("  Compression level: {}", config.output.compression_level);
            println!(
                "  Workers: {} (max: {})",
                config.workers.default, config.workers.max
            );
            println!();
            println!("OpenAlex:");
            println!("  Base URL: {}", config.openalex.base_url);
            println!();
            println!("PubMed:");
            println!("  Base URL: {}", config.pubmed.base_url);
            println!();
            println!("Semantic Scholar:");
            println!("  API URL: {}", config.s2.api_url);
            println!(
                "  API Key: {}",
                if config.s2.api_key.is_some() {
                    "configured"
                } else {
                    "not set"
                }
            );
            Ok(())
        }
    }
}
