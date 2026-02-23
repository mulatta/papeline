//! papeline - Unified CLI for academic dataset pipelines
//!
//! Fetches and transforms datasets from OpenAlex, Semantic Scholar,
//! and other academic data sources into Parquet format.

use std::sync::Arc;

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

    /// Enable debug logging
    #[arg(long, global = true)]
    debug: bool,

    /// Config file path (default: ./papeline.toml or ~/.config/papeline/config.toml)
    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,

    /// Read timeout in seconds for stall detection
    #[arg(long, global = true)]
    read_timeout: Option<u64>,

    /// Maximum retry attempts for transient failures
    #[arg(long, global = true)]
    max_retries: Option<u32>,

    /// Milliseconds between parallel worker starts
    #[arg(long, global = true)]
    stagger_ms: Option<u64>,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch datasets from various sources
    Fetch(cmd::fetch::FetchArgs),
    /// Join PubMed, OpenAlex, and S2 datasets
    Join(cmd::join::JoinArgs),
    /// Run full pipeline from run.toml (fetch + join with caching)
    Run(cmd::run::RunArgs),
    /// Manage content-addressable store (cache)
    Store(cmd::store::StoreArgs),
    /// Show current configuration
    Config,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Progress context (TTY auto-detect)
    let progress = Arc::new(papeline_core::ProgressContext::new());

    // Logging:
    //   TTY:     quiet (warn) unless --debug  — progress bars show activity
    //   non-TTY: info unless --debug          — logs are the only progress indicator
    let is_tty = progress.is_tty();
    let multi = if is_tty { Some(progress.multi()) } else { None };
    let quiet = if is_tty { !cli.debug } else { false };
    papeline_core::init_logging(quiet, cli.debug, multi);

    // Load configuration
    let config = if let Some(path) = cli.config {
        Config::from_file(&path)?
    } else {
        Config::load()?
    };

    // Apply HTTP settings (config file defaults, CLI overrides)
    let http_config = papeline_core::HttpConfig {
        read_timeout: std::time::Duration::from_secs(
            cli.read_timeout.unwrap_or(config.http.read_timeout),
        ),
        max_retries: cli.max_retries.unwrap_or(config.http.max_retries),
        stagger_ms: cli.stagger_ms.unwrap_or(config.http.stagger_ms),
    };
    papeline_core::set_http_config(http_config);

    match cli.command {
        Command::Fetch(args) => cmd::fetch::run(args, &config, &progress),
        Command::Join(args) => cmd::join::run(args, &progress),
        Command::Run(args) => cmd::run::run(args, &config, &progress),
        Command::Store(args) => cmd::store::run(args),
        Command::Config => {
            use comfy_table::{
                Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL,
            };

            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL)
                .apply_modifier(UTF8_ROUND_CORNERS)
                .set_header(vec![
                    Cell::new("Setting").fg(Color::Cyan),
                    Cell::new("Value").fg(Color::Cyan),
                ]);

            table.add_row(vec![
                "Output directory",
                &config.output.default_dir.display().to_string(),
            ]);
            table.add_row(vec![
                "Compression level",
                &config.output.compression_level.to_string(),
            ]);
            table.add_row(vec![
                "Workers",
                &format!("{} (max: {})", config.workers.default, config.workers.max),
            ]);
            table.add_row(vec!["OA base URL", &config.openalex.base_url]);
            table.add_row(vec!["PM base URL", &config.pubmed.base_url]);
            table.add_row(vec!["S2 API URL", &config.s2.api_url]);
            table.add_row(vec![
                "S2 API key",
                if config.s2.api_key.is_some() {
                    "configured"
                } else {
                    "not set"
                },
            ]);
            table.add_row(vec![
                "Read timeout",
                &format!("{}s", config.http.read_timeout),
            ]);
            table.add_row(vec!["Max retries", &config.http.max_retries.to_string()]);
            table.add_row(vec!["Stagger", &format!("{}ms", config.http.stagger_ms)]);

            eprintln!("\n{table}");
            Ok(())
        }
    }
}
