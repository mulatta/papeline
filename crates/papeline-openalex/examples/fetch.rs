//! Example: Fetch OpenAlex works data
//!
//! Usage:
//!   cargo run -p papeline-openalex --example fetch -- \
//!     --entity works \
//!     --since 2025-02-01 \
//!     --output ./test-output \
//!     --limit 5

use std::path::PathBuf;

use chrono::NaiveDate;

fn main() -> anyhow::Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Parse arguments
    let args: Vec<String> = std::env::args().collect();
    let config = parse_args(&args)?;

    log::info!("Starting OpenAlex fetch");
    log::info!("  Entity: {}", config.entity);
    log::info!("  Since: {:?}", config.since);
    log::info!("  Output: {}", config.output_dir.display());
    log::info!("  Max shards: {:?}", config.max_shards);
    log::info!("  Workers: {}", config.workers);

    // Run pipeline
    let summary = papeline_openalex::run(&config)?;

    log::info!("Fetch complete!");
    log::info!("  Total rows: {}", summary.total_rows);
    log::info!(
        "  Completed shards: {}/{}",
        summary.completed_shards,
        summary.total_shards
    );

    if summary.failed_shards > 0 {
        log::warn!("  Failed shards: {}", summary.failed_shards);
    }

    Ok(())
}

fn parse_args(args: &[String]) -> anyhow::Result<papeline_openalex::Config> {
    let mut config = papeline_openalex::Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--entity" | "-e" => {
                i += 1;
                config.entity = papeline_openalex::Entity::from_name(&args[i])
                    .ok_or_else(|| anyhow::anyhow!("Unknown entity: {}", args[i]))?;
            }
            "--since" | "-s" => {
                i += 1;
                config.since = Some(NaiveDate::parse_from_str(&args[i], "%Y-%m-%d")?);
            }
            "--output" | "-o" => {
                i += 1;
                config.output_dir = PathBuf::from(&args[i]);
            }
            "--limit" | "-l" => {
                i += 1;
                config.max_shards = Some(args[i].parse()?);
            }
            "--workers" | "-w" => {
                i += 1;
                config.workers = args[i].parse()?;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            arg => {
                anyhow::bail!("Unknown argument: {}", arg);
            }
        }
        i += 1;
    }

    Ok(config)
}

fn print_help() {
    eprintln!(
        r#"OpenAlex data fetcher example

USAGE:
    cargo run -p papeline-openalex --example fetch -- [OPTIONS]

OPTIONS:
    -e, --entity <NAME>     Entity to fetch (works) [default: works]
    -s, --since <DATE>      Only fetch records updated since DATE (YYYY-MM-DD)
    -o, --output <DIR>      Output directory [default: output]
    -l, --limit <N>         Maximum shards to process
    -w, --workers <N>       Number of parallel workers [default: auto]
    -h, --help              Print help

EXAMPLES:
    # Fetch recent works (last 30 days)
    cargo run -p papeline-openalex --example fetch -- --since 2025-01-01 --limit 5

    # Full download (WARNING: very large!)
    cargo run -p papeline-openalex --example fetch -- --output ./openalex-data
"#
    );
}
