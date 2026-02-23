//! Fetch subcommand - download datasets from various sources

use std::path::PathBuf;

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Args, Subcommand, ValueEnum};
use comfy_table::{Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use papeline_core::SharedProgress;

use crate::config::Config;

#[derive(Args, Debug)]
pub struct FetchArgs {
    #[command(subcommand)]
    pub source: FetchSource,
}

#[derive(Subcommand, Debug)]
pub enum FetchSource {
    /// Fetch OpenAlex dataset
    Openalex(OpenAlexArgs),
    /// Fetch PubMed baseline dataset
    Pubmed(PubmedArgs),
    /// Fetch Semantic Scholar dataset
    S2(S2Args),
    /// Fetch all sources in parallel
    All(AllArgs),
}

#[derive(Args, Debug)]
pub struct OpenAlexArgs {
    /// Entity type to fetch
    #[arg(short, long, value_enum, default_value = "works")]
    pub entity: OpenAlexEntity,

    /// Only fetch records updated since this date (YYYY-MM-DD)
    #[arg(short, long, value_parser = parse_date)]
    pub since: Option<NaiveDate>,

    /// Output directory
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Maximum number of shards to process
    #[arg(short = 'l', long)]
    pub limit: Option<usize>,

    /// Number of parallel workers
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Zstd compression level (1-22)
    #[arg(short, long)]
    pub zstd_level: Option<i32>,
}

#[derive(Clone, ValueEnum, Debug)]
pub enum OpenAlexEntity {
    Works,
    Authors,
    Sources,
    Institutions,
    Publishers,
    Topics,
    Funders,
}

impl From<OpenAlexEntity> for papeline_openalex::Entity {
    fn from(e: OpenAlexEntity) -> Self {
        match e {
            OpenAlexEntity::Works => papeline_openalex::Entity::Works,
            OpenAlexEntity::Authors => papeline_openalex::Entity::Authors,
            OpenAlexEntity::Sources => papeline_openalex::Entity::Sources,
            OpenAlexEntity::Institutions => papeline_openalex::Entity::Institutions,
            OpenAlexEntity::Publishers => papeline_openalex::Entity::Publishers,
            OpenAlexEntity::Topics => papeline_openalex::Entity::Topics,
            OpenAlexEntity::Funders => papeline_openalex::Entity::Funders,
        }
    }
}

#[derive(Args, Debug)]
pub struct PubmedArgs {
    /// Output directory
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Maximum number of files to process
    #[arg(short = 'l', long)]
    pub limit: Option<usize>,

    /// Number of parallel workers
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Zstd compression level (1-22)
    #[arg(short, long)]
    pub zstd_level: Option<i32>,
}

#[derive(Args, Debug)]
pub struct S2Args {
    /// S2 release ID or "latest" (requires S2_API_KEY)
    #[arg(long, default_value = "latest")]
    pub release: String,

    /// FoS domain filter (required, comma-separated)
    #[arg(long, required = true, value_delimiter = ',')]
    pub domains: Vec<String>,

    /// Datasets to fetch (comma-separated)
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "papers,abstracts,tldrs,citations"
    )]
    pub datasets: Vec<String>,

    /// Output directory
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Maximum number of shards to process
    #[arg(short = 'l', long)]
    pub limit: Option<usize>,

    /// Number of parallel workers
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Zstd compression level (1-22)
    #[arg(short, long)]
    pub zstd_level: Option<i32>,
}

#[derive(Args, Debug)]
pub struct AllArgs {
    /// Output directory
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Run sources in parallel
    #[arg(long)]
    pub parallel: bool,

    /// Maximum number of shards to process per source
    #[arg(short = 'l', long)]
    pub limit: Option<usize>,

    /// FoS domain filter for S2 (comma-separated, required to include S2)
    #[arg(long, value_delimiter = ',')]
    pub domains: Option<Vec<String>>,

    /// Number of parallel workers per source
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Zstd compression level (1-22)
    #[arg(short, long)]
    pub zstd_level: Option<i32>,
}

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| format!("Invalid date format: {e}"))
}

pub fn run(args: FetchArgs, config: &Config, progress: &SharedProgress) -> Result<()> {
    match args.source {
        FetchSource::Openalex(oa_args) => fetch_openalex(oa_args, config, progress),
        FetchSource::Pubmed(pm_args) => fetch_pubmed(pm_args, config, progress),
        FetchSource::S2(s2_args) => fetch_s2(s2_args, config, progress),
        FetchSource::All(all_args) => fetch_all(all_args, config, progress),
    }
}

/// Print a key-value summary table on stderr
fn print_summary(title: &str, rows: &[(&str, String)]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new(title).fg(Color::Cyan),
            Cell::new("Value").fg(Color::Cyan),
        ]);
    for (label, value) in rows {
        table.add_row(vec![Cell::new(label), Cell::new(value)]);
    }
    eprintln!("\n{table}");
}

fn fetch_openalex(args: OpenAlexArgs, config: &Config, progress: &SharedProgress) -> Result<()> {
    let output_dir = args
        .output
        .unwrap_or_else(|| config.output.default_dir.clone());
    let _workers = args.workers; // pool is global; kept for CLI compat
    let zstd_level = args.zstd_level.unwrap_or(config.output.compression_level);

    let oa_config = papeline_openalex::Config {
        entity: args.entity.into(),
        since: args.since,
        output_dir: output_dir.clone(),
        max_shards: args.limit,
        zstd_level,
        topic_filter: Default::default(),
    };

    log::info!("Fetching OpenAlex {}", oa_config.entity);
    log::info!("  Output: {}", output_dir.display());
    log::info!("  Since: {:?}", args.since);

    let summary = papeline_openalex::run(&oa_config, progress.clone())?;

    print_summary(
        "OpenAlex",
        &[
            (
                "Shards",
                format!(
                    "{}/{} ({} failed)",
                    summary.completed_shards, summary.total_shards, summary.failed_shards
                ),
            ),
            (
                "Rows",
                format!(
                    "{} from {} lines ({} errors)",
                    summary.total_rows, summary.total_lines, summary.parse_errors
                ),
            ),
            ("Time", format!("{:.1}s", summary.elapsed.as_secs_f64())),
        ],
    );

    if summary.failed_shards > 0 {
        anyhow::bail!("Some shards failed");
    }

    Ok(())
}

fn fetch_pubmed(args: PubmedArgs, config: &Config, progress: &SharedProgress) -> Result<()> {
    let output_dir = args
        .output
        .unwrap_or_else(|| config.output.default_dir.join("pubmed"));
    let _workers = args.workers; // pool is global; kept for CLI compat
    let zstd_level = args.zstd_level.unwrap_or(config.output.compression_level);

    let pm_config = papeline_pubmed::Config {
        output_dir: output_dir.clone(),
        max_files: args.limit,
        zstd_level,
        ..Default::default()
    };

    log::info!("Fetching PubMed baseline");
    log::info!("  Output: {}", output_dir.display());

    let summary = papeline_pubmed::run(&pm_config, progress.clone())?;

    print_summary(
        "PubMed",
        &[
            (
                "Files",
                format!(
                    "{}/{} ({} failed)",
                    summary.completed_files, summary.total_files, summary.failed_files
                ),
            ),
            ("Articles", format!("{}", summary.total_articles)),
            ("Time", format!("{:.1}s", summary.elapsed.as_secs_f64())),
        ],
    );

    if summary.failed_files > 0 {
        anyhow::bail!("Some files failed");
    }

    Ok(())
}

fn fetch_s2(args: S2Args, config: &Config, progress: &SharedProgress) -> Result<()> {
    let output_dir = args
        .output
        .unwrap_or_else(|| config.output.default_dir.clone());
    let _workers = args.workers; // pool is global; kept for CLI compat
    let zstd_level = args.zstd_level.unwrap_or(config.output.compression_level);

    log::info!("Fetching Semantic Scholar");
    log::info!("  Release: {}", args.release);
    log::info!("  Domains: {:?}", args.domains);
    log::info!("  Datasets: {:?}", args.datasets);
    log::info!("  Output: {}", output_dir.display());

    // Build FetchArgs for S2 (reuse existing Config conversion logic)
    let s2_fetch_args = papeline_semantic_scholar::FetchArgs {
        release: Some(args.release),
        url_dir: None,
        domains: args.domains,
        datasets: args.datasets,
        output_dir,
        max_shards: args.limit,
        zstd_level,
    };

    // Convert to Config (handles release resolution, URL fetching)
    let s2_config = papeline_semantic_scholar::Config::try_from(s2_fetch_args)?;

    // Run S2 pipeline with shared progress
    let exit_code = papeline_semantic_scholar::run(&s2_config, progress.clone())?;

    if exit_code != std::process::ExitCode::SUCCESS {
        anyhow::bail!("S2 pipeline failed");
    }

    Ok(())
}

fn fetch_all(args: AllArgs, config: &Config, progress: &SharedProgress) -> Result<()> {
    let output_dir = args
        .output
        .clone()
        .unwrap_or_else(|| config.output.default_dir.clone());
    let _workers = args.workers; // pool is global; kept for CLI compat
    let zstd_level = args.zstd_level.unwrap_or(config.output.compression_level);
    let include_s2 = args.domains.is_some();

    log::info!("Fetching all sources");
    log::info!("  Output: {}", output_dir.display());
    log::info!("  Parallel: {}", args.parallel);
    log::info!("  Include S2: {}", include_s2);

    if args.parallel {
        fetch_all_parallel(&args, config, output_dir, zstd_level, progress)
    } else {
        fetch_all_sequential(&args, config, output_dir, zstd_level, progress)
    }
}

fn fetch_all_sequential(
    args: &AllArgs,
    config: &Config,
    output_dir: PathBuf,
    zstd_level: i32,
    progress: &SharedProgress,
) -> Result<()> {
    // OpenAlex
    let oa_args = OpenAlexArgs {
        entity: OpenAlexEntity::Works,
        since: None,
        output: Some(output_dir.join("openalex")),
        limit: args.limit,
        workers: None,
        zstd_level: Some(zstd_level),
    };
    fetch_openalex(oa_args, config, progress)?;

    // S2 (if domains provided)
    if let Some(domains) = args.domains.clone() {
        let s2_args = S2Args {
            release: "latest".to_string(),
            domains,
            datasets: vec![
                "papers".to_string(),
                "abstracts".to_string(),
                "tldrs".to_string(),
                "citations".to_string(),
            ],
            output: Some(output_dir.join("s2")),
            limit: args.limit,
            workers: None,
            zstd_level: Some(zstd_level),
        };
        fetch_s2(s2_args, config, progress)?;
    } else {
        log::info!("S2 skipped (no --domains provided)");
    }

    eprintln!("\nAll sources completed.");
    Ok(())
}

fn fetch_all_parallel(
    args: &AllArgs,
    config: &Config,
    output_dir: PathBuf,
    zstd_level: i32,
    progress: &SharedProgress,
) -> Result<()> {
    use papeline_core::SHARED_RUNTIME;

    // Clone config values for move into spawned tasks
    let config_clone = config.clone();
    let oa_output = output_dir.join("openalex");
    let limit = args.limit;
    let progress_clone = progress.clone();

    // Spawn OpenAlex task
    let oa_handle = SHARED_RUNTIME.handle().spawn_blocking(move || {
        let oa_args = OpenAlexArgs {
            entity: OpenAlexEntity::Works,
            since: None,
            output: Some(oa_output),
            limit,
            workers: None,
            zstd_level: Some(zstd_level),
        };
        fetch_openalex(oa_args, &config_clone, &progress_clone)
    });

    // Spawn S2 task (if domains provided)
    let s2_handle = if let Some(domains) = args.domains.clone() {
        let config_clone = config.clone();
        let s2_output = output_dir.join("s2");
        let progress_clone = progress.clone();

        Some(SHARED_RUNTIME.handle().spawn_blocking(move || {
            let s2_args = S2Args {
                release: "latest".to_string(),
                domains,
                datasets: vec![
                    "papers".to_string(),
                    "abstracts".to_string(),
                    "tldrs".to_string(),
                    "citations".to_string(),
                ],
                output: Some(s2_output),
                limit,
                workers: None,
                zstd_level: Some(zstd_level),
            };
            fetch_s2(s2_args, &config_clone, &progress_clone)
        }))
    } else {
        log::info!("S2 skipped (no --domains provided)");
        None
    };

    // Wait for all tasks
    let results = SHARED_RUNTIME.handle().block_on(async {
        let oa_result = oa_handle.await;

        let s2_result = if let Some(handle) = s2_handle {
            Some(handle.await)
        } else {
            None
        };

        (oa_result, s2_result)
    });

    // Check results
    let mut errors = Vec::new();

    match results.0 {
        Ok(Ok(())) => log::info!("OpenAlex completed successfully"),
        Ok(Err(e)) => errors.push(format!("OpenAlex: {e}")),
        Err(e) => errors.push(format!("OpenAlex task panicked: {e}")),
    }

    if let Some(s2_result) = results.1 {
        match s2_result {
            Ok(Ok(())) => log::info!("S2 completed successfully"),
            Ok(Err(e)) => errors.push(format!("S2: {e}")),
            Err(e) => errors.push(format!("S2 task panicked: {e}")),
        }
    }

    if errors.is_empty() {
        eprintln!("\nAll sources completed successfully.");
        Ok(())
    } else {
        for e in &errors {
            log::error!("{e}");
        }
        anyhow::bail!("{} source(s) failed", errors.len())
    }
}
