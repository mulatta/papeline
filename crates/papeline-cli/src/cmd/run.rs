//! `papeline run` - orchestrate full pipeline from run.toml

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args;

use papeline_store::run_config::Defaults;
use papeline_store::stage::StageName;
use papeline_store::store::LookupResult;
use papeline_store::{RunConfig, StageInput, StageManifest, Store};

use crate::config::Config;

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Path to run.toml
    pub run_config: PathBuf,

    /// Force re-run all stages (ignore cache)
    #[arg(long)]
    pub force: bool,

    /// Show stage status without executing
    #[arg(long)]
    pub dry_run: bool,

    /// Number of parallel workers
    #[arg(short, long)]
    pub workers: Option<usize>,
}

/// Stage execution plan entry.
struct StagePlan {
    name: StageName,
    input: StageInput,
    status: StageStatus,
    manifest: Option<StageManifest>,
}

#[derive(Debug, Clone, Copy)]
enum StageStatus {
    Cached,
    NeedsRun,
}

impl std::fmt::Display for StageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cached => write!(f, "CACHED"),
            Self::NeedsRun => write!(f, "NEEDS_RUN"),
        }
    }
}

pub fn run(args: RunArgs, config: &Config) -> Result<()> {
    // 1. Parse run.toml
    let run_config = RunConfig::from_file(&args.run_config)
        .with_context(|| format!("failed to load {}", args.run_config.display()))?;
    run_config.validate()?;

    let defaults = Defaults {
        pubmed_base_url: config.pubmed.base_url.clone(),
        openalex_base_url: config.openalex.base_url.clone(),
        s2_api_url: config.s2.api_url.clone(),
        zstd_level: config.output.compression_level,
    };

    let workers = args.workers.unwrap_or(config.workers.default);

    // 2. Resolve S2 release if needed
    let s2_release_id = if run_config.s2.is_some() {
        let release_str = run_config.s2_release().unwrap();
        if release_str == "latest" {
            let api_key = std::env::var("S2_API_KEY")
                .context("S2_API_KEY required to resolve 'latest' release")?;
            let id = papeline_semantic_scholar::api::resolve_release(&release_str, &api_key)?;
            log::info!("Resolved S2 release 'latest' → {id}");
            Some(id)
        } else {
            Some(release_str)
        }
    } else {
        None
    };

    // 3. Build stage inputs for fetch stages
    let store = Store::new(&run_config.output)?;
    let mut fetch_plans: Vec<StagePlan> = Vec::new();

    if let Some(input) = run_config.pubmed_input(&defaults) {
        let (status, manifest) = check_cache(&store, &input, args.force);
        fetch_plans.push(StagePlan {
            name: StageName::Pubmed,
            input,
            status,
            manifest,
        });
    }

    if let Some(input) = run_config.openalex_input(&defaults) {
        let (status, manifest) = check_cache(&store, &input, args.force);
        fetch_plans.push(StagePlan {
            name: StageName::Openalex,
            input,
            status,
            manifest,
        });
    }

    if let Some(ref release_id) = s2_release_id {
        if let Some(input) = run_config.s2_input(release_id) {
            let (status, manifest) = check_cache(&store, &input, args.force);
            fetch_plans.push(StagePlan {
                name: StageName::S2,
                input,
                status,
                manifest,
            });
        }
    }

    // 4. Display plan
    println!("=== Pipeline Plan ===");
    println!("{:<12} {:<10} {:<10}", "Stage", "Hash", "Status");
    println!("{}", "-".repeat(34));
    for plan in &fetch_plans {
        println!(
            "{:<12} {:<10} {:<10}",
            plan.name,
            plan.input.short_hash(),
            plan.status
        );
    }

    if run_config.should_join() {
        println!("{:<12} {:<10} {:<10}", "join", "(pending)", "(after fetch)");
    }
    println!();

    if args.dry_run {
        println!("(dry-run mode, no execution)");
        return Ok(());
    }

    // 5. Execute fetch stages that need running
    for plan in &mut fetch_plans {
        if matches!(plan.status, StageStatus::Cached) {
            log::info!("{}: cached ({})", plan.name, plan.input.short_hash());
            continue;
        }

        log::info!("{}: executing...", plan.name);
        let tmp_dir = store.stage_tmp_dir(&plan.input);

        // Clean stale tmp
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)?;
        }
        std::fs::create_dir_all(&tmp_dir)?;

        match plan.name {
            StageName::Pubmed => {
                run_pubmed(&run_config, config, &tmp_dir, workers)?;
            }
            StageName::Openalex => {
                run_openalex(&run_config, config, &tmp_dir, workers)?;
            }
            StageName::S2 => {
                let release_id = s2_release_id.as_ref().unwrap();
                run_s2(&run_config, config, &tmp_dir, workers, release_id)?;
            }
            StageName::Join => unreachable!("join handled separately"),
        }

        let recursive = plan.name == StageName::S2;
        let manifest = store.commit_stage(&plan.input, &tmp_dir, recursive)?;
        log::info!(
            "{}: committed (content_hash: {})",
            plan.name,
            &manifest.content_hash[..8]
        );
        plan.manifest = Some(manifest);
        plan.status = StageStatus::Cached;
    }

    // 6. Join stage
    let mut join_plan: Option<StagePlan> = None;
    if run_config.should_join() {
        // Get content hashes from all 3 fetch stages
        let get_content_hash = |name: StageName| -> Result<String> {
            fetch_plans
                .iter()
                .find(|p| p.name == name)
                .and_then(|p| p.manifest.as_ref())
                .map(|m| m.content_hash.clone())
                .with_context(|| format!("{name} manifest missing for join"))
        };

        let pm_hash = get_content_hash(StageName::Pubmed)?;
        let oa_hash = get_content_hash(StageName::Openalex)?;
        let s2_hash = get_content_hash(StageName::S2)?;

        let join_input = run_config
            .join_input(&pm_hash, &oa_hash, &s2_hash)
            .expect("should_join() was true");

        let (status, manifest) = check_cache(&store, &join_input, args.force);

        println!(
            "{:<12} {:<10} {:<10}",
            "join",
            join_input.short_hash(),
            status
        );

        if matches!(status, StageStatus::Cached) {
            log::info!("join: cached ({})", join_input.short_hash());
            join_plan = Some(StagePlan {
                name: StageName::Join,
                input: join_input,
                status: StageStatus::Cached,
                manifest,
            });
        } else {
            log::info!("join: executing...");
            let tmp_dir = store.stage_tmp_dir(&join_input);
            if tmp_dir.exists() {
                std::fs::remove_dir_all(&tmp_dir)?;
            }
            std::fs::create_dir_all(&tmp_dir)?;

            // Get stage directories for join inputs
            let pm_plan = fetch_plans
                .iter()
                .find(|p| p.name == StageName::Pubmed)
                .unwrap();
            let oa_plan = fetch_plans
                .iter()
                .find(|p| p.name == StageName::Openalex)
                .unwrap();
            let s2_plan = fetch_plans
                .iter()
                .find(|p| p.name == StageName::S2)
                .unwrap();

            let join_config = papeline_join::JoinConfig {
                pubmed_dir: store.stage_dir(&pm_plan.input),
                openalex_dir: store.stage_dir(&oa_plan.input),
                s2_dir: store.stage_dir(&s2_plan.input),
                output_dir: tmp_dir.clone(),
                memory_limit: run_config.join_memory_limit(),
            };

            let summary = papeline_join::run(&join_config)?;
            log::info!(
                "join: {} nodes, {} OA matched, {} S2 matched",
                summary.total_nodes,
                summary.openalex_matched,
                summary.s2_matched
            );

            let manifest = store.commit_stage(&join_input, &tmp_dir, false)?;
            log::info!(
                "join: committed (content_hash: {})",
                &manifest.content_hash[..8]
            );

            join_plan = Some(StagePlan {
                name: StageName::Join,
                input: join_input,
                status: StageStatus::Cached,
                manifest: Some(manifest),
            });
        }
    }

    // 7. Create run entry
    let mut all_stages: Vec<(StageName, &StageInput, &StageManifest, bool)> = Vec::new();
    for plan in &fetch_plans {
        let was_cached = matches!(plan.status, StageStatus::Cached);
        all_stages.push((
            plan.name,
            &plan.input,
            plan.manifest.as_ref().unwrap(),
            was_cached,
        ));
    }
    if let Some(ref jp) = join_plan {
        all_stages.push((
            jp.name,
            &jp.input,
            jp.manifest.as_ref().unwrap(),
            matches!(jp.status, StageStatus::Cached),
        ));
    }

    let run_dir = store.create_run(&all_stages)?;

    // 8. Summary
    println!();
    println!("=== Run Complete ===");
    println!("Run: {}", run_dir.display());
    println!("{:<12} {:<10} {:<10} Status", "Stage", "Hash", "Content");
    println!("{}", "-".repeat(50));
    for plan in fetch_plans.iter().chain(join_plan.iter()) {
        if let Some(ref m) = plan.manifest {
            println!(
                "{:<12} {:<10} {:<10} {}",
                plan.name,
                plan.input.short_hash(),
                &m.content_hash[..8],
                plan.status
            );
        }
    }

    Ok(())
}

fn check_cache(
    store: &Store,
    input: &StageInput,
    force: bool,
) -> (StageStatus, Option<StageManifest>) {
    if force {
        return (StageStatus::NeedsRun, None);
    }
    match store.lookup(input) {
        LookupResult::Cached { manifest, .. } => (StageStatus::Cached, Some(manifest)),
        LookupResult::NeedsRun => (StageStatus::NeedsRun, None),
    }
}

fn run_pubmed(
    run_config: &RunConfig,
    config: &Config,
    output_dir: &std::path::Path,
    workers: usize,
) -> Result<()> {
    let cfg = run_config.pubmed.as_ref().unwrap();
    let pm_config = papeline_pubmed::Config {
        output_dir: output_dir.to_path_buf(),
        max_files: cfg.limit,
        workers,
        zstd_level: cfg.zstd_level.unwrap_or(run_config.zstd_level),
        base_url: cfg
            .base_url
            .clone()
            .unwrap_or_else(|| config.pubmed.base_url.clone()),
    };

    let summary = papeline_pubmed::run(&pm_config)?;
    log::info!(
        "pubmed: {}/{} files, {} articles",
        summary.completed_files,
        summary.total_files,
        summary.total_articles
    );
    if summary.failed_files > 0 {
        anyhow::bail!("pubmed: {} files failed", summary.failed_files);
    }
    Ok(())
}

fn run_openalex(
    run_config: &RunConfig,
    _config: &Config,
    output_dir: &std::path::Path,
    workers: usize,
) -> Result<()> {
    let cfg = run_config.openalex.as_ref().unwrap();
    let entity = cfg.entity.as_deref().unwrap_or("works");
    let entity = match entity {
        "works" => papeline_openalex::Entity::Works,
        "authors" => papeline_openalex::Entity::Authors,
        "sources" => papeline_openalex::Entity::Sources,
        "institutions" => papeline_openalex::Entity::Institutions,
        "publishers" => papeline_openalex::Entity::Publishers,
        "topics" => papeline_openalex::Entity::Topics,
        "funders" => papeline_openalex::Entity::Funders,
        other => anyhow::bail!("unknown openalex entity: {other}"),
    };

    let since = cfg
        .since
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .with_context(|| format!("invalid date: {s}"))
        })
        .transpose()?;

    let oa_config = papeline_openalex::Config {
        entity,
        since,
        output_dir: output_dir.to_path_buf(),
        max_shards: cfg.limit,
        workers,
        zstd_level: cfg.zstd_level.unwrap_or(run_config.zstd_level),
    };

    let summary = papeline_openalex::run(&oa_config)?;
    log::info!(
        "openalex: {}/{} shards, {} rows",
        summary.completed_shards,
        summary.total_shards,
        summary.total_rows
    );
    if summary.failed_shards > 0 {
        anyhow::bail!("openalex: {} shards failed", summary.failed_shards);
    }
    Ok(())
}

fn run_s2(
    run_config: &RunConfig,
    _config: &Config,
    output_dir: &std::path::Path,
    workers: usize,
    release_id: &str,
) -> Result<()> {
    let cfg = run_config.s2.as_ref().unwrap();
    let datasets = cfg.datasets.clone().unwrap_or_else(|| {
        vec![
            "papers".into(),
            "abstracts".into(),
            "tldrs".into(),
            "citations".into(),
        ]
    });

    let s2_fetch_args = papeline_semantic_scholar::FetchArgs {
        release: Some(release_id.to_string()),
        url_dir: None,
        domains: cfg.domains.clone(),
        datasets,
        output_dir: output_dir.to_path_buf(),
        workers,
        max_shards: cfg.limit,
        zstd_level: cfg.zstd_level.unwrap_or(run_config.zstd_level),
    };

    let s2_config = papeline_semantic_scholar::Config::try_from(s2_fetch_args)?;
    let progress = Arc::new(papeline_core::ProgressContext::new());
    let exit_code = papeline_semantic_scholar::run(&s2_config, progress)?;

    if exit_code != std::process::ExitCode::SUCCESS {
        anyhow::bail!("s2 pipeline failed");
    }
    Ok(())
}
