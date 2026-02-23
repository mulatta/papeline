//! `papeline run` - orchestrate full pipeline from run.toml

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use comfy_table::{Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use indicatif::ProgressBar;

use papeline_core::SharedProgress;
use papeline_store::run_config::{
    Defaults, JoinStageConfig, OpenAlexStageConfig, PubmedStageConfig, S2StageConfig,
};
use papeline_store::stage::StageName;
use papeline_store::store::LookupResult;
use papeline_store::{RunConfig, StageInput, StageManifest, Store};

use crate::config::Config;

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Path to run.toml (optional)
    pub run_config: Option<PathBuf>,

    /// Force re-run all stages (ignore cache)
    #[arg(long)]
    pub force: bool,

    /// Show stage status without executing
    #[arg(long)]
    pub dry_run: bool,

    /// Number of rayon pool threads
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Maximum concurrent downloads (default: workers * 2)
    #[arg(long)]
    pub max_downloads: Option<usize>,

    /// Output directory
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Zstd compression level (1-22, default from run.toml or 3)
    #[arg(long)]
    pub zstd_level: Option<i32>,

    #[command(flatten)]
    pub pm: PubmedFlags,

    #[command(flatten)]
    pub oa: OpenAlexFlags,

    #[command(flatten)]
    pub s2: S2Flags,

    #[command(flatten)]
    pub join_flags: JoinFlags,
}

#[derive(Args, Debug)]
#[command(next_help_heading = "PubMed")]
pub struct PubmedFlags {
    /// Enable PubMed stage
    #[arg(long)]
    pub pm: bool,
    /// Maximum files to process
    #[arg(long)]
    pub pm_limit: Option<usize>,
    /// PubMed FTP base URL
    #[arg(long)]
    pub pm_base_url: Option<String>,
    /// Include daily updatefiles in addition to baseline
    #[arg(long)]
    pub pm_updates: bool,
}

impl PubmedFlags {
    fn is_active(&self) -> bool {
        self.pm || self.pm_limit.is_some() || self.pm_base_url.is_some() || self.pm_updates
    }
}

#[derive(Args, Debug)]
#[command(next_help_heading = "OpenAlex")]
pub struct OpenAlexFlags {
    /// Enable OpenAlex stage
    #[arg(long)]
    pub oa: bool,
    /// Entity type (works, authors, ...)
    #[arg(long)]
    pub oa_entity: Option<String>,
    /// Only records updated since date (YYYY-MM-DD)
    #[arg(long)]
    pub oa_since: Option<String>,
    /// Maximum shards
    #[arg(long)]
    pub oa_limit: Option<usize>,
    /// Topic domain filter (comma-separated, e.g. "Health Sciences,Life Sciences")
    #[arg(long, value_delimiter = ',')]
    pub oa_domains: Option<Vec<String>>,
    /// Topic field filter (comma-separated, e.g. "Medicine,Biology")
    #[arg(long, value_delimiter = ',')]
    pub oa_fields: Option<Vec<String>>,
    /// Topic ID filter (comma-separated, e.g. "T10978,T12345")
    #[arg(long, value_delimiter = ',')]
    pub oa_topics: Option<Vec<String>>,
}

impl OpenAlexFlags {
    fn is_active(&self) -> bool {
        self.oa
            || self.oa_entity.is_some()
            || self.oa_since.is_some()
            || self.oa_limit.is_some()
            || self.oa_domains.is_some()
            || self.oa_fields.is_some()
            || self.oa_topics.is_some()
    }
}

#[derive(Args, Debug)]
#[command(next_help_heading = "Semantic Scholar")]
pub struct S2Flags {
    /// Enable S2 stage
    #[arg(long)]
    pub s2: bool,
    /// FoS domain filter (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub s2_domains: Option<Vec<String>>,
    /// Release ID or "latest"
    #[arg(long)]
    pub s2_release: Option<String>,
    /// Datasets (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub s2_datasets: Option<Vec<String>>,
    /// Maximum shards
    #[arg(long)]
    pub s2_limit: Option<usize>,
}

impl S2Flags {
    fn is_active(&self) -> bool {
        self.s2
            || self.s2_domains.is_some()
            || self.s2_release.is_some()
            || self.s2_datasets.is_some()
            || self.s2_limit.is_some()
    }
}

#[derive(Args, Debug)]
#[command(next_help_heading = "Join")]
pub struct JoinFlags {
    /// DuckDB memory limit (e.g. "16GB")
    #[arg(long)]
    pub join_memory: Option<String>,
}

impl JoinFlags {
    fn is_active(&self) -> bool {
        self.join_memory.is_some()
    }
}

/// Stage execution plan entry.
struct StagePlan {
    name: StageName,
    input: StageInput,
    status: StageStatus,
    manifest: Option<StageManifest>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StageStatus {
    Cached,
    NeedsRun,
    Pending,
}

impl StageStatus {
    fn cell(self) -> Cell {
        match self {
            Self::Cached => Cell::new("cached").fg(Color::Green),
            Self::NeedsRun => Cell::new("run").fg(Color::Yellow),
            Self::Pending => Cell::new("pending").fg(Color::DarkGrey),
        }
    }

    fn ansi(self) -> &'static str {
        match self {
            Self::Cached => "\x1b[32mcached\x1b[0m",
            Self::NeedsRun => "\x1b[33mrun\x1b[0m",
            Self::Pending => "\x1b[2mpending\x1b[0m",
        }
    }
}

/// Format a stage line message: `{input_hash}  {content_hash}  {status}`
fn stage_msg(plan: &StagePlan) -> String {
    let input = if plan.status == StageStatus::Pending {
        "-".to_string()
    } else {
        plan.input.short_hash()
    };
    let content = plan
        .manifest
        .as_ref()
        .map(|m| m.content_hash[..8].to_string())
        .unwrap_or_else(|| "-".into());
    format!("{input:<10} {content:<10} {}", plan.status.ansi())
}

fn stage_msg_running(plan: &StagePlan) -> String {
    let input = plan.input.short_hash();
    format!("{input:<10} {:<10} \x1b[33mrunning\x1b[0m", "-")
}

fn stage_msg_done(plan: &StagePlan) -> String {
    let input = plan.input.short_hash();
    let content = plan
        .manifest
        .as_ref()
        .map(|m| m.content_hash[..8].to_string())
        .unwrap_or_else(|| "-".into());
    format!("{input:<10} {content:<10} \x1b[32mcached\x1b[0m")
}

fn apply_cli_overrides(config: &mut RunConfig, args: &RunArgs) {
    if let Some(ref output) = args.output {
        config.output = output.clone();
    }
    if let Some(v) = args.zstd_level {
        config.zstd_level = v;
    }

    // PubMed
    if args.pm.is_active() {
        let cfg = config.pubmed.get_or_insert_with(PubmedStageConfig::default);
        if let Some(v) = args.pm.pm_limit {
            cfg.limit = Some(v);
        }
        if let Some(ref v) = args.pm.pm_base_url {
            cfg.base_url = Some(v.clone());
        }
        if args.pm.pm_updates {
            cfg.include_updates = Some(true);
        }
    }

    // OpenAlex
    if args.oa.is_active() {
        let cfg = config
            .openalex
            .get_or_insert_with(OpenAlexStageConfig::default);
        if let Some(ref v) = args.oa.oa_entity {
            cfg.entity = Some(v.clone());
        }
        if let Some(ref v) = args.oa.oa_since {
            cfg.since = Some(v.clone());
        }
        if let Some(v) = args.oa.oa_limit {
            cfg.limit = Some(v);
        }
        if let Some(ref v) = args.oa.oa_domains {
            cfg.domains = v.clone();
        }
        if let Some(ref v) = args.oa.oa_fields {
            cfg.fields = v.clone();
        }
        if let Some(ref v) = args.oa.oa_topics {
            cfg.topics = v.clone();
        }
    }

    // S2
    if args.s2.is_active() {
        let cfg = config.s2.get_or_insert_with(|| S2StageConfig {
            domains: Vec::new(),
            release: None,
            datasets: None,
            limit: None,
        });
        if let Some(ref v) = args.s2.s2_domains {
            cfg.domains = v.clone();
        }
        if let Some(ref v) = args.s2.s2_release {
            cfg.release = Some(v.clone());
        }
        if let Some(ref v) = args.s2.s2_datasets {
            cfg.datasets = Some(v.clone());
        }
        if let Some(v) = args.s2.s2_limit {
            cfg.limit = Some(v);
        }
    }

    // Join
    if args.join_flags.is_active() {
        let cfg = config.join.get_or_insert_with(JoinStageConfig::default);
        if let Some(ref v) = args.join_flags.join_memory {
            cfg.memory_limit = v.clone();
        }
    }
}

pub fn run(args: RunArgs, config: &Config, progress: &SharedProgress) -> Result<()> {
    // 1. Load RunConfig (from file or empty)
    let mut run_config = match &args.run_config {
        Some(path) => RunConfig::from_file(path)
            .with_context(|| format!("failed to load {}", path.display()))?,
        None => RunConfig::empty(),
    };

    // 2. Apply CLI overrides
    apply_cli_overrides(&mut run_config, &args);

    // 3. Validate
    run_config.validate()?;

    if run_config.active_fetch_stages().is_empty() {
        anyhow::bail!("no stages enabled; use --pm, --oa, --s2 flags or provide a run.toml");
    }

    let defaults = Defaults {
        pubmed_base_url: config.pubmed.base_url.clone(),
    };

    let workers = args.workers.unwrap_or(config.workers.default);
    let max_downloads = args.max_downloads.unwrap_or(workers * 2);

    // Initialize global rayon pool and download semaphore
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build_global()
        .context("Failed to set global thread pool")?;
    papeline_core::set_download_concurrency(max_downloads);
    log::info!("Pool: {workers} threads, max downloads: {max_downloads}");

    // 2. Resolve S2 release if needed
    let s2_release_id = if run_config.s2.is_some() {
        let release_str = run_config.s2_release().unwrap();
        if release_str == "latest" {
            let api_key = std::env::var("S2_API_KEY")
                .context("S2_API_KEY required to resolve 'latest' release")?;
            let id = papeline_semantic_scholar::api::resolve_release(&release_str, &api_key)?;
            log::info!("Resolved S2 release 'latest' -> {id}");
            Some(id)
        } else {
            Some(release_str)
        }
    } else {
        None
    };

    // 3. Resolve PubMed manifest (pre-fetch for baseline_year + seq_end)
    let pm_manifest_data = if run_config.pubmed.is_some() {
        let base_url = run_config
            .pubmed
            .as_ref()
            .unwrap()
            .base_url
            .clone()
            .unwrap_or_else(|| defaults.pubmed_base_url.clone());
        let include_updates = run_config.pubmed_include_updates();
        log::info!(
            "Fetching PubMed manifest from {base_url} (updates={})",
            include_updates
        );
        let (entries, year, seq_end) =
            papeline_pubmed::manifest::fetch_combined_manifest(&base_url, include_updates)?;
        log::info!(
            "PubMed baseline year={year}, seq_end={seq_end}, {} files",
            entries.len()
        );
        Some((entries, year, seq_end))
    } else {
        None
    };

    // 4. Build stage inputs for fetch stages
    let store = Store::new(&run_config.output)?;
    let mut fetch_plans: Vec<StagePlan> = Vec::new();

    // Pre-fetched PubMed entries to pass to runner (avoids double fetch)
    let mut pm_entries: Option<Vec<papeline_pubmed::manifest::ManifestEntry>> = None;

    if let Some((entries, year, seq_end)) = pm_manifest_data {
        if let Some(input) = run_config.pubmed_input(&defaults, year, seq_end) {
            let (status, manifest) = check_cache(&store, &input, args.force);
            if status != StageStatus::Cached {
                pm_entries = Some(entries);
            }
            fetch_plans.push(StagePlan {
                name: StageName::Pubmed,
                input,
                status,
                manifest,
            });
        }
    }

    if let Some(input) = run_config.openalex_input() {
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

    // 4. Compute join plan if possible (even for dry-run)
    let mut join_plan: Option<StagePlan> = None;
    if run_config.should_join() {
        // Try to compute join input hash from cached fetch manifests
        let all_cached = fetch_plans.iter().all(|p| p.manifest.is_some());

        if all_cached {
            let pm_hash = get_content_hash(&fetch_plans, StageName::Pubmed)?;
            let oa_hash = get_content_hash(&fetch_plans, StageName::Openalex)?;
            let s2_hash = get_content_hash(&fetch_plans, StageName::S2)?;

            let join_input = run_config
                .join_input(&pm_hash, &oa_hash, &s2_hash)
                .expect("should_join() was true");

            let (status, manifest) = check_cache(&store, &join_input, args.force);
            join_plan = Some(StagePlan {
                name: StageName::Join,
                input: join_input,
                status,
                manifest,
            });
        } else {
            // Can't compute join hash yet — some fetch stages need to run first
            // Use a dummy input for display purposes
            let dummy_input = run_config
                .join_input("(pending)", "(pending)", "(pending)")
                .expect("should_join() was true");
            join_plan = Some(StagePlan {
                name: StageName::Join,
                input: dummy_input,
                status: StageStatus::Pending,
                manifest: None,
            });
        }
    }

    // 5. Display plan
    if args.dry_run {
        print_plan_table(&fetch_plans, join_plan.as_ref());
        return Ok(());
    }

    // Create live stage lines (stop spinner immediately for cached stages)
    let fetch_lines: Vec<ProgressBar> = fetch_plans
        .iter()
        .map(|p| {
            let pb = progress.stage_line(&p.name.to_string());
            pb.set_message(stage_msg(p));
            if p.status == StageStatus::Cached {
                pb.finish();
            }
            pb
        })
        .collect();
    let join_line: Option<ProgressBar> = join_plan.as_ref().map(|jp| {
        let pb = progress.stage_line(&jp.name.to_string());
        pb.set_message(stage_msg(jp));
        if jp.status != StageStatus::NeedsRun {
            pb.finish();
        }
        pb
    });

    // 6. Execute fetch stages in parallel
    let run_indices: Vec<usize> = fetch_plans
        .iter()
        .enumerate()
        .filter(|(_, p)| p.status != StageStatus::Cached)
        .map(|(i, _)| i)
        .collect();

    for &i in &run_indices {
        fetch_lines[i].enable_steady_tick(std::time::Duration::from_millis(80));
        fetch_lines[i].set_message(stage_msg_running(&fetch_plans[i]));
    }

    if !run_indices.is_empty() {
        // Rebind as shared refs so move closures capture references, not owned values
        let store_ref = &store;
        let rc_ref = &run_config;
        let s2_ref = &s2_release_id;
        // Wrap in Mutex so we can take() from one thread inside the scope
        let pm_entries = std::sync::Mutex::new(pm_entries);

        let results: Vec<Result<(usize, StageManifest)>> = std::thread::scope(|scope| {
            let handles: Vec<_> = run_indices
                .iter()
                .map(|&i| {
                    let plan = &fetch_plans[i];
                    let pb = fetch_lines[i].clone();
                    let tmp_dir = store_ref.stage_tmp_dir(&plan.input);
                    let pm_entries_ref = &pm_entries;

                    scope.spawn(move || -> Result<(usize, StageManifest)> {
                        if tmp_dir.exists() {
                            std::fs::remove_dir_all(&tmp_dir)?;
                        }
                        std::fs::create_dir_all(&tmp_dir)?;

                        match plan.name {
                            StageName::Pubmed => {
                                let entries = pm_entries_ref.lock().unwrap().take();
                                run_pubmed(rc_ref, config, &tmp_dir, entries, progress)?;
                            }
                            StageName::Openalex => {
                                run_openalex(rc_ref, config, &tmp_dir, progress)?;
                            }
                            StageName::S2 => {
                                let release_id = s2_ref.as_deref().unwrap();
                                run_s2(rc_ref, config, &tmp_dir, release_id, progress)?;
                            }
                            StageName::Join => unreachable!("join handled separately"),
                        }

                        let manifest = store_ref.commit_stage(&plan.input, &tmp_dir, false)?;

                        let ih = plan.input.short_hash();
                        let ch = &manifest.content_hash[..8];
                        pb.set_message(format!("{ih:<10} {ch:<10} \x1b[32mcached\x1b[0m"));
                        pb.finish();

                        Ok((i, manifest))
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().expect("stage thread panicked"))
                .collect()
        });

        let mut first_error: Option<anyhow::Error> = None;
        for result in results {
            match result {
                Ok((i, manifest)) => {
                    fetch_plans[i].manifest = Some(manifest);
                    fetch_plans[i].status = StageStatus::Cached;
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
    }

    // 7. Join stage (recompute hash now that all fetches are done)
    if run_config.should_join() {
        let pm_hash = get_content_hash(&fetch_plans, StageName::Pubmed)?;
        let oa_hash = get_content_hash(&fetch_plans, StageName::Openalex)?;
        let s2_hash = get_content_hash(&fetch_plans, StageName::S2)?;

        let join_input = run_config
            .join_input(&pm_hash, &oa_hash, &s2_hash)
            .expect("should_join() was true");

        let (status, manifest) = check_cache(&store, &join_input, args.force);

        if status == StageStatus::Cached {
            join_plan = Some(StagePlan {
                name: StageName::Join,
                input: join_input,
                status: StageStatus::Cached,
                manifest,
            });
            if let Some(ref jl) = join_line {
                jl.set_message(stage_msg(join_plan.as_ref().unwrap()));
                jl.finish();
            }
        } else {
            // Update join line: now we know the real hash
            let mut jp = StagePlan {
                name: StageName::Join,
                input: join_input.clone(),
                status: StageStatus::NeedsRun,
                manifest: None,
            };
            if let Some(ref jl) = join_line {
                jl.enable_steady_tick(std::time::Duration::from_millis(80));
                jl.set_message(stage_msg_running(&jp));
            }

            let tmp_dir = store.stage_tmp_dir(&join_input);
            if tmp_dir.exists() {
                std::fs::remove_dir_all(&tmp_dir)?;
            }
            std::fs::create_dir_all(&tmp_dir)?;

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

            let jl_ref = join_line.as_ref();
            let summary = papeline_join::run_with_progress(&join_config, |step, desc| {
                if let Some(jl) = jl_ref {
                    jl.set_message(format!(
                        "{:<10} {:<10} \x1b[33mstep {step}/9: {desc}\x1b[0m",
                        jp.input.short_hash(),
                        "-"
                    ));
                }
            })?;
            log::info!(
                "join: {} nodes, {} OA ({} DOI + {} PMID), {} S2 ({} DOI + {} PMID), {} citations",
                summary.total_nodes,
                summary.openalex_matched,
                summary.openalex_doi,
                summary.openalex_pmid,
                summary.s2_matched,
                summary.s2_doi,
                summary.s2_pmid,
                summary.citations_exported,
            );

            let manifest = store.commit_stage(&join_input, &tmp_dir, false)?;
            jp.manifest = Some(manifest);
            jp.status = StageStatus::Cached;

            if let Some(ref jl) = join_line {
                jl.set_message(stage_msg_done(&jp));
                jl.finish();
            }

            join_plan = Some(jp);
        }
    }

    // 8. Create run entry
    let mut all_stages: Vec<(StageName, &StageInput, &StageManifest, bool)> = Vec::new();
    for plan in &fetch_plans {
        let was_cached = plan.status == StageStatus::Cached;
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
            jp.status == StageStatus::Cached,
        ));
    }

    let run_dir = store.create_run(&all_stages)?;

    // 9. Finish stage lines and print run directory
    for pb in &fetch_lines {
        pb.finish();
    }
    if let Some(ref jl) = join_line {
        jl.finish();
    }
    eprintln!("Run: {}", run_dir.display());

    Ok(())
}

fn print_plan_table(fetch_plans: &[StagePlan], join_plan: Option<&StagePlan>) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Stage").fg(Color::Cyan),
            Cell::new("Input").fg(Color::Cyan),
            Cell::new("Content").fg(Color::Cyan),
            Cell::new("Status").fg(Color::Cyan),
        ]);

    for plan in fetch_plans {
        let content = plan
            .manifest
            .as_ref()
            .map(|m| m.content_hash[..8].to_string())
            .unwrap_or_else(|| "-".into());
        table.add_row(vec![
            Cell::new(plan.name),
            Cell::new(plan.input.short_hash()),
            Cell::new(content),
            plan.status.cell(),
        ]);
    }

    if let Some(jp) = join_plan {
        let (hash_str, content) = if jp.status == StageStatus::Pending {
            ("-".into(), "-".into())
        } else {
            let content = jp
                .manifest
                .as_ref()
                .map(|m| m.content_hash[..8].to_string())
                .unwrap_or_else(|| "-".into());
            (jp.input.short_hash(), content)
        };
        table.add_row(vec![
            Cell::new(jp.name),
            Cell::new(hash_str),
            Cell::new(content),
            jp.status.cell(),
        ]);
    }

    eprintln!("\n{table}");
}

fn get_content_hash(plans: &[StagePlan], name: StageName) -> Result<String> {
    plans
        .iter()
        .find(|p| p.name == name)
        .and_then(|p| p.manifest.as_ref())
        .map(|m| m.content_hash.clone())
        .with_context(|| format!("{name} manifest missing for join"))
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
    entries: Option<Vec<papeline_pubmed::manifest::ManifestEntry>>,
    progress: &SharedProgress,
) -> Result<()> {
    let cfg = run_config.pubmed.as_ref().unwrap();
    let pm_config = papeline_pubmed::Config {
        output_dir: output_dir.to_path_buf(),
        max_files: cfg.limit,
        zstd_level: run_config.zstd_level,
        base_url: cfg
            .base_url
            .clone()
            .unwrap_or_else(|| config.pubmed.base_url.clone()),
    };

    let summary = if let Some(entries) = entries {
        papeline_pubmed::run_with_entries(&pm_config, entries, progress.clone())?
    } else {
        papeline_pubmed::run(&pm_config, progress.clone())?
    };
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
    progress: &SharedProgress,
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

    let topic_filter = papeline_openalex::TopicFilter::new(
        cfg.domains.clone(),
        cfg.fields.clone(),
        cfg.topics.clone(),
    );
    let oa_config = papeline_openalex::Config {
        entity,
        since,
        output_dir: output_dir.to_path_buf(),
        max_shards: cfg.limit,
        zstd_level: run_config.zstd_level,
        topic_filter,
    };

    let summary = papeline_openalex::run(&oa_config, progress.clone())?;
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
    release_id: &str,
    progress: &SharedProgress,
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
        max_shards: cfg.limit,
        zstd_level: run_config.zstd_level,
    };

    let s2_config = papeline_semantic_scholar::Config::try_from(s2_fetch_args)?;
    let exit_code = papeline_semantic_scholar::run(&s2_config, progress.clone())?;

    if exit_code != std::process::ExitCode::SUCCESS {
        anyhow::bail!("s2 pipeline failed");
    }
    Ok(())
}
