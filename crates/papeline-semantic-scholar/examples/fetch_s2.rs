//! Semantic Scholar Dataset Fetcher example
//!
//! Run with: cargo run -p papeline-semantic-scholar --example fetch -- <args>

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use clap::Parser;
use papeline_core::{ProgressContext, init_logging, shutdown_flag};
use papeline_semantic_scholar::{
    Cli, Command, Config, DatasetAction, ReleaseAction, cmd_dataset_list, cmd_release_list, run,
};

fn main() -> ExitCode {
    let cli = Cli::parse();

    let progress = Arc::new(ProgressContext::new());
    let multi = if progress.is_tty() {
        Some(progress.multi())
    } else {
        None
    };
    init_logging(cli.quiet, cli.verbose, multi);

    match cli.command {
        Command::Release { action } => match action {
            ReleaseAction::List => match cmd_release_list() {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    log::error!("{e:#}");
                    ExitCode::from(2)
                }
            },
        },
        Command::Dataset { action } => match action {
            DatasetAction::List { release } => match cmd_dataset_list(&release) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    log::error!("{e:#}");
                    ExitCode::from(2)
                }
            },
        },
        Command::Fetch(args) => {
            setup_signal_handler();
            let config = match Config::try_from(args) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Configuration error: {e:#}");
                    return ExitCode::from(2);
                }
            };
            match run(&config, progress) {
                Ok(code) => code,
                Err(e) => {
                    log::error!("Fatal error: {e:#}");
                    ExitCode::from(2)
                }
            }
        }
    }
}

fn setup_signal_handler() {
    // First signal: set graceful shutdown flag
    // Second signal: force exit (default SIGINT behavior restored)
    // SAFETY: AtomicBool::store and process::exit are async-signal-safe
    unsafe {
        signal_hook::low_level::register(signal_hook::consts::SIGTERM, || {
            if shutdown_flag().swap(true, Ordering::Relaxed) {
                std::process::exit(130);
            }
        })
        .expect("Failed to register SIGTERM handler");
        signal_hook::low_level::register(signal_hook::consts::SIGINT, || {
            if shutdown_flag().swap(true, Ordering::Relaxed) {
                std::process::exit(130);
            }
        })
        .expect("Failed to register SIGINT handler");
    }
}
