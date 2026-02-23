//! `papeline store` - manage content-addressable store

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Args, Subcommand};
use comfy_table::{Cell, Color, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use papeline_store::Store;

#[derive(Args, Debug)]
pub struct StoreArgs {
    #[command(subcommand)]
    pub action: StoreAction,
}

#[derive(Subcommand, Debug)]
pub enum StoreAction {
    /// List cached stage outputs
    List {
        /// Data directory (containing store/)
        #[arg(short, long)]
        dir: PathBuf,
    },
    /// Remove unreferenced cache entries
    Gc {
        /// Data directory (containing store/)
        #[arg(short, long)]
        dir: PathBuf,

        /// Actually delete (otherwise dry-run)
        #[arg(long)]
        confirm: bool,
    },
    /// Verify content hashes
    Verify {
        /// Data directory (containing store/)
        #[arg(short, long)]
        dir: PathBuf,

        /// Specific input hash to verify (default: all)
        hash: Option<String>,
    },
}

pub fn run(args: StoreArgs) -> Result<()> {
    match args.action {
        StoreAction::List { dir } => list(&dir),
        StoreAction::Gc { dir, confirm } => gc(&dir, confirm),
        StoreAction::Verify { dir, hash } => verify(&dir, hash.as_deref()),
    }
}

fn short(hash: &str) -> &str {
    &hash[..std::cmp::min(8, hash.len())]
}

fn list(dir: &Path) -> Result<()> {
    let store = Store::new(dir)?;
    let entries = store.list()?;

    if entries.is_empty() {
        eprintln!("No cached entries.");
        return Ok(());
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Hash").fg(Color::Cyan),
            Cell::new("Stage").fg(Color::Cyan),
            Cell::new("Files").fg(Color::Cyan),
            Cell::new("Content").fg(Color::Cyan),
            Cell::new("Created").fg(Color::Cyan),
            Cell::new("Ref").fg(Color::Cyan),
        ]);

    for entry in &entries {
        let ref_cell = if entry.referenced {
            Cell::new("yes").fg(Color::Green)
        } else {
            Cell::new("no").fg(Color::DarkGrey)
        };
        table.add_row(vec![
            Cell::new(&entry.input_hash),
            Cell::new(entry.stage),
            Cell::new(entry.file_count),
            Cell::new(short(&entry.content_hash)),
            Cell::new(&entry.created_at),
            ref_cell,
        ]);
    }

    eprintln!("\n{table}");
    eprintln!("{} entries total", entries.len());
    Ok(())
}

fn gc(dir: &Path, confirm: bool) -> Result<()> {
    let store = Store::new(dir)?;

    if !confirm {
        let entries = store.list()?;
        let unreferenced: Vec<_> = entries.iter().filter(|e| !e.referenced).collect();

        if unreferenced.is_empty() {
            eprintln!("No unreferenced entries to remove.");
        } else {
            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL)
                .apply_modifier(UTF8_ROUND_CORNERS)
                .set_header(vec![
                    Cell::new("Hash").fg(Color::Cyan),
                    Cell::new("Stage").fg(Color::Cyan),
                ]);

            for entry in &unreferenced {
                table.add_row(vec![Cell::new(&entry.input_hash), Cell::new(entry.stage)]);
            }

            eprintln!(
                "\nWould remove {} unreferenced entries:",
                unreferenced.len()
            );
            eprintln!("{table}");
            eprintln!("Run with --confirm to actually delete.");
        }
        return Ok(());
    }

    let removed = store.gc()?;
    if removed.is_empty() {
        eprintln!("Nothing to clean up.");
    } else {
        eprintln!("Removed {} entries:", removed.len());
        for hash in &removed {
            eprintln!("  {hash}");
        }
    }
    Ok(())
}

fn verify(dir: &Path, hash: Option<&str>) -> Result<()> {
    let store = Store::new(dir)?;

    if let Some(h) = hash {
        let results = store.verify(h)?;
        print_verify_results(h, &results);
    } else {
        let all = store.verify_all()?;
        if all.is_empty() {
            eprintln!("No entries to verify.");
            return Ok(());
        }

        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_header(vec![
                Cell::new("Hash").fg(Color::Cyan),
                Cell::new("Files").fg(Color::Cyan),
                Cell::new("Status").fg(Color::Cyan),
            ]);

        let mut all_ok = true;
        for (h, results) in &all {
            let ok = results.iter().all(|r| r.ok);
            if !ok {
                all_ok = false;
            }
            let status_cell = if ok {
                Cell::new("OK").fg(Color::Green)
            } else {
                Cell::new("FAIL").fg(Color::Red)
            };
            table.add_row(vec![Cell::new(h), Cell::new(results.len()), status_cell]);
        }

        eprintln!("\n{table}");

        // Show mismatch details after summary table
        for (h, results) in &all {
            print_mismatches(h, results);
        }

        if all_ok {
            eprintln!("All entries verified OK.");
        } else {
            eprintln!("Some entries have integrity issues!");
        }
    }
    Ok(())
}

fn print_verify_results(hash: &str, results: &[papeline_store::store::VerifyResult]) -> bool {
    let all_ok = results.iter().all(|r| r.ok);
    let status = if all_ok { "OK" } else { "FAIL" };
    eprintln!("[{status}] {hash} ({} files)", results.len());

    print_mismatches(hash, results);

    all_ok
}

fn print_mismatches(hash: &str, results: &[papeline_store::store::VerifyResult]) {
    for r in results {
        if !r.ok {
            eprintln!("  MISMATCH in {hash}: {}", r.path);
            eprintln!("    expected: {}", short(&r.expected));
            eprintln!("    actual:   {}", short(&r.actual));
        }
    }
}
