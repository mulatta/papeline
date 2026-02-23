//! `papeline store` - manage content-addressable store

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Args, Subcommand};

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

fn list(dir: &Path) -> Result<()> {
    let store = Store::new(dir)?;
    let entries = store.list()?;

    if entries.is_empty() {
        println!("No cached entries.");
        return Ok(());
    }

    println!(
        "{:<10} {:<10} {:<6} {:<10} {:<18} Ref",
        "Hash", "Stage", "Files", "Content", "Created"
    );
    println!("{}", "-".repeat(70));

    for entry in &entries {
        let ref_str = if entry.referenced { "yes" } else { "no" };
        println!(
            "{:<10} {:<10} {:<6} {:<10} {:<18} {ref_str}",
            entry.input_hash,
            format!("{:?}", entry.stage).to_lowercase(),
            entry.file_count,
            entry.content_hash,
            entry.created_at,
        );
    }

    println!("\n{} entries total", entries.len());
    Ok(())
}

fn gc(dir: &Path, confirm: bool) -> Result<()> {
    let store = Store::new(dir)?;

    if !confirm {
        // Dry-run: show what would be removed
        let entries = store.list()?;
        let unreferenced: Vec<_> = entries.iter().filter(|e| !e.referenced).collect();

        if unreferenced.is_empty() {
            println!("No unreferenced entries to remove.");
        } else {
            println!("Would remove {} unreferenced entries:", unreferenced.len());
            for entry in &unreferenced {
                println!("  {} ({:?})", entry.input_hash, entry.stage);
            }
            println!("\nRun with --confirm to actually delete.");
        }
        return Ok(());
    }

    let removed = store.gc()?;
    if removed.is_empty() {
        println!("Nothing to clean up.");
    } else {
        println!("Removed {} entries:", removed.len());
        for hash in &removed {
            println!("  {hash}");
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
            println!("No entries to verify.");
            return Ok(());
        }
        let mut all_ok = true;
        for (h, results) in &all {
            let ok = print_verify_results(h, results);
            if !ok {
                all_ok = false;
            }
        }
        if all_ok {
            println!("\nAll entries verified OK.");
        } else {
            println!("\nSome entries have integrity issues!");
        }
    }
    Ok(())
}

fn print_verify_results(hash: &str, results: &[papeline_store::store::VerifyResult]) -> bool {
    let all_ok = results.iter().all(|r| r.ok);
    let status = if all_ok { "OK" } else { "FAIL" };
    println!("[{status}] {hash}");

    for r in results {
        if !r.ok {
            println!("  MISMATCH: {}", r.path);
            println!("    expected: {}", &r.expected[..16]);
            println!(
                "    actual:   {}",
                &r.actual[..std::cmp::min(16, r.actual.len())]
            );
        }
    }

    all_ok
}
