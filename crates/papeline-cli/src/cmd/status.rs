//! Status subcommand (placeholder)

use std::path::PathBuf;

use anyhow::Result;
use clap::Args;

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Directory to check status
    #[arg(default_value = ".")]
    pub dir: PathBuf,
}

pub fn run(_args: StatusArgs) -> Result<()> {
    anyhow::bail!("status command not implemented")
}
