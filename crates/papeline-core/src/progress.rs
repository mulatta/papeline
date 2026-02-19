//! Progress reporting for TTY and non-TTY environments.
//!
//! TTY mode: indicatif progress bars per worker (clear on completion).
//! Non-TTY mode: log-based output (no progress bars).

use std::io::IsTerminal;
use std::sync::Arc;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// Progress bar style with `bytes`/`total_bytes` and ETA
fn bar_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "{spinner:.green} {prefix:>16.cyan.bold} {bar:12.cyan/blue} {bytes:.dim}/{total_bytes:.dim} eta {eta:.dim}  {wide_msg}",
        )
        .expect("invalid template")
        .progress_chars("▰▱")
}

/// Progress bar style as spinner (no total bytes known)
fn spinner_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{spinner:.green} {prefix:>16.cyan.bold} {bytes:.dim} {wide_msg}")
        .expect("invalid template")
        .progress_chars("▰▱")
}

/// Upgrade a progress bar to show `bytes`/`total_bytes` with bar.
///
/// Call this after `open_gzip_reader` returns `total_bytes`.
pub fn upgrade_to_bar(pb: &ProgressBar, total: u64) {
    pb.set_length(total);
    pb.set_style(bar_style());
}

/// Central progress context managing multi-progress bars.
pub struct ProgressContext {
    multi: MultiProgress,
    is_tty: bool,
}

impl ProgressContext {
    /// Create new context, detecting TTY automatically.
    pub fn new() -> Self {
        let is_tty = std::io::stderr().is_terminal();
        Self {
            multi: MultiProgress::new(),
            is_tty,
        }
    }

    /// Create shard progress bar with download progress.
    ///
    /// TTY: visible bar with bytes progress and ETA.
    /// Non-TTY: hidden (no-op).
    ///
    /// Starts as spinner. Call `upgrade_to_bar` after getting `total_bytes`
    /// from `open_gzip_reader` to show the progress bar with ETA.
    pub fn shard_bar(&self, name: &str) -> ProgressBar {
        if !self.is_tty {
            return ProgressBar::hidden();
        }

        let pb = self.multi.add(ProgressBar::new_spinner());
        pb.set_style(spinner_style());
        pb.set_prefix(name.to_string());
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        pb
    }

    /// Whether running in TTY mode.
    pub fn is_tty(&self) -> bool {
        self.is_tty
    }

    /// Get reference to `MultiProgress` for log bridge.
    pub fn multi(&self) -> &MultiProgress {
        &self.multi
    }
}

impl Default for ProgressContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe wrapper for `ProgressContext`.
pub type SharedProgress = Arc<ProgressContext>;

/// Format number with thousand separators.
pub fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_num_zero() {
        assert_eq!(fmt_num(0), "0");
    }

    #[test]
    fn fmt_num_small() {
        assert_eq!(fmt_num(1), "1");
        assert_eq!(fmt_num(12), "12");
        assert_eq!(fmt_num(123), "123");
    }

    #[test]
    fn fmt_num_thousands() {
        assert_eq!(fmt_num(1_000), "1,000");
        assert_eq!(fmt_num(1_234), "1,234");
        assert_eq!(fmt_num(12_345), "12,345");
        assert_eq!(fmt_num(123_456), "123,456");
    }

    #[test]
    fn fmt_num_millions() {
        assert_eq!(fmt_num(1_000_000), "1,000,000");
        assert_eq!(fmt_num(1_234_567), "1,234,567");
    }

    #[test]
    fn fmt_num_large() {
        assert_eq!(fmt_num(1_234_567_890), "1,234,567,890");
    }
}
