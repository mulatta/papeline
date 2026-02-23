//! Retry with exponential backoff for shard processing

use std::time::Duration;

use indicatif::ProgressBar;

use crate::error::ShardError;
use crate::stream::http_config;

/// Exponential backoff: 2^attempt seconds (2s, 4s, 8s, ...)
pub const fn backoff_duration(attempt: u32) -> Duration {
    Duration::from_secs(2u64.pow(attempt))
}

/// Retry a fallible shard operation with exponential backoff.
///
/// On retryable errors, logs the failure, updates the progress bar, sleeps,
/// and retries up to `max_retries` (from global [`HttpConfig`]).
///
/// Returns `Ok(T)` on first success, or the final `Err` on exhaustion / non-retryable error.
pub fn retry_with_backoff<T>(
    shard_label: &str,
    pb: &ProgressBar,
    mut attempt_fn: impl FnMut() -> Result<T, ShardError>,
) -> Result<T, ShardError> {
    let max_retries = http_config().max_retries;
    let mut attempt = 0u32;
    loop {
        match attempt_fn() {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_retries && e.is_retryable() => {
                attempt += 1;
                pb.set_message(format!("retry {attempt}/{max_retries}..."));
                log::debug!(
                    "{shard_label}: attempt {attempt}/{max_retries} failed: {e}, retrying..."
                );
                std::thread::sleep(backoff_duration(attempt));
            }
            Err(e) => {
                log::error!("{shard_label}: failed permanently: {e}");
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_exponential() {
        assert_eq!(backoff_duration(1), Duration::from_secs(2));
        assert_eq!(backoff_duration(2), Duration::from_secs(4));
        assert_eq!(backoff_duration(3), Duration::from_secs(8));
    }
}
