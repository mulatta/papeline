//! URL expiry detection for S3 pre-signed URLs.
//!
//! S3 pre-signed URLs contain expiration information:
//! - SigV4: `X-Amz-Date=20250220T120000Z&X-Amz-Expires=3600`
//! - SigV2: `Expires=1740052800` (Unix timestamp)
//!
//! Note: URL signature expiry and AWS STS token expiry are separate.
//! Use `test_url_validity` for actual validation when STS expiry is suspected.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use papeline_core::stream::{SHARED_RUNTIME, http_client};

/// Margin before actual expiry to consider URL as "expiring soon"
/// 30 minutes to account for long-running phases
pub const EXPIRY_MARGIN: Duration = Duration::from_secs(1800); // 30 minutes

/// Extract expiry time from S3 pre-signed URL.
///
/// Returns `None` if URL doesn't contain recognizable expiry parameters.
pub fn get_expiry(url: &str) -> Option<SystemTime> {
    let query = url.split('?').nth(1)?;

    // Try SigV4 first: X-Amz-Date + X-Amz-Expires
    let amz_date = extract_param(query, "X-Amz-Date");
    let amz_expires = extract_param(query, "X-Amz-Expires");

    if let (Some(date_str), Some(expires_str)) = (amz_date, amz_expires) {
        if let (Some(start), Ok(duration_secs)) =
            (parse_amz_date(date_str), expires_str.parse::<u64>())
        {
            return Some(start + Duration::from_secs(duration_secs));
        }
    }

    // Try SigV2: Expires (Unix timestamp)
    if let Some(expires_str) = extract_param(query, "Expires") {
        if let Ok(ts) = expires_str.parse::<u64>() {
            return Some(UNIX_EPOCH + Duration::from_secs(ts));
        }
    }

    None
}

/// Check if URL is expiring soon (within margin).
///
/// Returns `false` if expiry cannot be determined (assume valid).
pub fn is_expiring_soon(url: &str, margin: Duration) -> bool {
    match get_expiry(url) {
        Some(expiry) => {
            let now = SystemTime::now();
            let threshold = now + margin;
            threshold >= expiry
        }
        None => false, // Can't determine expiry, assume valid
    }
}

/// Get remaining time until URL expires.
pub fn time_until_expiry(url: &str) -> Option<Duration> {
    let expiry = get_expiry(url)?;
    expiry.duration_since(SystemTime::now()).ok()
}

/// Test URL validity by making a small GET request.
///
/// This catches both URL signature expiry AND AWS STS token expiry.
/// Returns true if URL is valid, false if expired/invalid.
pub fn test_url_validity(url: &str) -> bool {
    // Use Range header to request just 1 byte (minimal overhead)
    let result: Result<reqwest::Response, reqwest::Error> =
        SHARED_RUNTIME.handle().block_on(async {
            http_client()
                .get(url)
                .header("Range", "bytes=0-0")
                .send()
                .await
        });

    match result {
        Ok(resp) => {
            // 200 OK or 206 Partial Content = valid
            let status = resp.status().as_u16();
            if status == 200 || status == 206 {
                true
            } else {
                // 400 = STS token expired
                // 403 = URL signature expired or access denied
                // 410 = URL expired (some S3 configs)
                log::debug!("URL validity test failed: HTTP {status}");
                false
            }
        }
        Err(e) => {
            // Check if it's a status error
            if let Some(status) = e.status() {
                log::debug!("URL validity test failed: HTTP {}", status.as_u16());
                false
            } else {
                // Network error - assume valid (don't refresh on network issues)
                log::debug!("URL validity test network error: {e}");
                true
            }
        }
    }
}

/// Extract query parameter value from query string.
fn extract_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(v);
            }
        }
    }
    None
}

/// Parse AWS date format: 20250220T120000Z
fn parse_amz_date(s: &str) -> Option<SystemTime> {
    if s.len() < 15 {
        return None;
    }

    let year: i32 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(4..6)?.parse().ok()?;
    let day: u32 = s.get(6..8)?.parse().ok()?;
    let hour: u32 = s.get(9..11)?.parse().ok()?;
    let min: u32 = s.get(11..13)?.parse().ok()?;
    let sec: u32 = s.get(13..15)?.parse().ok()?;

    // Convert to Unix timestamp (simplified, assumes UTC)
    // Days since Unix epoch for given year/month/day
    let days = days_since_epoch(year, month, day)?;
    let secs = days as u64 * 86400 + hour as u64 * 3600 + min as u64 * 60 + sec as u64;

    Some(UNIX_EPOCH + Duration::from_secs(secs))
}

/// Calculate days since Unix epoch (1970-01-01) for a given date.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Days in each month (non-leap year)
    const DAYS_IN_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let is_leap = |y: i32| (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);

    // Days from years
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }

    // Days from months
    for m in 1..month {
        let mut d = DAYS_IN_MONTH[(m - 1) as usize] as i64;
        if m == 2 && is_leap(year) {
            d += 1;
        }
        days += d;
    }

    // Days
    days += (day - 1) as i64;

    Some(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_param_found() {
        let query = "X-Amz-Date=20250220T120000Z&X-Amz-Expires=3600&X-Amz-Signature=abc";
        assert_eq!(extract_param(query, "X-Amz-Date"), Some("20250220T120000Z"));
        assert_eq!(extract_param(query, "X-Amz-Expires"), Some("3600"));
    }

    #[test]
    fn extract_param_not_found() {
        let query = "foo=bar&baz=qux";
        assert_eq!(extract_param(query, "X-Amz-Date"), None);
    }

    #[test]
    fn parse_amz_date_valid() {
        let dt = parse_amz_date("20250220T120000Z").unwrap();
        let duration = dt.duration_since(UNIX_EPOCH).unwrap();
        // 2025-02-20 12:00:00 UTC
        // Approximate check (within a day)
        let expected_approx = 1740052800u64; // rough estimate
        let actual = duration.as_secs();
        assert!((actual as i64 - expected_approx as i64).abs() < 86400);
    }

    #[test]
    fn parse_amz_date_invalid() {
        assert!(parse_amz_date("invalid").is_none());
        assert!(parse_amz_date("2025").is_none());
    }

    #[test]
    fn get_expiry_sigv4() {
        let url = "https://s3.amazonaws.com/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20250220T120000Z&X-Amz-Expires=3600&X-Amz-Signature=abc";
        let expiry = get_expiry(url).unwrap();
        // Should be 1 hour after 2025-02-20 12:00:00
        let start = parse_amz_date("20250220T120000Z").unwrap();
        let expected = start + Duration::from_secs(3600);
        assert_eq!(expiry, expected);
    }

    #[test]
    fn get_expiry_sigv2() {
        let url = "https://s3.amazonaws.com/bucket/key?AWSAccessKeyId=xxx&Expires=1740056400&Signature=yyy";
        let expiry = get_expiry(url).unwrap();
        let expected = UNIX_EPOCH + Duration::from_secs(1740056400);
        assert_eq!(expiry, expected);
    }

    #[test]
    fn get_expiry_no_params() {
        let url = "https://example.com/file.gz";
        assert!(get_expiry(url).is_none());
    }

    #[test]
    fn is_expiring_soon_true() {
        // URL that expired in the past
        let url = "https://s3.amazonaws.com/bucket/key?Expires=1000000000";
        assert!(is_expiring_soon(url, Duration::from_secs(0)));
    }

    #[test]
    fn is_expiring_soon_unknown() {
        // No expiry info -> assume not expiring
        let url = "https://example.com/file.gz";
        assert!(!is_expiring_soon(url, Duration::from_secs(3600)));
    }

    #[test]
    fn days_since_epoch_basic() {
        // 1970-01-01 should be 0
        assert_eq!(days_since_epoch(1970, 1, 1), Some(0));
        // 1970-01-02 should be 1
        assert_eq!(days_since_epoch(1970, 1, 2), Some(1));
        // 1971-01-01 should be 365
        assert_eq!(days_since_epoch(1971, 1, 1), Some(365));
    }

    #[test]
    fn days_since_epoch_leap_year() {
        // 2000 is a leap year
        // 2000-03-01 vs 1999-03-01 should differ by 366
        let d2000 = days_since_epoch(2000, 3, 1).unwrap();
        let d1999 = days_since_epoch(1999, 3, 1).unwrap();
        assert_eq!(d2000 - d1999, 366);
    }
}
