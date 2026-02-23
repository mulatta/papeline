//! Common error type for shard processing pipelines

use crate::stream::StreamError;

/// Error from processing a single data shard (download + transform).
///
/// Wraps either a network/HTTP error ([`StreamError`]) or a local I/O error.
/// Used by both OpenAlex and Semantic Scholar pipelines.
#[derive(Debug)]
pub enum ShardError {
    Stream(StreamError),
    Io(std::io::Error),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "IO: {e}"),
        }
    }
}

impl std::error::Error for ShardError {}

impl ShardError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Stream(e) => e.is_retryable(),
            Self::Io(e) => e.kind() != std::io::ErrorKind::StorageFull,
        }
    }

    /// Check if the error indicates an expired pre-signed URL.
    ///
    /// S2 uses pre-signed URLs that expire; 400/403/410 indicate this.
    pub fn is_url_expired(&self) -> bool {
        // 403/410 = URL signature expired
        // 400 = AWS STS token expired (pre-signed URL with expired temporary credentials)
        matches!(
            self,
            Self::Stream(StreamError::Http {
                status: Some(400 | 403 | 410),
                ..
            })
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    fn http_err(status: u16) -> StreamError {
        StreamError::Http {
            status: Some(status),
            message: "test".to_string(),
        }
    }

    #[test]
    fn shard_error_stream_403_not_retryable() {
        let err = ShardError::Stream(http_err(403));
        assert!(!err.is_retryable());
    }

    #[test]
    fn shard_error_stream_500_retryable() {
        let err = ShardError::Stream(http_err(500));
        assert!(err.is_retryable());
    }

    #[test]
    fn shard_error_io_storage_full_not_retryable() {
        let err = ShardError::Io(std::io::Error::new(ErrorKind::StorageFull, "disk full"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn shard_error_io_other_retryable() {
        let err = ShardError::Io(std::io::Error::new(ErrorKind::BrokenPipe, "pipe"));
        assert!(err.is_retryable());
    }

    #[test]
    fn shard_error_display_io() {
        let err = ShardError::Io(std::io::Error::new(ErrorKind::NotFound, "not found"));
        let msg = format!("{err}");
        assert!(msg.contains("IO:"));
    }

    #[test]
    fn is_url_expired_403() {
        let err = ShardError::Stream(http_err(403));
        assert!(err.is_url_expired());
    }

    #[test]
    fn is_url_expired_410() {
        let err = ShardError::Stream(http_err(410));
        assert!(err.is_url_expired());
    }

    #[test]
    fn is_url_expired_400() {
        let err = ShardError::Stream(http_err(400));
        assert!(err.is_url_expired());
    }

    #[test]
    fn is_url_expired_false_for_500() {
        let err = ShardError::Stream(http_err(500));
        assert!(!err.is_url_expired());
    }

    #[test]
    fn is_url_expired_false_for_io() {
        let err = ShardError::Io(std::io::Error::other("test"));
        assert!(!err.is_url_expired());
    }
}
