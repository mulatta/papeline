//! HTTP streaming with gzip decompression and read timeout.
//!
//! Uses async reqwest internally with tokio::time::timeout for stall detection,
//! but presents a sync interface for compatibility with rayon workers.

use std::io::{self, BufReader, Read};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::Context;
use std::time::Duration;

use flate2::read::GzDecoder;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, ReadBuf};

/// Read timeout for stall detection (10 seconds with no data = stall)
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Connect timeout
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Error types for stream operations
#[derive(Debug)]
pub enum StreamError {
    /// HTTP error with optional status code
    Http {
        status: Option<u16>,
        message: String,
    },
    /// I/O error
    Io(std::io::Error),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http {
                status: Some(s),
                message,
            } => write!(f, "HTTP {s}: {message}"),
            Self::Http {
                status: None,
                message,
            } => write!(f, "HTTP error: {message}"),
            Self::Io(e) => write!(f, "IO error: {e}"),
        }
    }
}

impl std::error::Error for StreamError {}

impl StreamError {
    /// Create HTTP error from reqwest error
    pub fn from_reqwest(e: &reqwest::Error) -> Self {
        Self::Http {
            status: e.status().map(|s| s.as_u16()),
            message: e.to_string(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http { status, .. } => {
                // 400 = AWS STS token expired, not retryable
                // 403 = URL signature expired, not retryable
                // 410 = URL expired, not retryable
                !matches!(status, Some(400) | Some(403) | Some(410))
            }
            Self::Io(e) => {
                // Disk full is not retryable, timeout IS retryable
                e.kind() != std::io::ErrorKind::StorageFull
            }
        }
    }
}

impl From<std::io::Error> for StreamError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Shared async HTTP client with connection pooling.
static SHARED_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_max_idle_per_host(8)
        .build()
        .expect("failed to build HTTP client")
});

/// Get shared HTTP client.
pub fn http_client() -> &'static reqwest::Client {
    &SHARED_CLIENT
}

/// Shared tokio runtime for HTTP operations.
pub static SHARED_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
});

/// Buffer size for gzip stream reader (256KB)
const GZIP_BUF_SIZE: usize = 256 * 1024;

/// Buffered reader over a gzipped HTTP response body with byte counting
pub type GzipReader = BufReader<GzDecoder<CountingReader<TimeoutReader>>>;

/// Shared byte counter for progress tracking
pub type ByteCounter = Arc<AtomicU64>;

/// HTTP GET → gunzip → buffered reader with byte counter
///
/// Returns (reader, byte_counter, total_bytes)
pub fn open_gzip_reader(url: &str) -> Result<(GzipReader, ByteCounter, Option<u64>), StreamError> {
    let url = url.to_string();

    // Make async request, get response
    let (reader, total_bytes) = SHARED_RUNTIME.handle().block_on(async {
        let response = SHARED_CLIENT
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| StreamError::from_reqwest(&e))?;

        let total_bytes = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());

        // Convert response body stream to AsyncRead
        let stream = response.bytes_stream();
        let async_reader = tokio_util::io::StreamReader::new(
            stream.map(|result| result.map_err(io::Error::other)),
        );

        Ok::<_, StreamError>((TimeoutReader::new(Box::pin(async_reader)), total_bytes))
    })?;

    let counter = Arc::new(AtomicU64::new(0));
    let counting_reader = CountingReader {
        inner: reader,
        count: counter.clone(),
    };
    let gz = GzDecoder::new(counting_reader);
    let buf = BufReader::with_capacity(GZIP_BUF_SIZE, gz);

    Ok((buf, counter, total_bytes))
}

/// Reader wrapper that tracks bytes read
pub struct CountingReader<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// Async-to-sync bridge with read timeout.
///
/// Wraps an async reader and provides sync Read interface.
/// Each read operation has a timeout - if no data arrives within
/// READ_TIMEOUT, returns TimedOut error (which triggers retry).
pub struct TimeoutReader {
    inner: Pin<Box<dyn AsyncRead + Send + Sync>>,
}

impl TimeoutReader {
    fn new(inner: Pin<Box<dyn AsyncRead + Send + Sync>>) -> Self {
        Self { inner }
    }
}

impl Read for TimeoutReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        SHARED_RUNTIME.handle().block_on(async {
            let read_future = async {
                let mut read_buf = ReadBuf::new(buf);
                std::future::poll_fn(|cx: &mut Context<'_>| {
                    Pin::as_mut(&mut self.inner).poll_read(cx, &mut read_buf)
                })
                .await?;
                Ok::<_, io::Error>(read_buf.filled().len())
            };

            match tokio::time::timeout(READ_TIMEOUT, read_future).await {
                Ok(result) => result,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "read timeout (10s with no data)",
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_err(status: u16) -> StreamError {
        StreamError::Http {
            status: Some(status),
            message: "test".to_string(),
        }
    }

    #[test]
    fn http_403_not_retryable() {
        assert!(!http_err(403).is_retryable());
    }

    #[test]
    fn http_410_not_retryable() {
        assert!(!http_err(410).is_retryable());
    }

    #[test]
    fn http_400_not_retryable() {
        assert!(!http_err(400).is_retryable());
    }

    #[test]
    fn http_500_retryable() {
        assert!(http_err(500).is_retryable());
    }

    #[test]
    fn http_429_retryable() {
        assert!(http_err(429).is_retryable());
    }

    #[test]
    fn io_timeout_retryable() {
        let err = StreamError::Io(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
        assert!(err.is_retryable());
    }

    #[test]
    fn io_storage_full_not_retryable() {
        let err = StreamError::Io(io::Error::new(io::ErrorKind::StorageFull, "disk full"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn io_connection_reset_retryable() {
        let err = StreamError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
        assert!(err.is_retryable());
    }

    #[test]
    fn http_none_status_retryable() {
        // Network error without status code should be retryable
        let err = StreamError::Http {
            status: None,
            message: "connection refused".to_string(),
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn display_http_with_status() {
        let err = http_err(404);
        assert_eq!(format!("{err}"), "HTTP 404: test");
    }

    #[test]
    fn display_http_without_status() {
        let err = StreamError::Http {
            status: None,
            message: "timeout".to_string(),
        };
        assert_eq!(format!("{err}"), "HTTP error: timeout");
    }

    #[test]
    fn display_io_error() {
        let err = StreamError::Io(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        assert!(format!("{err}").contains("IO error"));
    }
}
