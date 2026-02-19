//! Graceful shutdown support via atomic flag

use std::sync::atomic::{AtomicBool, Ordering};

/// Global shutdown flag — set by SIGTERM/SIGINT handler
pub fn shutdown_flag() -> &'static AtomicBool {
    static FLAG: AtomicBool = AtomicBool::new(false);
    &FLAG
}

/// Check if shutdown was requested
pub fn is_shutdown_requested() -> bool {
    shutdown_flag().load(Ordering::Relaxed)
}

/// Request shutdown (for signal handlers)
pub fn request_shutdown() {
    shutdown_flag().store(true, Ordering::Relaxed);
}
