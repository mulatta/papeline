//! Counting semaphore for limiting concurrent downloads.
//!
//! Uses `Mutex + Condvar` from std — no external dependencies.

use std::sync::{Condvar, Mutex};

/// A counting semaphore that limits concurrent access to a shared resource.
pub struct Semaphore {
    state: Mutex<usize>,
    cond: Condvar,
}

/// RAII guard that releases one permit on drop.
pub struct SemaphoreGuard<'a>(&'a Semaphore);

impl Semaphore {
    /// Create a semaphore with `permits` initial permits.
    pub fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(permits),
            cond: Condvar::new(),
        }
    }

    /// Block until a permit is available, then acquire it.
    pub fn acquire(&self) -> SemaphoreGuard<'_> {
        let mut count = self.state.lock().unwrap();
        while *count == 0 {
            count = self.cond.wait(count).unwrap();
        }
        *count -= 1;
        SemaphoreGuard(self)
    }
}

impl Drop for SemaphoreGuard<'_> {
    fn drop(&mut self) {
        let mut count = self.0.state.lock().unwrap();
        *count += 1;
        self.0.cond.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn acquire_and_release() {
        let sem = Semaphore::new(2);
        let _g1 = sem.acquire();
        let _g2 = sem.acquire();
        // Both acquired — count should be 0 internally
        assert_eq!(*sem.state.lock().unwrap(), 0);
        drop(_g1);
        assert_eq!(*sem.state.lock().unwrap(), 1);
    }

    #[test]
    fn blocking_acquire() {
        let sem = Arc::new(Semaphore::new(1));
        let guard = sem.acquire();

        let sem2 = sem.clone();
        let handle = std::thread::spawn(move || {
            let _g = sem2.acquire();
            42
        });

        // Give thread time to block
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(guard); // release → unblock the other thread

        assert_eq!(handle.join().unwrap(), 42);
    }
}
