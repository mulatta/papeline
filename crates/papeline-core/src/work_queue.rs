//! Lock-free work queue for distributing shards across parallel workers

use std::sync::atomic::{AtomicUsize, Ordering};

/// Lock-free work queue distributing items to workers.
///
/// Workers call [`next()`](WorkQueue::next) to atomically claim the next item.
/// Supports optional filtering at construction time for resume/skip logic.
pub struct WorkQueue<S> {
    items: Vec<S>,
    cursor: AtomicUsize,
}

impl<S> WorkQueue<S> {
    /// Create queue from all items (no filtering)
    pub fn new(items: Vec<S>) -> Self {
        Self {
            items,
            cursor: AtomicUsize::new(0),
        }
    }

    /// Create queue, keeping only items that pass the filter (resume support)
    pub fn filtered(items: Vec<S>, keep: impl Fn(&S) -> bool) -> Self {
        let filtered: Vec<S> = items.into_iter().filter(|s| keep(s)).collect();
        log::debug!("{} items in work queue", filtered.len());
        Self {
            items: filtered,
            cursor: AtomicUsize::new(0),
        }
    }

    /// Get next item to process (lock-free)
    pub fn next(&self) -> Option<&S> {
        let i = self.cursor.fetch_add(1, Ordering::Relaxed);
        self.items.get(i)
    }

    /// Total items in queue
    pub fn total(&self) -> usize {
        self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_returns_all() {
        let q = WorkQueue::new(vec![1, 2, 3]);
        assert_eq!(q.total(), 3);
        assert_eq!(q.next(), Some(&1));
        assert_eq!(q.next(), Some(&2));
        assert_eq!(q.next(), Some(&3));
        assert_eq!(q.next(), None);
    }

    #[test]
    fn filtered_skips() {
        let q = WorkQueue::filtered(vec![1, 2, 3, 4], |x| *x % 2 == 0);
        assert_eq!(q.total(), 2);
        assert_eq!(q.next(), Some(&2));
        assert_eq!(q.next(), Some(&4));
        assert_eq!(q.next(), None);
    }

    #[test]
    fn empty_queue() {
        let q: WorkQueue<i32> = WorkQueue::new(vec![]);
        assert_eq!(q.total(), 0);
        assert_eq!(q.next(), None);
    }
}
