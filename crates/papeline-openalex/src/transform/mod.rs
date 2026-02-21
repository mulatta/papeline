//! JSON-to-Arrow record batch accumulators for OpenAlex entities

pub mod author;
pub mod funder;
pub mod institution;
pub mod publisher;
pub mod source;
pub mod topic;
pub mod work;

use arrow::array::RecordBatch;

/// Batch size for creating `RecordBatch` from accumulated rows
pub const RECORD_BATCH_SIZE: usize = 8192;

/// Accumulator trait for batch processing
pub trait Accumulator {
    type Row;

    /// Push a row into the accumulator
    fn push(&mut self, row: Self::Row);

    /// Number of rows currently buffered
    fn len(&self) -> usize;

    /// Check if buffer is full and should be flushed
    fn is_full(&self) -> bool {
        self.len() >= RECORD_BATCH_SIZE
    }

    /// Check if buffer is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Take buffered rows as a RecordBatch, resetting internal state
    fn take_batch(&mut self) -> RecordBatch;
}

// Re-exports
pub use author::{AuthorAccumulator, AuthorRow};
pub use funder::{FunderAccumulator, FunderRow};
pub use institution::{InstitutionAccumulator, InstitutionRow};
pub use publisher::{PublisherAccumulator, PublisherRow};
pub use source::{SourceAccumulator, SourceRow};
pub use topic::{TopicAccumulator, TopicRow};
pub use work::{WorkAccumulator, WorkRow};
