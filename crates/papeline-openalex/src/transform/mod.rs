//! JSON-to-Arrow record batch accumulators for OpenAlex entities

pub mod author;
pub mod funder;
pub mod institution;
pub mod publisher;
pub mod source;
pub mod topic;
pub mod work;

pub use papeline_core::{Accumulator, DEFAULT_BATCH_SIZE as RECORD_BATCH_SIZE};

// Re-exports
pub use author::{AuthorAccumulator, AuthorRow};
pub use funder::{FunderAccumulator, FunderRow};
pub use institution::{InstitutionAccumulator, InstitutionRow};
pub use publisher::{PublisherAccumulator, PublisherRow};
pub use source::{SourceAccumulator, SourceRow};
pub use topic::{TopicAccumulator, TopicRow};
pub use work::{WorkAccumulator, WorkRow};
