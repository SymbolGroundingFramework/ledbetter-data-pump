//! `ldp-core` — the runtime contract: chunks, partitions, the plan, the
//! traits every connector implements, checkpointing, the expression engine,
//! transforms, and the partitioned worker pool.
//!
//! Dependency direction (contract §2): `ldp-core` → `ldp-arrow` only.
//! `ldp-config` depends on this crate to *build* an [`ExecutionPlan`]; nothing
//! here knows about YAML.
//!
//! Deliberate deviations from the contract sketch, each with a reason:
//!
//! * `ExecutionPlan`/`Stage` live here, not in `ldp-config` — `ldp-core` needs
//!   them at run time and must not depend on config (that would invert the
//!   dependency arrow).
//! * `Source::read_partition` returns a **stream** of batches instead of one
//!   `DataChunk`. A single partition can exceed memory, and DoD #5 requires
//!   `max_batch_rows`/`max_batch_bytes` to be enforced — impossible if the
//!   connector must materialise a whole partition before returning.
//! * `Transform::apply` returns the batch plus a dropped-row count so row
//!   isolation (addendum §1, Phase 1 "basic") is observable in the summary.

pub mod checkpoint;
pub mod chunk;
pub mod error;
pub mod expr;
pub mod log;
pub mod partition;
pub mod plan;
pub mod runner;
pub mod traits;
pub mod transform;

pub use checkpoint::PipelineState;
pub use chunk::DataChunk;
pub use error::{LdpError, ValidationErrors};
pub use expr::Expr;
pub use log::{LogLevel, Logger};
pub use partition::{plan_range, Partition};
pub use plan::{
    Compression, ConnectionSpec, ExecutionPlan, PartitionKind, PartitionSpec, Settings, SinkKind,
    SinkSpec, SourceSpec, TransformSpec,
};
pub use runner::{run, RunReport, SinkBinding};
pub use traits::{BatchStream, Sink, Source, Transform, Transformed};
