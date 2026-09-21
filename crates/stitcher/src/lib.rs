//! `stitcher` — domain-agnostic Kafka → store → Kafka stream-aggregation framework.

pub mod builtins;
pub mod codec;
pub mod config;
pub mod enrichment;
pub mod eval;
pub mod errors;
pub mod filters;
pub mod inspect;
pub mod json_util;
pub mod kafka;
pub mod merge;
pub mod metrics;
pub mod pipeline;
pub mod processor;
pub mod projection;
pub mod secret;
pub mod state;
pub mod store;
pub mod telemetry;
pub mod util;

pub use errors::{StitcherError, StitcherResult};
pub use pipeline::run;
pub use processor::{Key, OutMsg, Processor, Sign};
pub use projection::{Projection, ProjectionContext, StateLogger};
