//! `stitcher` — domain-agnostic Kafka → store → Kafka stream-aggregation framework.

pub mod builtins;
pub mod codec;
pub mod config;
pub mod enrichment;
pub mod errors;
pub mod filters;
pub mod inspect;
pub mod json_util;
pub mod kafka;
pub mod merge;
pub mod metrics;
pub mod pipeline;
pub mod processor;
pub mod secret;
pub mod state;
pub mod store;
pub mod telemetry;
pub mod util;

pub use errors::{StitcherError, StitcherResult};
pub use pipeline::{run, run_transformer};
pub use processor::{Key, OutMsg, Processor, Sign, Transformer};

// re-export the schema! proc-macro so consumers write `stitcher::schema!(...)` (PLAN §24)
pub use stitcher_macro::schema;
