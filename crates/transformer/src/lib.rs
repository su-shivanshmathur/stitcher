//! Config-driven output transformer: `transformer.yaml` → N Kafka topics as a
//! [`stitcher::Projection`], validated against the state schema at load.

mod config;
mod engine;
mod expr;
mod validate;

pub use config::load;
pub use engine::Transform;
