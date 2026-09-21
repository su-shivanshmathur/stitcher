//! Config-driven output transformer: `transformer.toml` → N Kafka topics as a
//! [`stitcher::Projection`], validated against the state schema at load.

mod config;
mod engine;
mod expr;

pub use config::load;
pub use engine::Transform;
