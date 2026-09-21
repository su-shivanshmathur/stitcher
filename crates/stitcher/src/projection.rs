//! The output seam: stitcher hands each state change to a [`Projection`] and depends on
//! nothing to do so; a consumer (the `transformer` crate) implements it, the binary wires it.

use serde_json::Value;

use crate::{
    enrichment::Enrichment,
    processor::{OutMsg, Sign},
};

/// Wall clock + enrichment handed to a [`Projection`] per call.
pub struct ProjectionContext<'a> {
    pub now_secs: i64,
    pub enrichment: &'a Enrichment,
}

/// Consumes one signed state change → sink records. `Send + Sync`: held across `.await`.
pub trait Projection: Send + Sync {
    fn project(&self, state: &Value, sign: Sign, ctx: &ProjectionContext<'_>) -> Vec<OutMsg>;
}

/// Default standalone projection: logs the transition (target `stitcher::state`), emits nothing.
pub struct StateLogger;

impl Projection for StateLogger {
    fn project(&self, state: &Value, sign: Sign, _ctx: &ProjectionContext<'_>) -> Vec<OutMsg> {
        tracing::info!(target: "stitcher::state", ?sign, %state, "state");
        Vec::new()
    }
}
