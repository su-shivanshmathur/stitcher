//! The output seam: stitcher hands each state change to a [`Projection`] and depends on
//! nothing to do so; a consumer (the `transformer` crate) implements it, the binary wires it.

use serde_json::Value;

use crate::{
    enrichment::Enrichment,
    processor::{OutMsg, Sign},
};

/// Wall clock, enrichment, and envelope inputs handed to a [`Projection`] per call.
pub struct ProjectionContext<'a> {
    pub now_secs: i64,
    pub enrichment: &'a Enrichment,
    /// Schema version stamped into each delta's envelope.
    pub state_version: i64,
    /// Greatest source offset feeding this key's state — the delta's causation token.
    pub causation_offset: i64,
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

/// Fans one state change to several projections, concatenating their deltas — so a config
/// `Transform` and hand-written projections compose behind one `Projection`.
pub struct CompositeProjection {
    members: Vec<Box<dyn Projection>>,
}

impl CompositeProjection {
    #[must_use]
    pub fn new(members: Vec<Box<dyn Projection>>) -> Self {
        Self { members }
    }
}

impl Projection for CompositeProjection {
    fn project(&self, state: &Value, sign: Sign, ctx: &ProjectionContext<'_>) -> Vec<OutMsg> {
        match self.members.as_slice() {
            [only] => only.project(state, sign, ctx),
            members => members
                .iter()
                .flat_map(|member| member.project(state, sign, ctx))
                .collect(),
        }
    }
}
