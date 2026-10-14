//! The output seam: stitcher hands each state change to a [`Projection`] and depends on
//! nothing to do so; a consumer (the `transformer` crate) implements it, the binary wires it.

use serde_json::Value;

use crate::{
    enrichment::Enrichment,
    processor::{Causation, OutMsg},
};

/// Wall clock, enrichment, and envelope inputs handed to a [`Projection`] per call.
pub struct ProjectionContext<'a> {
    pub now_secs: i64,
    pub enrichment: &'a Enrichment,
    /// Schema version stamped into each delta's envelope.
    pub state_version: i64,
    /// The source coordinate that caused this key's state change (see [`Causation`]).
    pub causation: &'a Causation,
    /// The aggregate's stable grouping key — the envelope identity anchor.
    pub aggregate_key: &'a str,
}

/// Consumes one state transition → sink records. `old` is the previously stored state
/// (absent on first sight), `new` the merged state; the impl decides the ±1 pair per row
/// so a retract and its emit ship together.
pub trait Projection: Send + Sync {
    fn project_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg>;
}

/// Default standalone projection: logs the transition (target `stitcher::state`), emits nothing.
pub struct StateLogger;

impl Projection for StateLogger {
    fn project_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        _ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg> {
        tracing::info!(target: "stitcher::state", ?old, %new, "state transition");
        Vec::new()
    }
}

/// Fans one state change to several projections, concatenating their deltas.
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
    fn project_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg> {
        match self.members.as_slice() {
            [only] => only.project_transition(old, new, ctx),
            members => members
                .iter()
                .flat_map(|member| member.project_transition(old, new, ctx))
                .collect(),
        }
    }
}
