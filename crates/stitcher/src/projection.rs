//! The output seam: stitcher hands each state change to a [`Projection`] and depends on
//! nothing else to do so.

use serde_json::Value;

use crate::{
    enrichment::Enrichment,
    processor::{Causation, OutMsg},
};

pub struct ProjectionContext<'a> {
    pub now_secs: i64,
    pub enrichment: &'a Enrichment,
    pub state_version: i64,
    pub causation: &'a Causation,
    /// The stable grouping key — the envelope identity anchor.
    pub aggregate_key: &'a str,
}

/// One transition → sink records; the impl pairs each retract with its emit.
pub trait Projection: Send + Sync {
    fn project_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg>;
}

/// Logs the transition (target `stitcher::state`), emits nothing.
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
