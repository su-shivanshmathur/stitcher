//! Transform evaluation scope: paths resolve against the merged `state` or the current
//! `explode`d `each` element; `lookup` joins the enrichment map.

use std::borrow::Cow;

use serde_json::Value;

use stitcher::{enrichment::Enrichment, eval::EvalContext, json_util};

/// Evaluation scope for one projected row.
pub struct StateScope<'a> {
    /// The merged state (`state.` path root).
    pub state: &'a Value,
    /// The current `explode`d element (`each.` path root), if any.
    pub element: Option<&'a Value>,
    /// Enrichment join map (backs `lookup(table, key)`).
    pub enrichment: &'a Enrichment,
}

impl<'a> EvalContext<'a> for StateScope<'a> {
    /// `state.` → the merged state, `each.` → the current element, a bare `state`/`each`
    /// → that whole value, an unprefixed path → the state (convenience).
    fn resolve_path(&self, path: &str) -> Cow<'a, Value> {
        if let Some(rest) = path.strip_prefix("state.") {
            lookup_into(self.state, rest)
        } else if path == "state" {
            Cow::Borrowed(self.state)
        } else if let Some(rest) = path.strip_prefix("each.") {
            self.element.map_or(Cow::Owned(Value::Null), |element| {
                lookup_into(element, rest)
            })
        } else if path == "each" {
            self.element.map_or(Cow::Owned(Value::Null), Cow::Borrowed)
        } else {
            lookup_into(self.state, path)
        }
    }

    /// `lookup(table, key)`: a two-level enrichment join — `enrichment[table][key]`.
    fn lookup(&self, table: &str, key: &str) -> Value {
        self.enrichment
            .snapshot()
            .get(table)
            .and_then(|rows| rows.get(key))
            .cloned()
            .unwrap_or(Value::Null)
    }
}

fn lookup_into<'a>(root: &'a Value, path: &str) -> Cow<'a, Value> {
    json_util::get_path(root, path)
        .map(Cow::Borrowed)
        .unwrap_or(Cow::Owned(Value::Null))
}
