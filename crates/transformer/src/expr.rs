//! Transform evaluation scope: an [`EvalContext`] whose paths resolve against the merged
//! `state` or the current `explode`d `each` element, and whose `lookup` joins the
//! enrichment map. The [`Expr`] walk itself lives in [`stitcher::eval`] — this only supplies
//! the two context-specific operations.

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

impl EvalContext for StateScope<'_> {
    /// `state.` → the merged state, `each.` → the current element, a bare `state`/`each`
    /// → that whole value, an unprefixed path → the state (convenience).
    fn resolve_path(&self, path: &str) -> Value {
        if let Some(rest) = path.strip_prefix("state.") {
            lookup_into(self.state, rest)
        } else if path == "state" {
            self.state.clone()
        } else if let Some(rest) = path.strip_prefix("each.") {
            self.element
                .map_or(Value::Null, |element| lookup_into(element, rest))
        } else if path == "each" {
            self.element.cloned().unwrap_or(Value::Null)
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

fn lookup_into(root: &Value, path: &str) -> Value {
    json_util::get_path(root, path)
        .cloned()
        .unwrap_or(Value::Null)
}
