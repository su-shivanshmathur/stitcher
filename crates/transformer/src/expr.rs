//! Paths resolve against the merged `state` or the current `explode`d `each` element;
//! `lookup` joins the enrichment map.

use std::borrow::Cow;

use serde_json::Value;

use stitcher::{enrichment::Enrichment, eval::EvalContext, json_util};

pub struct StateScope<'a> {
    pub state: &'a Value,
    pub element: Option<&'a Value>,
    pub enrichment: &'a Enrichment,
}

impl<'a> EvalContext<'a> for StateScope<'a> {
    /// `state.`/`each.` roots, bare `state`/`each` → that whole value; unprefixed → the state.
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

    /// `enrichment[table][key]`.
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
