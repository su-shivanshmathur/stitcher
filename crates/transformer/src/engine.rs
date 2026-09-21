//! The transform engine: one merged state `Value` (+ its `sign`) → `Vec<OutMsg>` across the
//! configured streams. Each stream: gate → project fields (optionally `explode`ing a map
//! into per-entry rows merged with `common` fields) → stamp `sign_flag` → retention-filter.

use serde_json::Value;
use stitcher_dsl::expr::Expr;

use super::expr::StateScope;
use stitcher::{
    builtins, eval, filters, json_util,
    processor::{OutMsg, Sign},
    projection::{Projection, ProjectionContext},
};

/// A compiled `transformer.toml`: the ordered set of output streams.
pub struct Transform {
    streams: Vec<CompiledStream>,
}

/// Per-stream retention window (epoch-seconds timestamp field + max age in days).
pub(super) struct Retention {
    pub days: i64,
    pub key: String,
}

/// One compiled output stream.
pub(super) struct CompiledStream {
    pub topic: String,
    pub sign_flag: bool,
    pub gate: Option<Expr>,
    pub retention: Option<Retention>,
    pub explode: Option<Expr>,
    pub common: Vec<(String, Expr)>,
    pub row: Vec<(String, Expr)>,
    pub key: Option<Expr>,
}

impl Transform {
    pub(super) fn new(streams: Vec<CompiledStream>) -> Self {
        Self { streams }
    }
}

impl Projection for Transform {
    /// Project a merged state into sink records across every stream, stamping `sign`.
    fn project(&self, state: &Value, sign: Sign, ctx: &ProjectionContext<'_>) -> Vec<OutMsg> {
        self.streams
            .iter()
            .flat_map(|stream| stream.render(state, sign, ctx))
            .collect()
    }
}

impl CompiledStream {
    fn render(&self, state: &Value, sign: Sign, ctx: &ProjectionContext<'_>) -> Vec<OutMsg> {
        let base = StateScope {
            state,
            element: None,
            enrichment: ctx.enrichment,
        };
        // top-level gate (== isStateProper): a closed gate emits nothing
        let open = self
            .gate
            .as_ref()
            .is_none_or(|gate| builtins::truthy(&eval::eval(gate, &base)));
        if !open {
            return Vec::new();
        }
        let elements: Vec<Value> = match &self.explode {
            Some(explode_expr) => explode_entries(&eval::eval(explode_expr, &base)),
            None => vec![Value::Null], // a single row, no `each` scope
        };
        elements
            .iter()
            .filter_map(|element| self.render_row(state, sign, ctx, element))
            .collect()
    }

    fn render_row(
        &self,
        state: &Value,
        sign: Sign,
        ctx: &ProjectionContext<'_>,
        element: &Value,
    ) -> Option<OutMsg> {
        let cx = StateScope {
            state,
            element: self.explode.as_ref().map(|_| element),
            enrichment: ctx.enrichment,
        };
        let mut obj = serde_json::Map::new();
        for (name, source) in self.common.iter().chain(self.row.iter()) {
            let value = eval::eval(source, &cx);
            if !value.is_null() {
                obj.insert(name.clone(), value); // omit nulls (== omitNothingFields)
            }
        }
        if self.sign_flag {
            obj.insert("sign_flag".to_string(), Value::from(sign.value()));
        }
        let payload = Value::Object(obj);
        self.passes_retention(&payload, ctx.now_secs).then(|| {
            let key = self
                .key
                .as_ref()
                .and_then(|key| builtins::key_string(&eval::eval(key, &cx)))
                .unwrap_or_default();
            OutMsg {
                topic: self.topic.clone(),
                key,
                // serialize failures are dropped (unreachable for a serde_json::Map)
                payload: serde_json::to_vec(&payload).unwrap_or_default(),
            }
        })
    }

    /// Keep a row unless its retention timestamp is older than the window. A missing
    /// or unparseable timestamp is RETAINED (no silent loss), matching the pipeline's
    /// [`filters::RetentionTable`].
    fn passes_retention(&self, payload: &Value, now_secs: i64) -> bool {
        self.retention.as_ref().is_none_or(|retention| {
            json_util::get_path(payload, &retention.key)
                .and_then(json_util::as_i64)
                .is_none_or(|secs| filters::within_retention(secs, retention.days, now_secs))
        })
    }
}

/// `explode` a keyed map into its entries (`{comparator, payload}` each, so `each.payload.*`
/// resolves) or an array into its items. Anything else ⇒ no rows.
fn explode_entries(collection: &Value) -> Vec<Value> {
    match collection {
        Value::Object(entries) => entries.values().cloned().collect(),
        Value::Array(items) => items.clone(),
        _ => Vec::new(),
    }
}

