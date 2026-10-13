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
    /// Stream name (globally unique; folded into envelope ids so same-topic streams don't collide).
    pub name: String,
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
        // each row carries a stable identity (keyed_map entry key / array index) so its envelope
        // ids are unique per row; a non-explode stream is one row with no identity.
        if open {
            let rows: Vec<(String, Value)> = match &self.explode {
                Some(explode_expr) => explode_entries(&eval::eval(explode_expr, &base)),
                None => vec![(String::new(), Value::Null)],
            };
            rows.iter()
                .filter_map(|(row_id, element)| self.render_row(state, sign, ctx, row_id, element))
                .collect()
        } else {
            Vec::new()
        }
    }

    fn render_row(
        &self,
        state: &Value,
        sign: Sign,
        ctx: &ProjectionContext<'_>,
        row_id: &str,
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
        // a -1 ships only with its +1: both legs are gated by retention. A row's retract (old) and
        // emit (new) share the same retention-key value when `retention.key` is a creation-time
        // field (the documented contract), so the pair is shipped or suppressed together — never a
        // lone -1.
        let should_emit = self.passes_retention(&payload, ctx.now_secs);
        should_emit.then(|| {
            let key = self
                .key
                .as_ref()
                .and_then(|key| builtins::key_string(&eval::eval(key, &cx)))
                .unwrap_or_default();
            self.out_msg(ctx, sign, row_id, key, &payload)
        })
    }

    /// Build one output record, stamping the delivery envelope. The dedup/correlation ids fold in
    /// the stream + row identity so same-topic streams and explode rows stay distinct.
    fn out_msg(
        &self,
        ctx: &ProjectionContext<'_>,
        sign: Sign,
        row_id: &str,
        key: String,
        payload: &Value,
    ) -> OutMsg {
        let partition_text = ctx.causation.partition.to_string();
        let offset_text = ctx.causation.offset.to_string();
        // correlation_id identifies the row's ±1 pair; event_id is the same parts plus the sign
        let base: [&str; 7] = [
            &self.name,
            &self.topic,
            &key,
            &ctx.causation.topic,
            &partition_text,
            &offset_text,
            row_id,
        ];
        OutMsg {
            event_id: envelope_id(base.iter().copied().chain(std::iter::once(sign.label()))),
            correlation_id: envelope_id(base.iter().copied()),
            state_version: ctx.state_version,
            produced_at: ctx.now_secs,
            causation: ctx.causation.clone(),
            topic: self.topic.clone(),
            key,
            payload: serde_json::to_vec(payload).unwrap_or_default(),
        }
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

/// `explode` a keyed map into `(entry key, {comparator, payload})` pairs (so `each.payload.*`
/// resolves and the entry key is a stable row identity) or an array into `(index, item)` pairs.
/// Anything else ⇒ no rows.
fn explode_entries(collection: &Value) -> Vec<(String, Value)> {
    match collection {
        Value::Object(entries) => entries
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

/// Stable FNV-1a-64 over NUL-separated parts, hex-encoded (deterministic across runs).
fn envelope_id<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for byte in part.as_bytes().iter().chain(std::iter::once(&0u8)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("{hash:016x}")
}
