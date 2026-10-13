//! The transform engine: one merged state `Value` (+ its `sign`) → `Vec<OutMsg>` across the
//! configured streams. Each stream: gate → project fields (optionally `explode`ing a map
//! into per-entry rows merged with `common` fields) → stamp `sign_flag` → retention-filter.

use serde_json::Value;
use stitcher_dsl::expr::Expr;

use super::expr::StateScope;
use stitcher::{
    builtins,
    enrichment::Enrichment,
    eval, filters, json_util,
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
    /// Project a state transition into sink records across every stream (retract `old`, emit `new`).
    fn project_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg> {
        self.streams
            .iter()
            .flat_map(|stream| stream.render_transition(old, new, ctx))
            .collect()
    }
}

impl CompiledStream {
    /// Gate on the NEW state, then per new row make ONE retention decision and ship the pair (`-1`
    /// from the matching old row, if any, + `+1` from the new row) or nothing — never a lone `-1`.
    /// (A gate-open→gate-closed transition strands the prior `+1`, like retention; downstream must
    /// be sign-aware / treat gate changes as grow-only.)
    fn render_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg> {
        let gate_scope = StateScope {
            state: new,
            element: None,
            enrichment: ctx.enrichment,
        };
        let open = self
            .gate
            .as_ref()
            .is_none_or(|gate| builtins::truthy(&eval::eval(gate, &gate_scope)));
        if open {
            let old_rows = old.map(|state| self.rows(state, ctx)).unwrap_or_default();
            self.rows(new, ctx)
                .iter()
                .flat_map(|(row_id, new_element)| {
                    self.render_pair(old, new, ctx, row_id, new_element, &old_rows)
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// The `(row_id, element)` rows a state explodes into (one empty-id row when not exploding). The
    /// row id (keyed_map entry key / array index) is a stable per-row identity.
    fn rows(&self, state: &Value, ctx: &ProjectionContext<'_>) -> Vec<(String, Value)> {
        match &self.explode {
            Some(explode) => {
                let base = StateScope {
                    state,
                    element: None,
                    enrichment: ctx.enrichment,
                };
                explode_entries(&eval::eval(explode, &base))
            }
            None => vec![(String::new(), Value::Null)],
        }
    }

    /// One row's ±1 pair, gated by the NEW row's retention so both legs ship together or neither.
    fn render_pair(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
        row_id: &str,
        new_element: &Value,
        old_rows: &[(String, Value)],
    ) -> Vec<OutMsg> {
        let new_scope = self.scope(new, new_element, ctx.enrichment);
        let new_payload = self.payload(&new_scope, Sign::Plus);
        if self.passes_retention(&new_payload, ctx.now_secs) {
            let emit = self.out_msg(
                ctx,
                Sign::Plus,
                row_id,
                self.key_of(&new_scope),
                &new_payload,
            );
            let retract = old.zip(old_rows.iter().find(|(id, _)| id == row_id)).map(
                |(old_state, (_, old_element))| {
                    let old_scope = self.scope(old_state, old_element, ctx.enrichment);
                    let old_payload = self.payload(&old_scope, Sign::Minus);
                    self.out_msg(
                        ctx,
                        Sign::Minus,
                        row_id,
                        self.key_of(&old_scope),
                        &old_payload,
                    )
                },
            );
            retract.into_iter().chain(std::iter::once(emit)).collect()
        } else {
            Vec::new()
        }
    }

    /// Evaluation scope for a row: the state, the explode element (only when exploding), enrichment.
    fn scope<'s>(
        &self,
        state: &'s Value,
        element: &'s Value,
        enrichment: &'s Enrichment,
    ) -> StateScope<'s> {
        StateScope {
            state,
            element: self.explode.as_ref().map(|_| element),
            enrichment,
        }
    }

    /// A row's projected payload: `common` + `row` fields (nulls omitted) + the `sign_flag`.
    fn payload(&self, cx: &StateScope<'_>, sign: Sign) -> Value {
        let mut obj = serde_json::Map::new();
        for (name, source) in self.common.iter().chain(self.row.iter()) {
            let value = eval::eval(source, cx);
            if !value.is_null() {
                obj.insert(name.clone(), value); // omit nulls (== omitNothingFields)
            }
        }
        if self.sign_flag {
            obj.insert("sign_flag".to_string(), Value::from(sign.value()));
        }
        Value::Object(obj)
    }

    /// The projected Kafka message key for a row (empty when the stream has no `key`).
    fn key_of(&self, cx: &StateScope<'_>) -> String {
        self.key
            .as_ref()
            .and_then(|key| builtins::key_string(&eval::eval(key, cx)))
            .unwrap_or_default()
    }

    /// Build one output record, stamping the delivery envelope. The ids fold in the stream, the
    /// stable aggregate key and the row identity (NOT the mutable projected key, so a row's ±1 pair
    /// correlates even if its projected key changes).
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
            ctx.aggregate_key,
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

/// Stable FNV-1a-128 over length-prefixed parts, hex-encoded. Deterministic across runs and
/// platforms; 128 bits keeps dedup-id birthday collisions negligible at scale. Length-prefixing
/// (fixed 8-byte LE) is injective, so no part boundary can be forged by content.
fn envelope_id<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut hash = OFFSET;
    for part in parts {
        let bytes = part.as_bytes();
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        for byte in len.to_le_bytes().iter().chain(bytes) {
            hash ^= u128::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    format!("{hash:032x}")
}
