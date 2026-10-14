//! One merged state `Value` → `Vec<OutMsg>` across streams: gate-diff old/new, project,
//! stamp `sign_flag`, retention-filter.

use std::{borrow::Cow, collections::HashMap};

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

pub struct Transform {
    streams: Vec<CompiledStream>,
}

/// Per-stream retention window (epoch-seconds timestamp field + max age in days).
pub(super) struct Retention {
    pub days: i64,
    pub key: String,
}

pub(super) struct CompiledStream {
    /// Globally unique; folded into envelope ids so same-topic streams don't collide.
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
    /// Old/new gate row-set diff — orphan-free: open→open pairs per row, closed→open only
    /// `+1`s, open→closed only `-1`s. Retention gates a pair on the NEW payload (retention
    /// keys are creation-time immutable, so a pair can't be split).
    fn render_transition(
        &self,
        old: Option<&Value>,
        new: &Value,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg> {
        let gate_open = |state: &Value| {
            self.gate.as_ref().is_none_or(|gate| {
                let scope = StateScope {
                    state,
                    element: None,
                    enrichment: ctx.enrichment,
                };
                builtins::truthy(&eval::eval(gate, &scope))
            })
        };
        let new_open = gate_open(new);
        let old_open = old.is_some_and(gate_open); // no old state ⇒ nothing was ever emitted
        match (old_open, new_open) {
            (false, false) => Vec::new(),
            (_, true) => {
                let old_exploded = match (old_open, old) {
                    (true, Some(state)) => Some(self.eval_explode(state, ctx)),
                    _ => None, // old gate closed (or absent): nothing was emitted
                };
                // Some(None) = non-explode (sentinel row); Some(Some(c)) = per-entry rows
                let old_rows: Vec<(String, &Value)> = match &old_exploded {
                    Some(exploded) => Self::rows_of(exploded.as_deref()),
                    None => Vec::new(),
                };
                let old_row_index: HashMap<&str, &Value> = old_rows
                    .iter()
                    .map(|(id, element)| (id.as_str(), *element))
                    .collect();
                let new_exploded = self.eval_explode(new, ctx);
                Self::rows_of(new_exploded.as_deref())
                    .into_iter()
                    .flat_map(|(row_id, new_element)| {
                        let old_row = old.zip(old_row_index.get(row_id.as_str()).copied());
                        self.render_pair(new, new_element, old_row, &row_id, ctx)
                    })
                    .collect()
            }
            (true, false) => match old {
                Some(old_state) => {
                    let exploded = self.eval_explode(old_state, ctx);
                    Self::rows_of(exploded.as_deref())
                        .into_iter()
                        .filter_map(|(row_id, old_element)| {
                            self.render_close(ctx, old_state, old_element, &row_id)
                        })
                        .collect()
                }
                None => Vec::new(), // unreachable: `old_open` implies `old` is Some
            },
        }
    }

    /// `None` when not exploding; borrows from `state` — `ctx` must outlive it.
    fn eval_explode<'s>(
        &self,
        state: &'s Value,
        ctx: &ProjectionContext<'s>,
    ) -> Option<Cow<'s, Value>> {
        self.explode.as_ref().map(|explode| {
            let base = StateScope {
                state,
                element: None,
                enrichment: ctx.enrichment,
            };
            eval::eval(explode, &base)
        })
    }

    /// Explode-collection rows (borrowed) or the sentinel row; row id = keyed_map key / array index.
    fn rows_of(exploded: Option<&Value>) -> Vec<(String, &Value)> {
        match exploded {
            Some(collection) => explode_entries(collection),
            None => vec![(String::new(), &NULL_ROW)],
        }
    }

    /// One row's ±1 pair, retention-gated on the NEW payload. An unchanged row (projected
    /// fields AND routing key) is silence — keeps a replayed first batch from pairing an
    /// unmatched `-1` with an already-deduped `+1`.
    fn render_pair(
        &self,
        new: &Value,
        new_element: &Value,
        old_row: Option<(&Value, &Value)>,
        row_id: &str,
        ctx: &ProjectionContext<'_>,
    ) -> Vec<OutMsg> {
        let new_scope = self.scope(new, new_element, ctx.enrichment);
        let new_fields = self.project(&new_scope);
        let Some((old_state, old_element)) = old_row else {
            // no previously-emitted old row (grow-only map row, or gate closed on old)
            let payload = self.stamp_sign(new_fields, Sign::Plus);
            return self
                .emit_if_retained(&payload, ctx, Sign::Plus, row_id, &new_scope)
                .into_iter()
                .collect();
        };
        // Retract content is recomputed with the LIVE enrichment map — a reload between
        // emit and retract can shift the −1's fields (known limit).
        let old_scope = self.scope(old_state, old_element, ctx.enrichment);
        let old_fields = self.project(&old_scope);
        // A key-only change is NOT a no-op: the old key needs its −1, the new key its +1.
        if old_fields == new_fields && self.key_of(&old_scope) == self.key_of(&new_scope) {
            return Vec::new(); // unchanged row: a cancelling pair ≡ silence
        }
        let new_payload = self.stamp_sign(new_fields, Sign::Plus);
        if !self.passes_retention(&new_payload, ctx.now_secs) {
            return Vec::new();
        }
        // serialize new_payload once; reuse bytes for both the emit id and the retract correlation
        let new_bytes = serialize(&new_payload);
        let old_payload = self.stamp_sign(old_fields, Sign::Minus);
        let old_bytes = serialize(&old_payload);
        // retract anchors correlation on the new content (pair identity = surviving content)
        let retract = self.out_msg_paired(
            ctx,
            Sign::Minus,
            row_id,
            self.key_of(&old_scope),
            old_bytes,
            &new_bytes,
        );
        // emit self-correlates on its own content
        let emit = self.out_msg_self(ctx, Sign::Plus, row_id, self.key_of(&new_scope), new_bytes);
        vec![retract, emit]
    }

    /// A gate-close transition's lone retract (retention on the OLD payload).
    fn render_close(
        &self,
        ctx: &ProjectionContext<'_>,
        old_state: &Value,
        old_element: &Value,
        row_id: &str,
    ) -> Option<OutMsg> {
        let old_scope = self.scope(old_state, old_element, ctx.enrichment);
        let old_payload = self.stamp_sign(self.project(&old_scope), Sign::Minus);
        self.emit_if_retained(&old_payload, ctx, Sign::Minus, row_id, &old_scope)
    }

    /// Retention-gated emit leg (a lone `+1`, or the shared gate for a pair).
    fn emit_if_retained(
        &self,
        payload: &Value,
        ctx: &ProjectionContext<'_>,
        sign: Sign,
        row_id: &str,
        scope: &StateScope<'_>,
    ) -> Option<OutMsg> {
        // serialize once; reuse bytes for both the id hash and the owned payload field
        self.passes_retention(payload, ctx.now_secs).then(|| {
            let bytes = serialize(payload);
            self.out_msg_self(ctx, sign, row_id, self.key_of(scope), bytes)
        })
    }

    /// Evaluation scope for a row: state, explode element, enrichment.
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

    /// `common` + `row` (nulls omitted); sign stamped separately so skip-comparisons see
    /// content, not the ±1.
    fn project(&self, cx: &StateScope<'_>) -> serde_json::Map<String, Value> {
        let mut obj = serde_json::Map::new();
        for (name, source) in self.common.iter().chain(self.row.iter()) {
            let value = eval::eval(source, cx);
            if !value.is_null() {
                obj.insert(name.clone(), value.into_owned());
            }
        }
        obj
    }

    fn stamp_sign(&self, mut fields: serde_json::Map<String, Value>, sign: Sign) -> Value {
        if self.sign_flag {
            fields.insert("sign_flag".to_string(), Value::from(sign.value()));
        }
        Value::Object(fields)
    }

    /// Projected Kafka key (empty when the stream has no `key`).
    fn key_of(&self, cx: &StateScope<'_>) -> String {
        self.key
            .as_ref()
            .and_then(|key| builtins::key_string(&eval::eval(key, cx)))
            .unwrap_or_default()
    }

    /// The 7 stable parts plus row-specific content.
    fn id_base<'x>(
        &'x self,
        ctx: &'x ProjectionContext<'x>,
        row_id: &'x str,
        partition_text: &'x str,
        offset_text: &'x str,
    ) -> [&'x str; 7] {
        [
            &self.name,
            &self.topic,
            ctx.aggregate_key,
            &ctx.causation.topic,
            partition_text,
            offset_text,
            row_id,
        ]
    }

    /// Self-correlating record (lone emits, close retracts, a pair's `+1`); payload bytes
    /// moved in — hashed and shipped from one buffer.
    fn out_msg_self(
        &self,
        ctx: &ProjectionContext<'_>,
        sign: Sign,
        row_id: &str,
        key: String,
        payload: Vec<u8>,
    ) -> OutMsg {
        let partition_text = ctx.causation.partition.to_string();
        let offset_text = ctx.causation.offset.to_string();
        let base = self.id_base(ctx, row_id, &partition_text, &offset_text);
        // correlation_id = event_id without the sign, hashing the row's own content
        let correlation_id = envelope_id(
            base.iter()
                .map(|p| p.as_bytes())
                .chain(std::iter::once(payload.as_slice())),
        );
        let event_id = envelope_id(
            base.iter()
                .map(|p| p.as_bytes())
                .chain(std::iter::once(payload.as_slice()))
                .chain(std::iter::once(sign.label().as_bytes())),
        );
        OutMsg {
            event_id,
            correlation_id,
            state_version: ctx.state_version,
            produced_at: ctx.now_secs,
            causation: ctx.causation.clone(),
            topic: self.topic.clone(),
            key,
            payload,
        }
    }

    /// The `-1` leg: event_id hashes its own payload + sign, correlation_id anchors on the
    /// surviving `+1` content, so the pair shares one correlation.
    fn out_msg_paired(
        &self,
        ctx: &ProjectionContext<'_>,
        sign: Sign,
        row_id: &str,
        key: String,
        payload: Vec<u8>,
        correlation: &[u8],
    ) -> OutMsg {
        let partition_text = ctx.causation.partition.to_string();
        let offset_text = ctx.causation.offset.to_string();
        let base = self.id_base(ctx, row_id, &partition_text, &offset_text);
        let correlation_id = envelope_id(
            base.iter()
                .map(|p| p.as_bytes())
                .chain(std::iter::once(correlation)),
        );
        let event_id = envelope_id(
            base.iter()
                .map(|p| p.as_bytes())
                .chain(std::iter::once(payload.as_slice()))
                .chain(std::iter::once(sign.label().as_bytes())),
        );
        OutMsg {
            event_id,
            correlation_id,
            state_version: ctx.state_version,
            produced_at: ctx.now_secs,
            causation: ctx.causation.clone(),
            topic: self.topic.clone(),
            key,
            payload,
        }
    }

    /// Missing or unparseable timestamp is RETAINED (no silent loss).
    fn passes_retention(&self, payload: &Value, now_secs: i64) -> bool {
        self.retention.as_ref().is_none_or(|retention| {
            json_util::get_path(payload, &retention.key)
                .and_then(json_util::as_i64)
                .is_none_or(|secs| filters::within_retention(secs, retention.days, now_secs))
        })
    }
}

/// keyed map → `(key, &entry)` rows, array → `(index, &item)`; anything else ⇒ no rows.
fn explode_entries(collection: &Value) -> Vec<(String, &Value)> {
    match collection {
        Value::Object(entries) => entries
            .iter()
            .map(|(key, value)| (key.clone(), value))
            .collect(),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value))
            .collect(),
        // Null = merge identity (expected, recurs every transition): debug, not warn.
        Value::Null => {
            tracing::debug!("explode: absent (null) collection; no rows emitted");
            Vec::new()
        }
        // Malformed stored state — the row(s) vanish with no ±1, so surface it (kind only;
        // the value may be sensitive).
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            let kind = match collection {
                Value::Bool(_) => "bool",
                Value::Number(_) => "number",
                _ => "string",
            };
            tracing::warn!(
                kind,
                "explode: non-collection (scalar) state; no rows emitted"
            );
            Vec::new()
        }
    }
}

/// Non-explode sentinel element (module-level so [`CompiledStream::rows_of`] can borrow it).
static NULL_ROW: Value = Value::Null;

/// Record bytes AND id-hash input from one buffer; BTreeMap `Map` ⇒ key-sorted, deterministic.
fn serialize(payload: &Value) -> Vec<u8> {
    serde_json::to_vec(payload).unwrap_or_default()
}

/// FNV-1a-128 over length-prefixed parts, hex — birthday-safe; length-prefixing is injective,
/// so part boundaries can't be forged.
fn envelope_id<'a>(parts: impl Iterator<Item = &'a [u8]>) -> String {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut hash = OFFSET;
    for part in parts {
        let len = u64::try_from(part.len()).unwrap_or(u64::MAX);
        for byte in len.to_le_bytes().iter().chain(part.iter()) {
            hash ^= u128::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    format!("{hash:032x}")
}
