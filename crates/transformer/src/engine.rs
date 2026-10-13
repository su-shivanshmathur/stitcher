//! The transform engine: one merged state `Value` (+ its `sign`) → `Vec<OutMsg>` across the
//! configured streams. Each stream: gate-diff the old/new states (a flip emits only the side
//! that changed) → project fields (optionally `explode`ing a map into per-entry rows merged
//! with `common` fields) → stamp `sign_flag` → retention-filter.

use std::collections::HashMap;

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
    /// Render one transition as the old/new gate row-set difference — never an orphan `-1`
    /// (a never-emitted row) nor a stranded `+1` (an emitted row gated out):
    ///
    /// - open→open: ±1 pair per matched row; closed→open: only `+1`s;
    ///   open→closed: only `-1`s; closed→closed: nothing.
    ///
    /// Retention gates a pair on the NEW payload, a close-retract on the OLD payload. That is
    /// orphan-free because retention keys are creation-time-immutable (`RetentionDoc`):
    /// fresh-now ⇒ fresh-at-emit, so a close-retract only targets rows that actually shipped.
    /// Don't gate close-retracts differently — that reintroduces orphan `-1`s.
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
                // Old rows retract only when the old gate was open; indexed once by row id
                // (O(1) match, not a per-row scan).
                let old_exploded = match (old_open, old) {
                    (true, Some(state)) => Some(self.eval_explode(state, ctx)),
                    _ => None, // old gate closed (or absent): nothing was emitted
                };
                // Some(None) = non-explode (sentinel row); Some(Some(c)) = per-entry rows
                let old_rows: Vec<(String, &Value)> = match &old_exploded {
                    Some(exploded) => Self::rows_of(exploded.as_ref()),
                    None => Vec::new(),
                };
                let old_row_index: HashMap<&str, &Value> = old_rows
                    .iter()
                    .map(|(id, element)| (id.as_str(), *element))
                    .collect();
                let new_exploded = self.eval_explode(new, ctx);
                Self::rows_of(new_exploded.as_ref())
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
                    Self::rows_of(exploded.as_ref())
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

    /// Evaluate the stream's `explode` expression (`None` when not exploding). Owned result —
    /// entries are then borrowed as rows, so no per-entry clones.
    fn eval_explode(&self, state: &Value, ctx: &ProjectionContext<'_>) -> Option<Value> {
        self.explode.as_ref().map(|explode| {
            let base = StateScope {
                state,
                element: None,
                enrichment: ctx.enrichment,
            };
            eval::eval(explode, &base)
        })
    }

    /// Rows of an evaluated explode collection (borrowed entries), or the single empty-id
    /// sentinel row when not exploding. Row id = keyed_map key / array index (stable identity).
    fn rows_of(exploded: Option<&Value>) -> Vec<(String, &Value)> {
        match exploded {
            Some(collection) => explode_entries(collection),
            None => vec![(String::new(), &NULL_ROW)],
        }
    }

    /// One row's ±1 pair, gated by the NEW row's retention (both legs ship together or
    /// neither). An UNCHANGED row (identical projected fields) emits nothing: a cancelling
    /// pair is silence, and it keeps a replayed first batch (`old == new`) from pairing an
    /// unmatched `-1` with an already-deduped `+1` (FINDINGS #3).
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
        let old_scope = self.scope(old_state, old_element, ctx.enrichment);
        let old_fields = self.project(&old_scope);
        if old_fields == new_fields {
            return Vec::new(); // unchanged row: a cancelling pair ≡ silence
        }
        let new_payload = self.stamp_sign(new_fields, Sign::Plus);
        if !self.passes_retention(&new_payload, ctx.now_secs) {
            return Vec::new(); // retention suppresses the pair together
        }
        let new_bytes = serialize(&new_payload);
        let emit = self.out_msg(
            ctx,
            Sign::Plus,
            row_id,
            self.key_of(&new_scope),
            &new_bytes,
            &new_bytes,
        );
        let old_payload = self.stamp_sign(old_fields, Sign::Minus);
        let retract = self.out_msg(
            ctx,
            Sign::Minus,
            row_id,
            self.key_of(&old_scope),
            &serialize(&old_payload),
            &new_bytes, // pair correlation anchors on the new content
        );
        vec![retract, emit]
    }

    /// A gate-close transition's lone retract (retention on the OLD payload — see
    /// [`Self::render_transition`]).
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

    /// The retention-gated emit leg for `payload` (a lone `+1`, or the shared gate for a pair).
    fn emit_if_retained(
        &self,
        payload: &Value,
        ctx: &ProjectionContext<'_>,
        sign: Sign,
        row_id: &str,
        scope: &StateScope<'_>,
    ) -> Option<OutMsg> {
        self.passes_retention(payload, ctx.now_secs).then(|| {
            self.out_msg(
                ctx,
                sign,
                row_id,
                self.key_of(scope),
                &serialize(payload),
                &serialize(payload),
            )
        })
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

    /// A row's projected fields: `common` + `row` fields (nulls omitted). Sign is stamped
    /// separately so skip-comparisons see content, not the ±1.
    fn project(&self, cx: &StateScope<'_>) -> serde_json::Map<String, Value> {
        let mut obj = serde_json::Map::new();
        for (name, source) in self.common.iter().chain(self.row.iter()) {
            let value = eval::eval(source, cx);
            if !value.is_null() {
                obj.insert(name.clone(), value); // omit nulls (== omitNothingFields)
            }
        }
        obj
    }

    /// Stamp the `sign_flag` onto projected fields (payload = fields + the optional ±1 stamp).
    fn stamp_sign(&self, mut fields: serde_json::Map<String, Value>, sign: Sign) -> Value {
        if self.sign_flag {
            fields.insert("sign_flag".to_string(), Value::from(sign.value()));
        }
        Value::Object(fields)
    }

    /// The projected Kafka message key for a row (empty when the stream has no `key`).
    fn key_of(&self, cx: &StateScope<'_>) -> String {
        self.key
            .as_ref()
            .and_then(|key| builtins::key_string(&eval::eval(key, cx)))
            .unwrap_or_default()
    }

    /// Build one output record. The ids fold the stream, stable aggregate key, row id and
    /// serialized payload CONTENT into the causation coordinate — not the mutable projected
    /// key — so a pair correlates across key changes and distinct content never shares an id.
    fn out_msg(
        &self,
        ctx: &ProjectionContext<'_>,
        sign: Sign,
        row_id: &str,
        key: String,
        payload_bytes: &[u8],
        correlation_content: &[u8],
    ) -> OutMsg {
        let partition_text = ctx.causation.partition.to_string();
        let offset_text = ctx.causation.offset.to_string();
        // correlation_id identifies the row's ±1 pair (anchored on the transition's surviving
        // content); event_id is the same parts plus the record's own payload and sign
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
            event_id: envelope_id(
                base.iter()
                    .map(|part| part.as_bytes())
                    .chain(std::iter::once(payload_bytes))
                    .chain(std::iter::once(sign.label().as_bytes())),
            ),
            correlation_id: envelope_id(
                base.iter()
                    .map(|part| part.as_bytes())
                    .chain(std::iter::once(correlation_content)),
            ),
            state_version: ctx.state_version,
            produced_at: ctx.now_secs,
            causation: ctx.causation.clone(),
            topic: self.topic.clone(),
            key,
            payload: payload_bytes.to_vec(),
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

/// `explode` a keyed map into `(entry key, &entry)` rows or an array into `(index, &item)` rows
/// (stable row ids; borrowed elements — no per-entry clones). Anything else ⇒ no rows.
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
        _ => Vec::new(),
    }
}

/// Non-explode sentinel element — module-level so [`CompiledStream::rows_of`] can return a
/// borrowed element.
static NULL_ROW: Value = Value::Null;

/// Serialize a payload once — the record bytes AND the id content hash (one source of truth).
/// Deterministic: `Map` is BTreeMap-backed (no `preserve_order`), so `to_vec` is key-sorted.
fn serialize(payload: &Value) -> Vec<u8> {
    serde_json::to_vec(payload).unwrap_or_default()
}

/// Stable FNV-1a-128 over length-prefixed byte parts, hex-encoded: 128 bits keeps birthday
/// collisions negligible; length-prefixing is injective, so part boundaries can't be forged.
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
