//! Inspect mode (INSPECT.md): structured `stitcher::inspect` tracing events for incoming
//! records, old→merged states, and outgoing sink records. PII-redacting by default,
//! sampleable/key-filtered, and non-destructive under dry-run (fresh consumer group;
//! nothing written or committed).

use serde::Serialize;
use serde_json::Value;

use crate::config::{DebugCfg, InspectFormat};
use crate::processor::OutMsg;

/// The string printed in place of redacted fields.
const MASK: &str = "***";

/// The `tracing` target all inspect events are emitted under.
pub const TARGET: &str = "stitcher::inspect";

/// Resolved inspect configuration (cheap checks on the hot path).
#[derive(Debug, Clone)]
pub struct Inspect {
    print_incoming: bool,
    print_state: bool,
    print_outgoing: bool,
    sample: f64,
    only_key_substr: String,
    redact: bool,
    redact_fields: Vec<String>,
    pretty: bool,
}

impl Inspect {
    /// Resolve from the `[debug]` config section.
    #[must_use]
    pub fn from_cfg(cfg: &DebugCfg) -> Self {
        Self {
            print_incoming: cfg.print_incoming,
            print_state: cfg.print_state,
            print_outgoing: cfg.print_outgoing,
            sample: cfg.sample,
            only_key_substr: cfg.only_key_substr.clone(),
            redact: cfg.redact,
            redact_fields: cfg.redact_fields.clone(),
            pretty: matches!(cfg.format, InspectFormat::Pretty),
        }
    }

    /// True when any printing is enabled (used to raise the log directive).
    #[must_use]
    pub fn any_printing(&self) -> bool {
        self.print_incoming || self.print_state || self.print_outgoing
    }

    /// Sampling gate: `sample >= 1.0` keeps everything, `<= 0.0` nothing.
    fn sample_ok(&self) -> bool {
        if self.sample >= 1.0 {
            return true;
        }
        if self.sample <= 0.0 {
            return false;
        }
        rand_unit() < self.sample
    }

    /// Key filter: `only_key_substr` empty ⇒ all keys pass; `None` (undecodable key)
    /// passes only when unfiltered.
    fn key_ok(&self, key: Option<&str>) -> bool {
        if self.only_key_substr.is_empty() {
            return true;
        }
        match key {
            Some(k) => k.contains(&self.only_key_substr),
            None => false,
        }
    }

    /// True when state printing is on for this key.
    #[must_use]
    pub fn state_enabled_for(&self, key: &str) -> bool {
        self.print_state && self.key_ok(Some(key))
    }

    /// Emit one incoming record (topic/partition/offset/key + rendered value).
    /// Applies sampling + key filtering internally; no-op when disabled.
    pub fn incoming(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        key: Option<&str>,
        payload: &[u8],
    ) {
        if !self.print_incoming || !self.key_ok(key) || !self.sample_ok() {
            return;
        }
        tracing::debug!(
            target: TARGET,
            topic,
            partition,
            offset,
            key = key.unwrap_or("-"),
            value = %self.render_bytes(payload),
            "INCOMING"
        );
    }

    /// Render a state (`Serialize`) through redaction + pretty/compact formatting.
    /// Called by the pipeline **before** `Merge` consumes the old value.
    #[must_use]
    pub fn render_state<S: Serialize>(&self, state: &S) -> String {
        match serde_json::to_value(state) {
            Ok(v) => self.render_value(v),
            Err(_) => "<unserializable state>".to_string(),
        }
    }

    /// Emit the old→merged pair for one key (`old`/`merged` are pre-rendered strings).
    pub fn emit_state(&self, key: &str, version: i64, old: &str, merged: &str) {
        tracing::debug!(
            target: TARGET,
            key,
            version,
            old = %old,
            merged = %merged,
            "STATE"
        );
    }

    /// Emit the outgoing sink records for a batch (topic/key + rendered payload),
    /// before retention filtering — i.e., everything the processor produced.
    pub fn outgoing(&self, msgs: &[OutMsg]) {
        if !self.print_outgoing {
            return;
        }
        for m in msgs {
            if !self.key_ok(Some(&m.key)) || !self.sample_ok() {
                continue;
            }
            tracing::debug!(
                target: TARGET,
                topic = %m.topic,
                key = %m.key,
                payload = %self.render_bytes(&m.payload),
                "OUTGOING"
            );
        }
    }

    /// Render a raw record payload: parse → redact → pretty/compact. Unparseable
    /// payloads are summarized (never printed raw — they may still carry PII).
    fn render_bytes(&self, payload: &[u8]) -> String {
        match serde_json::from_slice::<Value>(payload) {
            Ok(v) => self.render_value(v),
            Err(_) => format!("<unparseable payload: {} bytes>", payload.len()),
        }
    }

    /// Redact + format one JSON value.
    fn render_value(&self, mut v: Value) -> String {
        if self.redact {
            redact_value(&mut v, &self.redact_fields);
        }
        if self.pretty {
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| "<render failed>".to_string())
        } else {
            serde_json::to_string(&v).unwrap_or_else(|_| "<render failed>".to_string())
        }
    }
}

/// Recursively replace the values of any object key listed in `fields` with `***`.
fn redact_value(v: &mut Value, fields: &[String]) {
    match v {
        Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                if fields.iter().any(|f| f == k) {
                    *val = Value::String(MASK.to_string());
                } else {
                    redact_value(val, fields);
                }
            }
        }
        Value::Array(items) => {
            for val in items.iter_mut() {
                redact_value(val, fields);
            }
        }
        _ => {}
    }
}

/// Uniform `f64` in `[0, 1)`: 52 hash bits of a per-call `RandomState` hasher mapped onto
/// the floats of `[1, 2)` minus 1 (no `as`-cast, no thread-local state).
fn rand_unit() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    const SHIFT: u32 = u64::BITS - f64::MANTISSA_DIGITS; // keep the top 52 hash bits
    const F64_ONE: u64 = f64::to_bits(1.0); // exponent field for floats in [1, 2)

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0); // mix the per-call random seed into the final value
    f64::from_bits(F64_ONE | (hasher.finish() >> SHIFT)) - 1.0
}
