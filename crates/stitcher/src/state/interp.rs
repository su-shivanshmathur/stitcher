//! Runtime interpreter: a [`Processor`] driven by a compiled [`Program`], evaluating DSL
//! expressions through the shared [`crate::eval`] walker.

use std::collections::{BTreeMap, HashSet};

use serde_json::Value;
use stitcher_dsl::expr::Expr;

use crate::builtins;
use crate::eval::EvalContext;
use crate::json_util;
use crate::merge::MergeValue;
use crate::processor::{Key, Processor};
use crate::state::config::{FieldProg, KeySeg, Program};

/// Schema-driven `Processor` over [`MergeValue`] state: decode, merge and sink
/// emission all run from the compiled [`Program`] (lantern #37) — the framework
/// serves the full oracle requirement from config alone.
pub struct ConfigProcessor {
    prog: Program,
    tenant_ids: HashSet<String>,
}

impl ConfigProcessor {
    /// Construct with a compiled program and the tenant allow-list (empty = accept all).
    #[must_use]
    pub fn new(prog: Program, tenant_ids: &[String]) -> Self {
        Self {
            prog,
            tenant_ids: tenant_ids.iter().cloned().collect(),
        }
    }

    /// Admission filter (== `_decodeLog`): required non-empty paths, substring
    /// rejection, `log_type` allow-list, tenant allow-list. Rejections log their
    /// reason at `debug` (the why behind `messages_filtered("by_design")`).
    fn admit(&self, record: &Value) -> Option<()> {
        let filter = &self.prog.filter;
        for alternative_paths in &filter.require {
            let Some(required_value) = alternative_paths
                .iter()
                .find_map(|path| json_util::get_str(record, path))
            else {
                tracing::debug!(paths = ?alternative_paths, "record filtered: missing required path");
                return None;
            };
            if required_value.is_empty() {
                tracing::debug!(paths = ?alternative_paths, "record filtered: required path empty");
                return None;
            }
            if let Some(reject_token) = &filter.reject_if_contains {
                if required_value.contains(reject_token.as_str()) {
                    tracing::debug!(
                        value = %required_value,
                        reject = %reject_token,
                        "record filtered: contains reject token"
                    );
                    return None;
                }
            }
        }
        if !filter.log_type_in.is_empty() {
            let log_type = filter
                .log_type_path
                .iter()
                .find_map(|path| json_util::get_str(record, path));
            if !log_type.is_some_and(|lt| filter.log_type_in.iter().any(|allowed| allowed == lt)) {
                tracing::debug!(
                    log_type = ?log_type,
                    paths = ?filter.log_type_path,
                    "record filtered: log_type not allowed"
                );
                return None;
            }
        }
        if !self.tenant_ids.is_empty() {
            let tenant = filter
                .tenant_path
                .iter()
                .find_map(|path| json_util::get_str(record, path));
            if !tenant.is_some_and(|t| self.tenant_ids.contains(t)) {
                tracing::debug!(
                    tenant = ?tenant,
                    tenant_path = ?filter.tenant_path,
                    "record filtered: tenant not allowed"
                );
                return None;
            }
        }
        Some(())
    }

    /// Key template instantiation; a hole with no present alternative drops
    /// the record.
    fn key_of(&self, record: &Value) -> Option<Key> {
        let mut key = String::new();
        for segment in &self.prog.key {
            match segment {
                KeySeg::Literal(literal) => key.push_str(literal),
                KeySeg::Path(alternative_paths) => {
                    let Some(value) = alternative_paths
                        .iter()
                        .find_map(|path| json_util::get_str(record, path))
                    else {
                        tracing::debug!(paths = ?alternative_paths, "record dropped: primary-key segment missing");
                        return None;
                    };
                    key.push_str(value);
                }
            }
        }
        Some(key)
    }

    /// One record → its monoid contribution (`foldMap`).
    fn build_state(&self, record: &Value) -> MergeValue {
        let mut fields = BTreeMap::new();
        for (name, field) in &self.prog.fields {
            let contribution = build_field(field, record);
            fields.insert(name.clone(), contribution);
        }
        MergeValue::Map(fields)
    }
}

impl Processor for ConfigProcessor {
    type State = MergeValue;

    fn state_version(&self) -> i64 {
        self.prog.version
    }

    fn id_type(&self) -> &str {
        &self.prog.id_type
    }

    fn decode_stored(&self, blob: &[u8]) -> Option<Self::State> {
        let root: Value = serde_json::from_slice(blob).ok()?;
        let obj = root.as_object()?;
        let mut fields = BTreeMap::new();
        for (name, field) in &self.prog.fields {
            // absent ⇔ identity (serialized states omit identity entries)
            if let Some(v) = obj.get(name) {
                fields.insert(name.clone(), decode_field(field, v)?);
            }
        }
        Some(MergeValue::Map(fields))
    }

    fn primary_key(&self, raw: &[u8]) -> Option<Key> {
        let record: Value = serde_json::from_slice(raw).ok()?;
        self.key_of(&record)
    }

    fn decode(&self, raw: &[u8]) -> Option<Self::State> {
        self.decode_with_key(raw).map(|(_, s)| s)
    }

    fn decode_with_key(&self, raw: &[u8]) -> Option<(Key, Self::State)> {
        let record: Value = serde_json::from_slice(raw).ok()?;
        self.admit(&record)?;
        let key = self.key_of(&record)?;
        let state = self.build_state(&record);
        Some((key, state))
    }
}

/// Field node → its contribution for one record (mirror of `field_build`).
fn build_field(field: &FieldProg, record: &Value) -> MergeValue {
    match field {
        FieldProg::LatestBy {
            when,
            comparator,
            payload,
        } => {
            if let Some(when_expr) = when {
                if !builtins::truthy(&eval(when_expr, record)) {
                    // == LatestBy::default()
                    return MergeValue::LatestBy {
                        comparator: 0,
                        payload: Value::Null,
                    };
                }
            }
            build_latest_by(comparator, payload, record)
        }
        FieldProg::KeyedMap { when, key, value } => {
            if let Some(when_expr) = when {
                if !builtins::truthy(&eval(when_expr, record)) {
                    return MergeValue::Map(BTreeMap::new()); // == KeyedMap::default()
                }
            }
            match builtins::key_string(&eval(key, record)) {
                Some(entry_key) => {
                    let mut entries = BTreeMap::new();
                    entries.insert(
                        entry_key,
                        build_latest_by(&value.comparator, &value.payload, record),
                    );
                    MergeValue::Map(entries)
                }
                None => MergeValue::Map(BTreeMap::new()),
            }
        }
        FieldProg::Last { when, value } => {
            if let Some(when_expr) = when {
                if !builtins::truthy(&eval(when_expr, record)) {
                    return MergeValue::Null; // == LastWrite(None)
                }
            }
            let value = eval(value, record);
            match builtins::meaningful(&value) {
                Some(_) => MergeValue::Leaf(value),
                None => MergeValue::Null,
            }
        }
        FieldProg::Once { when, value } => {
            if let Some(when_expr) = when {
                if !builtins::truthy(&eval(when_expr, record)) {
                    return MergeValue::Null; // == FirstWrite(None)
                }
            }
            let value = eval(value, record);
            match builtins::meaningful(&value) {
                Some(_) => MergeValue::Once(value),
                None => MergeValue::Null,
            }
        }
        FieldProg::Counter { when } => {
            let counts_this_record = match when {
                Some(when_expr) => builtins::truthy(&eval(when_expr, record)),
                None => true,
            };
            MergeValue::Counter(i64::from(counts_this_record))
        }
    }
}

/// `latest_by` node contribution (comparator first, then payload — evaluation order
/// matches the generated code's `let` sequence).
fn build_latest_by(comparator: &Expr, payload: &Expr, record: &Value) -> MergeValue {
    let comparator_value = eval(comparator, record);
    let comparator = builtins::parse_time(&comparator_value)
        .or_else(|| json_util::as_i64(&comparator_value))
        .unwrap_or(0);
    MergeValue::LatestBy {
        comparator,
        payload: eval(payload, record),
    }
}

/// Stored JSON → field value, per the compiled node kind (schema-driven decode:
/// `MergeValue`'s JSON shape is ambiguous without it). Strict like typed serde:
/// malformed shapes ⇒ `None` (state treated absent), never a partial read.
fn decode_field(field: &FieldProg, stored: &Value) -> Option<MergeValue> {
    match field {
        FieldProg::LatestBy { .. } => decode_latest_by(stored),
        FieldProg::KeyedMap { .. } => {
            // inner nodes are always latest_by (v1): each entry shape-checks directly
            let entries = stored.as_object()?;
            let mut decoded = BTreeMap::new();
            for (entry_key, entry) in entries {
                decoded.insert(entry_key.clone(), decode_latest_by(entry)?);
            }
            Some(MergeValue::Map(decoded))
        }
        FieldProg::Last { .. } => Some(if stored.is_null() {
            MergeValue::Null
        } else {
            MergeValue::Leaf(stored.clone())
        }),
        FieldProg::Once { .. } => Some(if stored.is_null() {
            MergeValue::Null
        } else {
            MergeValue::Once(stored.clone())
        }),
        FieldProg::Counter { .. } => Some(MergeValue::Counter(stored.as_i64()?)),
    }
}

fn decode_latest_by(v: &Value) -> Option<MergeValue> {
    let obj = v.as_object()?;
    let comparator = obj.get("comparator").and_then(Value::as_i64)?;
    let payload = obj.get("payload")?.clone();
    Some(MergeValue::LatestBy {
        comparator,
        payload,
    })
}

/// Evaluation scope for the state-build stage: paths resolve against one raw record,
/// and enrichment is unavailable (`lookup` ⇒ `Null`, the [`EvalContext`] default).
struct RecordScope<'a> {
    record: &'a Value,
}

impl EvalContext for RecordScope<'_> {
    fn resolve_path(&self, path: &str) -> Value {
        json_util::get_path(self.record, path)
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn root(&self) -> Value {
        self.record.clone()
    }
}

/// Evaluate a compiled expression against one record via the shared [`crate::eval`]
/// walker (semantics thus stay identical to the transform + generated code).
fn eval(expr: &Expr, record: &Value) -> Value {
    crate::eval::eval(expr, &RecordScope { record })
}

