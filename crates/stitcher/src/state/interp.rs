//! Runtime interpreter: a [`Processor`] driven entirely by a compiled [`Program`]
//! (lantern #37). Evaluation mirrors `stitcher_macro`'s generated code expression for
//! expression — the oracle/codegen/interpreter parity smoke (PLAN #39) pins that.

use std::collections::{BTreeMap, HashSet};

use serde_json::Value;
use stitcher_dsl::expr::{BinOp, Builtin, Expr};

use crate::builtins;
use crate::json_util;
use crate::merge::MergeValue;
use crate::processor::{Key, OutMsg, Processor, Sign};
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
    /// rejection, `log_type` allow-list, tenant allow-list.
    fn admit(&self, record: &Value) -> Option<()> {
        let f = &self.prog.filter;
        for path in &f.require {
            let v = json_util::get_str(record, path)?;
            if v.is_empty() {
                return None;
            }
            if let Some(sub) = &f.reject_if_contains {
                if v.contains(sub.as_str()) {
                    return None;
                }
            }
        }
        if !f.log_type_in.is_empty() {
            match json_util::get_str(record, "log.log_type") {
                Some(lt) if f.log_type_in.iter().any(|a| a == lt) => {}
                _ => return None,
            }
        }
        if !self.tenant_ids.is_empty() {
            match json_util::get_str(record, &f.tenant_path) {
                Some(t) if self.tenant_ids.contains(t) => {}
                _ => return None,
            }
        }
        Some(())
    }

    /// Key template instantiation; a missing hole value drops the record.
    fn key_of(&self, record: &Value) -> Option<Key> {
        let mut key = String::new();
        for seg in &self.prog.key {
            match seg {
                KeySeg::Literal(lit) => key.push_str(lit),
                KeySeg::Path(p) => key.push_str(json_util::get_str(record, p)?),
            }
        }
        Some(key)
    }

    /// One record → its monoid contribution (`foldMap`).
    fn build_state(&self, record: &Value) -> MergeValue {
        let mut fields = BTreeMap::new();
        for (name, field) in &self.prog.fields {
            let v = build_field(field, record);
            fields.insert(name.clone(), v);
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

    fn encode(&self, state: &Self::State, sign: Sign) -> Vec<OutMsg> {
        // mirror of the codegen's `gen_sink`: gate on the field carrying state, then
        // fan out map entries or emit one record per sink.
        let mut out = Vec::new();
        let fields = match state {
            MergeValue::Map(m) => m,
            _ => return out,
        };
        let sign_flag = sign.value();
        for sink in &self.prog.sinks {
            let Some(value) = fields.get(&sink.field) else {
                continue; // absent ⇔ identity ⇔ gate closed
            };
            if value.is_identity() {
                continue; // == typed is_default/is_empty/is_none/is_zero gates
            }
            if sink.fan_out {
                if let MergeValue::Map(entries) = value {
                    for (k, entry) in entries {
                        let mut obj = serde_json::Map::with_capacity(3);
                        obj.insert("sign_flag".to_string(), Value::from(sign_flag));
                        obj.insert("key".to_string(), Value::String(k.clone()));
                        obj.insert(sink.field.clone(), entry.to_value_full());
                        out.push(out_msg(&sink.topic, k.clone(), &Value::Object(obj)));
                    }
                }
            } else {
                let mut obj = serde_json::Map::with_capacity(2);
                obj.insert("sign_flag".to_string(), Value::from(sign_flag));
                obj.insert(sink.field.clone(), value.to_value_full());
                let payload = Value::Object(obj);
                let key = match &sink.key_path {
                    Some(p) => {
                        let full = format!("{}.payload.{}", sink.field, p);
                        json_util::get_str(&payload, &full)
                            .unwrap_or(&sink.field)
                            .to_string()
                    }
                    None => sink.field.clone(),
                };
                out.push(out_msg(&sink.topic, key, &payload));
            }
        }
        out
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
            if let Some(w) = when {
                if !builtins::truthy(&eval(w, record)) {
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
            if let Some(w) = when {
                if !builtins::truthy(&eval(w, record)) {
                    return MergeValue::Map(BTreeMap::new()); // == KeyedMap::default()
                }
            }
            match builtins::key_string(&eval(key, record)) {
                Some(k) => {
                    let mut m = BTreeMap::new();
                    m.insert(
                        k,
                        build_latest_by(&value.comparator, &value.payload, record),
                    );
                    MergeValue::Map(m)
                }
                None => MergeValue::Map(BTreeMap::new()),
            }
        }
        FieldProg::Last { when, value } => {
            if let Some(w) = when {
                if !builtins::truthy(&eval(w, record)) {
                    return MergeValue::Null; // == LastWrite(None)
                }
            }
            let v = eval(value, record);
            match builtins::meaningful(&v) {
                Some(_) => MergeValue::Leaf(v),
                None => MergeValue::Null,
            }
        }
        FieldProg::Counter { when } => {
            let on = match when {
                Some(w) => builtins::truthy(&eval(w, record)),
                None => true,
            };
            MergeValue::Counter(i64::from(on))
        }
    }
}

/// `latest_by` node contribution (comparator first, then payload — evaluation order
/// matches the generated code's `let` sequence).
fn build_latest_by(comparator: &Expr, payload: &Expr, record: &Value) -> MergeValue {
    let c = eval(comparator, record);
    let comparator = builtins::parse_time(&c)
        .or_else(|| json_util::as_i64(&c))
        .unwrap_or(0);
    MergeValue::LatestBy {
        comparator,
        payload: eval(payload, record),
    }
}

/// Stored JSON → field value, per the compiled node kind (schema-driven decode:
/// `MergeValue`'s JSON shape is ambiguous without it). Strict like typed serde:
/// malformed shapes ⇒ `None` (state treated absent), never a partial read.
fn decode_field(field: &FieldProg, v: &Value) -> Option<MergeValue> {
    match field {
        FieldProg::LatestBy { .. } => decode_latest_by(v),
        FieldProg::KeyedMap { .. } => {
            // inner nodes are always latest_by (v1): each entry shape-checks directly
            let obj = v.as_object()?;
            let mut m = BTreeMap::new();
            for (k, inner) in obj {
                m.insert(k.clone(), decode_latest_by(inner)?);
            }
            Some(MergeValue::Map(m))
        }
        FieldProg::Last { .. } => Some(if v.is_null() {
            MergeValue::Null
        } else {
            MergeValue::Leaf(v.clone())
        }),
        FieldProg::Counter { .. } => Some(MergeValue::Counter(v.as_i64()?)),
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

/// Expression evaluation, mirroring the generated code's semantics. Operands are
/// pure (total, side-effect-free), so codegen's short-circuiting `&&`/`||` and its
/// double-evaluated `meaningful(x)` argument are unobservable against this eager,
/// once-evaluated walk.
fn eval(expr: &Expr, record: &Value) -> Value {
    match expr {
        Expr::Path(p) => json_util::get_path(record, p)
            .cloned()
            .unwrap_or(Value::Null),
        Expr::Str(s) => Value::String(s.clone()),
        Expr::Int(i) => Value::from(*i),
        Expr::Float(f) => Value::from(*f),
        Expr::Bool(b) => Value::from(*b),
        Expr::Null => Value::Null,
        Expr::Not(inner) => Value::from(!builtins::truthy(&eval(inner, record))),
        Expr::Bin(op, l, r) => {
            let l = eval(l, record);
            let r = eval(r, record);
            match op {
                BinOp::Eq => Value::from(builtins::json_eq(&l, &r)),
                BinOp::Ne => Value::from(builtins::json_ne(&l, &r)),
                BinOp::Lt => num_cmp(&l, &r, |x, y| x < y),
                BinOp::Le => num_cmp(&l, &r, |x, y| x <= y),
                BinOp::Gt => num_cmp(&l, &r, |x, y| x > y),
                BinOp::Ge => num_cmp(&l, &r, |x, y| x >= y),
                BinOp::And => Value::from(builtins::truthy(&l) && builtins::truthy(&r)),
                BinOp::Or => Value::from(builtins::truthy(&l) || builtins::truthy(&r)),
            }
        }
        Expr::Call(builtin, args) => {
            let args: Vec<Value> = args.iter().map(|a| eval(a, record)).collect();
            match (builtin, args.as_slice()) {
                (Builtin::ParseTime, [a]) => match builtins::parse_time(a) {
                    Some(v) => Value::from(v),
                    None => Value::Null,
                },
                (Builtin::Meaningful, [a]) => {
                    if builtins::meaningful(a).is_some() {
                        a.clone()
                    } else {
                        Value::Null
                    }
                }
                (Builtin::Bucket, [a, b]) => Value::from(builtins::bucket(
                    json_util::as_i64(a).unwrap_or(0),
                    json_util::as_i64(b).unwrap_or(0),
                )),
                (Builtin::Round, [a]) => Value::from(builtins::round_half_even(a).unwrap_or(0)),
                (Builtin::Trim, [a]) => Value::from(builtins::trim(a).unwrap_or_default()),
                (Builtin::Lower, [a]) => Value::from(builtins::lower(a).unwrap_or_default()),
                (Builtin::Coalesce, [a, b]) => match builtins::coalesce(a, b) {
                    Some(v) => v.clone(),
                    None => Value::Null,
                },
                // arity is checked at program-compile time
                _ => Value::Null,
            }
        }
    }
}

/// Numeric comparison via `value_as_f64` views (non-numeric ⇒ false, like codegen).
fn num_cmp(l: &Value, r: &Value, op: impl FnOnce(f64, f64) -> bool) -> Value {
    Value::from(
        match (builtins::value_as_f64(l), builtins::value_as_f64(r)) {
            (Some(x), Some(y)) => op(x, y),
            _ => false,
        },
    )
}

/// One sink record (payload bytes serialize like the generated code: `{}` on failure).
fn out_msg(topic: &str, key: String, payload: &Value) -> OutMsg {
    OutMsg {
        topic: topic.to_string(),
        key,
        payload: serde_json::to_vec(payload).unwrap_or_else(|_| b"{}".to_vec()),
    }
}
