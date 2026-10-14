//! One evaluator for the whole DSL: the [`Expr`] walk, operator semantics and builtin
//! dispatch are shared; contexts differ only in path resolution and enrichment
//! availability ([`EvalContext`]), keeping their semantics identical by construction.

use std::borrow::Cow;

use serde_json::Value;
use stitcher_dsl::expr::{BinOp, Builtin, Expr, TimeFormat};

use crate::{builtins, json_util};

/// The scope an [`Expr`] evaluates against. The lifetime `'a` is tied to the DATA the
/// scope borrows (the record or merged state), not to `&self` — so `resolve_path`/`root`
/// can return `Borrowed` references that outlive the short-lived scope struct itself.
pub trait EvalContext<'a> {
    /// Resolve a dotted path to a value; a miss yields [`Value::Null`].
    fn resolve_path(&self, path: &str) -> Cow<'a, Value>;

    /// `$` — the whole record, as sent. Default: unavailable ⇒ `Null`.
    fn root(&self) -> Cow<'a, Value> {
        Cow::Owned(Value::Null)
    }

    /// `lookup(table, key)` enrichment join. Default: unavailable ⇒ `Null`.
    fn lookup(&self, _table: &str, _key: &str) -> Value {
        Value::Null
    }
}

/// Evaluate an expression in `cx`. Total: unresolved paths yield `Null`, non-numeric
/// comparisons yield `false`; never panics.
pub fn eval<'a, C: EvalContext<'a>>(expr: &Expr, cx: &C) -> Cow<'a, Value> {
    match expr {
        Expr::Root => cx.root(),
        Expr::Path(path) => cx.resolve_path(path),
        Expr::Str(s) => Cow::Owned(Value::String(s.clone())),
        Expr::Int(i) => Cow::Owned(Value::from(*i)),
        Expr::Float(f) => Cow::Owned(Value::from(*f)),
        Expr::Bool(b) => Cow::Owned(Value::from(*b)),
        Expr::Null => Cow::Owned(Value::Null),
        Expr::Not(inner) => Cow::Owned(Value::from(!builtins::truthy(&eval(inner, cx)))),
        Expr::Bin(op, lhs, rhs) => Cow::Owned(apply_bin(*op, &eval(lhs, cx), &eval(rhs, cx))),
        Expr::Call(builtin, args) => Cow::Owned(apply_builtin(*builtin, args, cx)),
    }
}

fn apply_bin(op: BinOp, lhs: &Value, rhs: &Value) -> Value {
    match op {
        BinOp::Eq => Value::from(builtins::json_eq(lhs, rhs)),
        BinOp::Ne => Value::from(builtins::json_ne(lhs, rhs)),
        BinOp::Lt => num_cmp(lhs, rhs, |a, b| a < b),
        BinOp::Le => num_cmp(lhs, rhs, |a, b| a <= b),
        BinOp::Gt => num_cmp(lhs, rhs, |a, b| a > b),
        BinOp::Ge => num_cmp(lhs, rhs, |a, b| a >= b),
        BinOp::And => Value::from(builtins::truthy(lhs) && builtins::truthy(rhs)),
        BinOp::Or => Value::from(builtins::truthy(lhs) || builtins::truthy(rhs)),
    }
}

/// Evaluate builtins lazily — each arg is evaluated inline, avoiding a per-call `Vec<Value>`.
fn apply_builtin<'a, C: EvalContext<'a>>(builtin: Builtin, args: &[Expr], cx: &C) -> Value {
    match (builtin, args) {
        (Builtin::ParseTime, [arg]) => {
            builtins::parse_time(&eval(arg, cx), None).map_or(Value::Null, Value::from)
        }
        (Builtin::ParseTime, [arg, fmt_val]) => {
            // a declared-but-unresolvable format fails closed (Null), never legacy
            let fmt_cow = eval(fmt_val, cx);
            match fmt_cow.as_str().and_then(|s| s.parse::<TimeFormat>().ok()) {
                Some(fmt) => {
                    builtins::parse_time(&eval(arg, cx), Some(fmt)).map_or(Value::Null, Value::from)
                }
                None => Value::Null,
            }
        }
        (Builtin::Meaningful, [arg]) => {
            let v = eval(arg, cx);
            // return the matched value; .into_owned() moves out of Cow without cloning a Borrowed
            if builtins::meaningful(&v).is_some() {
                v.into_owned()
            } else {
                Value::Null
            }
        }
        (Builtin::Bucket, [value, width]) => Value::from(builtins::bucket(
            json_util::as_i64(&eval(value, cx)).unwrap_or(0),
            json_util::as_i64(&eval(width, cx)).unwrap_or(0),
        )),
        (Builtin::Round, [arg]) => {
            Value::from(builtins::round_half_even(&eval(arg, cx)).unwrap_or(0))
        }
        (Builtin::Trim, [arg]) => Value::from(builtins::trim(&eval(arg, cx)).unwrap_or_default()),
        (Builtin::Lower, [arg]) => Value::from(builtins::lower(&eval(arg, cx)).unwrap_or_default()),
        (Builtin::Coalesce, [first, second]) => {
            let a = eval(first, cx);
            let b = eval(second, cx);
            builtins::coalesce(&a, &b).cloned().unwrap_or(Value::Null)
        }
        (Builtin::Latest, [arg]) => builtins::latest(&eval(arg, cx)),
        (Builtin::First, [arg]) => builtins::first(&eval(arg, cx)),
        (Builtin::List, [arg]) => builtins::list(&eval(arg, cx)),
        (Builtin::Get, [value, path]) => {
            let v = eval(value, cx);
            let p = eval(path, cx);
            builtins::get_field(&v, p.as_str().unwrap_or(""))
        }
        (Builtin::Lookup, [table, key]) => {
            let t = eval(table, cx);
            let k = eval(key, cx);
            match (t.as_str(), builtins::key_string(&k)) {
                (Some(table), Some(key)) => cx.lookup(table, &key),
                _ => Value::Null,
            }
        }
        // arity is checked at compile/load time
        _ => Value::Null,
    }
}

/// Numeric comparison via `value_as_f64` views (non-numeric ⇒ false).
fn num_cmp(l: &Value, r: &Value, op: impl FnOnce(f64, f64) -> bool) -> Value {
    Value::from(
        match (builtins::value_as_f64(l), builtins::value_as_f64(r)) {
            (Some(x), Some(y)) => op(x, y),
            _ => false,
        },
    )
}
