//! One evaluator for the whole DSL. The [`Expr`] walk, operator semantics and builtin
//! dispatch are shared; only what varies between contexts — how a path resolves and
//! whether enrichment `lookup` is available — is abstracted behind [`EvalContext`]. The
//! state interpreter and the output transformer are then just two `EvalContext` impls
//! over the same [`eval`], which keeps their semantics identical by construction.

use serde_json::Value;
use stitcher_dsl::expr::{BinOp, Builtin, Expr};

use crate::{builtins, json_util};

/// The scope an [`Expr`] evaluates against.
///
/// Path roots differ (a raw record vs. the merged state + `each` element) and
/// enrichment is only reachable in some contexts, so those are the trait's methods;
/// literals, operators and pure builtins are handled once in [`eval`].
pub trait EvalContext {
    /// Resolve a dotted path to a value; a miss yields [`Value::Null`].
    fn resolve_path(&self, path: &str) -> Value;

    /// `lookup(table, key)` enrichment join. Default: enrichment is unavailable in
    /// this context, so it yields [`Value::Null`].
    fn lookup(&self, _table: &str, _key: &str) -> Value {
        Value::Null
    }
}

/// Evaluate an expression in `cx`. Total: unresolved paths yield `Null`, non-numeric
/// comparisons yield `false`; never panics.
pub fn eval<C: EvalContext>(expr: &Expr, cx: &C) -> Value {
    match expr {
        Expr::Path(path) => cx.resolve_path(path),
        Expr::Str(s) => Value::String(s.clone()),
        Expr::Int(i) => Value::from(*i),
        Expr::Float(f) => Value::from(*f),
        Expr::Bool(b) => Value::from(*b),
        Expr::Null => Value::Null,
        Expr::Not(inner) => Value::from(!builtins::truthy(&eval(inner, cx))),
        Expr::Bin(op, lhs, rhs) => apply_bin(*op, &eval(lhs, cx), &eval(rhs, cx)),
        Expr::Call(builtin, args) => {
            let values: Vec<Value> = args.iter().map(|arg| eval(arg, cx)).collect();
            apply_builtin(*builtin, &values, cx)
        }
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

fn apply_builtin<C: EvalContext>(builtin: Builtin, args: &[Value], cx: &C) -> Value {
    match (builtin, args) {
        (Builtin::ParseTime, [arg]) => builtins::parse_time(arg).map_or(Value::Null, Value::from),
        (Builtin::Meaningful, [arg]) => {
            if builtins::meaningful(arg).is_some() {
                arg.clone()
            } else {
                Value::Null
            }
        }
        (Builtin::Bucket, [value, width]) => Value::from(builtins::bucket(
            json_util::as_i64(value).unwrap_or(0),
            json_util::as_i64(width).unwrap_or(0),
        )),
        (Builtin::Round, [arg]) => Value::from(builtins::round_half_even(arg).unwrap_or(0)),
        (Builtin::Trim, [arg]) => Value::from(builtins::trim(arg).unwrap_or_default()),
        (Builtin::Lower, [arg]) => Value::from(builtins::lower(arg).unwrap_or_default()),
        (Builtin::Coalesce, [first, second]) => {
            builtins::coalesce(first, second).cloned().unwrap_or(Value::Null)
        }
        (Builtin::Latest, [arg]) => builtins::latest(arg),
        (Builtin::First, [arg]) => builtins::first(arg),
        (Builtin::List, [arg]) => builtins::list(arg),
        (Builtin::Get, [value, path]) => builtins::get_field(value, path.as_str().unwrap_or("")),
        (Builtin::Lookup, [table, key]) => match (table.as_str(), builtins::key_string(key)) {
            (Some(table), Some(key)) => cx.lookup(table, &key),
            _ => Value::Null,
        },
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
