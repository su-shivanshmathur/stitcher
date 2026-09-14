//! Dotted-path access into `serde_json::Value` records (shared by processors + codegen).

use serde_json::Value;

/// Resolve `a.b.c` against `v`. Arrays are NOT indexed by numeric segments
/// (object-only navigation); `None` on any miss.
#[must_use]
pub fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Resolve a path and return it as a string slice.
#[must_use]
pub fn get_str<'a>(v: &'a Value, path: &str) -> Option<&'a str> {
    get_path(v, path)?.as_str()
}

/// Best-effort numeric extraction (accepts `5`, `5.0`, `"5"`).
#[must_use]
pub fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(f64_to_i64)),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .ok()
            .or_else(|| s.trim().parse::<f64>().ok().map(f64_to_i64)),
        _ => None,
    }
}

/// Truncating float → int conversion, saturating at bounds,
/// NaN → 0. Rust's `f64 as i64` is already saturating + NaN→0, so one cast suffices.
#[allow(clippy::as_conversions)] // truncating cast is precisely the intent here
fn f64_to_i64(f: f64) -> i64 {
    f as i64
}
