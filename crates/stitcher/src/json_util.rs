//! Dotted-path access into `serde_json::Value`.

use serde_json::Value;

/// Resolve `a.b.c`; arrays are not indexed; `None` on any miss.
#[must_use]
pub fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

#[must_use]
pub fn get_str<'a>(v: &'a Value, path: &str) -> Option<&'a str> {
    get_path(v, path)?.as_str()
}

/// Accepts `5`, `5.0`, `"5"`.
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

/// `f64 as i64`: truncating, saturating, NaN → 0 — precisely the intent.
#[allow(clippy::as_conversions)] // truncating cast is precisely the intent here
fn f64_to_i64(f: f64) -> i64 {
    f as i64
}
