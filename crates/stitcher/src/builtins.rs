//! DSL runtime built-ins, used by the shared evaluator ([`crate::eval`]).

use serde_json::Value;
use stitcher_dsl::TimeFormat;

use crate::json_util;

/// Normalize a time value to epoch nanoseconds; `fmt` declares the input unit
/// (no format ⇒ number as-is, string as numeric or RFC3339).
#[must_use]
pub fn parse_time(v: &Value, fmt: Option<TimeFormat>) -> Option<i64> {
    match fmt {
        None => match v {
            Value::Number(_) => json_util::as_i64(v),
            Value::String(s) => json_util::as_i64(v).or_else(|| parse_rfc3339_nanos(s.trim())),
            _ => None,
        },
        Some(TimeFormat::Rfc3339) => parse_rfc3339_nanos(v.as_str()?.trim()),
        Some(TimeFormat::EpochSeconds) => json_util::as_i64(v)?.checked_mul(1_000_000_000),
        Some(TimeFormat::EpochMillis) => json_util::as_i64(v)?.checked_mul(1_000_000),
        Some(TimeFormat::EpochMicros) => json_util::as_i64(v)?.checked_mul(1_000),
        Some(TimeFormat::EpochNanos) => json_util::as_i64(v),
    }
}

/// RFC3339 / ISO-8601 → epoch nanos (accepts offsets; missing offset assumed UTC).
fn parse_rfc3339_nanos(s: &str) -> Option<i64> {
    const SEC: i64 = 1_000_000_000;
    // Full RFC3339 with offset, e.g. "2024-05-05T12:34:56.789Z" / "...+05:30".
    if let Ok(ts) = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339) {
        return Some(
            ts.unix_timestamp()
                .saturating_mul(SEC)
                .saturating_add(i64::from(ts.nanosecond())),
        );
    }
    // ISO without offset → treat as UTC.
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    time::PrimitiveDateTime::parse(s, &fmt)
        .ok()
        .map(|p| p.assume_utc().unix_timestamp().saturating_mul(SEC))
}

/// `meaningful`: drop `null`, `"null"`, `"UNKNOWN"`, `"unknown"`, `""`.
#[must_use]
pub fn meaningful(v: &Value) -> Option<&Value> {
    match v {
        Value::Null => None,
        Value::String(s) => {
            let t = s.trim();
            if t.is_empty() || t.eq_ignore_ascii_case("unknown") || t == "null" {
                None
            } else {
                Some(v)
            }
        }
        _ => Some(v),
    }
}

/// `bucket`: floor `v` to a multiple of `width` (euclidean, so negatives behave sanely).
#[must_use]
pub fn bucket(v: i64, width: i64) -> i64 {
    if width <= 0 {
        return v;
    }
    v.div_euclid(width).saturating_mul(width)
}

/// `round`: banker's rounding (round-half-to-even) on floats represented as JSON numbers.
#[must_use]
pub fn round_half_even(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => Some(match n.as_f64() {
            Some(f) => round_f64(f),
            None => return n.as_i64().or_else(|| n.as_u64().and_then(u64_to_i64)),
        }),
        Value::String(s) => s.trim().parse::<f64>().ok().map(round_f64),
        _ => None,
    }
}

/// `f64 → i64` with banker's rounding (ties to even); saturating `as`-cast semantics.
fn round_f64(f: f64) -> i64 {
    #[allow(clippy::as_conversions)] // truncating cast; saturating semantics
    {
        f.round_ties_even() as i64
    }
}

fn u64_to_i64(u: u64) -> Option<i64> {
    i64::try_from(u).ok()
}

/// `trim` a string value.
#[must_use]
pub fn trim(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.trim().to_string())
}

/// `lower` a string value.
#[must_use]
pub fn lower(v: &Value) -> Option<String> {
    v.as_str().map(str::to_ascii_lowercase)
}

/// `coalesce`: first meaningful argument.
#[must_use]
pub fn coalesce<'a>(a: &'a Value, b: &'a Value) -> Option<&'a Value> {
    meaningful(a).or_else(|| meaningful(b))
}

// ---------------------------------------------------------------------------
// truthiness / comparison / key helpers
// ---------------------------------------------------------------------------

/// Truthiness for DSL predicates: `Null`/`false`/`""`/`0` are false; everything else true.
#[must_use]
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_none_or(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Numeric view of a JSON value (for `<`/`<=`/`>`/`>=`).
#[must_use]
pub fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Reference equality used by generated code (no owned temporaries → clippy-clean).
#[must_use]
pub fn json_eq(a: &Value, b: &Value) -> bool {
    a == b
}

/// Reference inequality.
#[must_use]
pub fn json_ne(a: &Value, b: &Value) -> bool {
    a != b
}

/// A map-key string out of a JSON value: meaningful strings pass through, numbers stringify,
/// everything else is `None` (missing id ⇒ no insert).
#[must_use]
pub fn key_string(v: &Value) -> Option<String> {
    meaningful(v)?;
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(_) | Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

// ---------------------------------------------------------------------------
// collection reducers + path extraction (transform DSL; TRANSFORMER.md)
// ---------------------------------------------------------------------------

/// `latest(map)`: the `payload` of the keyed-map entry with the greatest stored
/// `comparator` (the aggregate's own recency — the most-recently-updated entry).
/// `Null` when empty or not a `{id: {comparator, payload}}` map.
#[must_use]
pub fn latest(map: &Value) -> Value {
    reduce_by_comparator(map, true)
}

/// `first(map)`: the `payload` of the entry with the smallest `comparator`.
#[must_use]
pub fn first(map: &Value) -> Value {
    reduce_by_comparator(map, false)
}

fn reduce_by_comparator(map: &Value, want_max: bool) -> Value {
    let Value::Object(entries) = map else {
        return Value::Null;
    };
    let mut best: Option<(i64, &Value)> = None; // (comparator, entry)
    for entry in entries.values() {
        let comparator = entry.get("comparator").and_then(Value::as_i64).unwrap_or(0);
        let better = match best {
            None => true,
            Some((incumbent, _)) if want_max => comparator > incumbent,
            Some((incumbent, _)) => comparator < incumbent,
        };
        if better {
            best = Some((comparator, entry));
        }
    }
    best.and_then(|(_, entry)| entry.get("payload").cloned())
        .unwrap_or(Value::Null)
}

/// `list(map)`: an array of every keyed-map entry's `payload` (or the array as-is
/// when already an array). Empty array for anything else.
#[must_use]
pub fn list(collection: &Value) -> Value {
    match collection {
        Value::Object(entries) => Value::Array(
            entries
                .values()
                .map(|entry| entry.get("payload").cloned().unwrap_or(Value::Null))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.clone()),
        _ => Value::Array(Vec::new()),
    }
}

/// `get(value, path)`: dotted-path lookup into a value — path into a call result,
/// e.g. `get(latest(state.attempts), 'connector')`. `Null` on any miss.
#[must_use]
pub fn get_field(value: &Value, path: &str) -> Value {
    json_util::get_path(value, path).cloned().unwrap_or(Value::Null)
}
