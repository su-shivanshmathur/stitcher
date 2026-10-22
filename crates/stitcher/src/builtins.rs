//! DSL runtime built-ins, used by the shared evaluator ([`crate::eval`]).

use serde_json::Value;
use stitcher_dsl::TimeFormat;

use crate::constants::NANOS_PER_SEC;
use crate::json_util;

/// `fmt` declares the input unit; absent ⇒ number as-is, string numeric or RFC3339.
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
    // Full RFC3339 with offset, e.g. "2024-05-05T12:34:56.789Z" / "...+05:30".
    if let Ok(ts) = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339) {
        return Some(
            ts.unix_timestamp()
                .saturating_mul(NANOS_PER_SEC)
                .saturating_add(i64::from(ts.nanosecond())),
        );
    }
    // ISO without offset → treat as UTC.
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    time::PrimitiveDateTime::parse(s, &fmt)
        .ok()
        .map(|p| p.assume_utc().unix_timestamp().saturating_mul(NANOS_PER_SEC))
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

#[must_use]
pub fn trim(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.trim().to_string())
}

#[must_use]
pub fn lower(v: &Value) -> Option<String> {
    v.as_str().map(str::to_ascii_lowercase)
}

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
/// Strings that parse as non-finite (NaN, inf) are rejected; JSON numbers are always finite.
#[must_use]
pub fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok().filter(|f| f.is_finite()),
        _ => None,
    }
}

/// Numbers compare numerically (`1 == 1.0`); else structural.
#[must_use]
pub fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => num_eq(x, y),
        _ => a == b,
    }
}

#[must_use]
pub fn json_ne(a: &Value, b: &Value) -> bool {
    !json_eq(a, b)
}

/// Exact over integers, f64 once a float is involved.
fn num_eq(x: &serde_json::Number, y: &serde_json::Number) -> bool {
    if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
        return xi == yi;
    }
    if let (Some(xu), Some(yu)) = (x.as_u64(), y.as_u64()) {
        return xu == yu;
    }
    match (x.as_f64(), y.as_f64()) {
        (Some(xf), Some(yf)) => xf == yf,
        _ => false,
    }
}

/// Meaningful strings pass, numbers stringify, else `None`.
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
// collection reducers + path extraction (transform DSL)
// ---------------------------------------------------------------------------

/// `latest(map)`: payload of the greatest-`comparator` entry; `Null` if not a keyed map.
#[must_use]
pub fn latest(map: &Value) -> Value {
    reduce_by_comparator(map, true)
}

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

/// Rank entries by numeric `field`; return the winning `(payload, field_value)`.
/// Non-numeric field values are skipped; ties keep the incumbent; `None` when empty / no numeric match.
/// One `get_field` walk per item (incumbent's field value cached); borrows from `coll`.
fn rank_by_field<'a>(coll: &'a Value, field: &str, want_max: bool) -> Option<(&'a Value, Value)> {
    let mut best: Option<(&Value, Value)> = None;
    for item in iter_payloads(coll) {
        let field_value = get_field(item, field);
        if value_as_f64(&field_value).is_none() {
            continue;
        }
        let take = match &best {
            None => true,
            Some((_, incumbent)) => match cmp_num(&field_value, incumbent) {
                Some(std::cmp::Ordering::Greater) => want_max,
                Some(std::cmp::Ordering::Less) => !want_max,
                _ => false,
            },
        };
        if take {
            best = Some((item, field_value));
        }
    }
    best
}

/// `latest(map, field)` / `first(map, field)`: payload of the entry whose `field` is greatest
/// (`want_max=true`) or least (`want_max=false`). Non-numeric field values are skipped; ties keep
/// the incumbent (same policy as [`rank_by_field`]). `Null` when empty or no numeric match.
#[must_use]
pub fn pick_by_field(coll: &Value, field: &str, want_max: bool) -> Value {
    rank_by_field(coll, field, want_max).map_or(Value::Null, |(item, _)| item.clone())
}

/// Every entry's `payload` (or the entry itself for plain objects); array items as-is; else empty.
/// Delegates to `iter_payloads` so `list(x)` and direct collection use produce identical payloads.
#[must_use]
pub fn list(collection: &Value) -> Value {
    Value::Array(iter_payloads(collection).into_iter().cloned().collect())
}

/// Dotted lookup; `Null` on any miss.
#[must_use]
pub fn get_field(value: &Value, path: &str) -> Value {
    json_util::get_path(value, path)
        .cloned()
        .unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------
// new builtins: parse_json, count, filter, sum, min, max
// ---------------------------------------------------------------------------

/// Yields each item: object ⇒ entry values (falling back to the entry itself when no `"payload"`
/// key is present), array ⇒ items, else empty.
fn iter_payloads(coll: &Value) -> Vec<&Value> {
    match coll {
        Value::Object(entries) => entries
            .values()
            .map(|entry| entry.get("payload").unwrap_or(entry))
            .collect(),
        Value::Array(items) => items.iter().collect(),
        _ => Vec::new(),
    }
}

/// `parse_json(x)`: parse a JSON string; non-string or parse failure ⇒ `Null`.
#[must_use]
pub fn parse_json(v: &Value) -> Value {
    v.as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null)
}

/// `count(coll)`: object key count or array length as `i64`; non-collection ⇒ `Null`.
#[must_use]
pub fn count_collection(coll: &Value) -> Value {
    match coll {
        Value::Object(m) => Value::from(i64::try_from(m.len()).unwrap_or(i64::MAX)),
        Value::Array(a) => Value::from(i64::try_from(a.len()).unwrap_or(i64::MAX)),
        _ => Value::Null,
    }
}

/// `filter(coll, field, want)`: array of payloads where `get_field(payload, field)` equals `want`.
#[must_use]
pub fn filter_collection(coll: &Value, field: &str, want: &Value) -> Value {
    Value::Array(
        iter_payloads(coll)
            .into_iter()
            .filter(|item| json_eq(&get_field(item, field), want))
            .cloned()
            .collect(),
    )
}

/// Non-truncating integer extraction: `Number` only if serde reports it as exact i64;
/// `String` only if it trims to a valid i64 literal.
fn exact_int(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// `sum(coll, field)`: exact-integer accumulation in i128, falling to f64 on any fractional value.
/// Non-numeric field values are skipped. Emits i64 when all inputs are integer-typed, f64 if any input is float-typed.
#[must_use]
pub fn sum_field(coll: &Value, field: &str) -> Value {
    let mut int_acc: i128 = 0;
    let mut float_acc = 0.0_f64;
    let mut any_float = false;
    for item in iter_payloads(coll) {
        let field_value = get_field(item, field);
        match exact_int(&field_value) {
            Some(i) => int_acc = int_acc.saturating_add(i128::from(i)),
            None => {
                if let Some(f) = value_as_f64(&field_value) {
                    float_acc += f;
                    any_float = true;
                }
            }
        }
    }
    if any_float {
        #[allow(clippy::as_conversions)] // i128→f64 for a mixed-type sum; precision is best-effort
        Value::from(float_acc + int_acc as f64)
    } else {
        i64::try_from(int_acc).map_or_else(
            |_| {
                #[allow(clippy::as_conversions)] // overflow fallback only
                Value::from(int_acc as f64)
            },
            Value::from,
        )
    }
}

/// Exact integer comparison, falling back to `f64::total_cmp` for fractional values.
/// Mirrors `num_eq`'s strategy: avoids the 53-bit mantissa collapse for large integers
/// (e.g. epoch nanos ~1.7e18 > 2^53).
fn cmp_num(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (exact_int(a), exact_int(b)) {
        (Some(x), Some(y)) => Some(x.cmp(&y)),
        _ => value_as_f64(a)
            .zip(value_as_f64(b))
            .map(|(x, y)| x.total_cmp(&y)),
    }
}

/// `min(coll, field)`: least numeric field over the collection; empty or no numeric field ⇒ `Null`.
/// Returns the original value at exact type/precision.
#[must_use]
pub fn min_field(coll: &Value, field: &str) -> Value {
    rank_by_field(coll, field, false).map_or(Value::Null, |(_, field_value)| field_value)
}

/// `max(coll, field)`: greatest numeric field over the collection; empty or no numeric field ⇒ `Null`.
/// Returns the original value at exact type/precision.
#[must_use]
pub fn max_field(coll: &Value, field: &str) -> Value {
    rank_by_field(coll, field, true).map_or(Value::Null, |(_, field_value)| field_value)
}
