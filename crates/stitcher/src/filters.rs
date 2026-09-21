//! Retention: keep a record iff `now_s − record_s < retention_days × 86400`.

/// Raw check on epoch-seconds timestamps.
#[must_use]
pub fn within_retention(record_secs: i64, retention_days: i64, now_secs: i64) -> bool {
    let window = retention_days.saturating_mul(86_400);
    now_secs.saturating_sub(record_secs) < window
}
