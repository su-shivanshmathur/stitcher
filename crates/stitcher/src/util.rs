//! Small shared helpers.

/// Current unix time, seconds (saturating: pre-1970 clocks → 0).
#[must_use]
pub fn now_secs() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
    )
    .unwrap_or(i64::MAX)
}

/// Current unix time, nanoseconds.
#[must_use]
pub fn now_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}
