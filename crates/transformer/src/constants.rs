//! Crate-wide constants, centralized so tunable values live in one module rather than scattered
//! across files.

// --- retention ---

/// Milliseconds per second; used to convert a millis timestamp to seconds for retention.
pub(crate) const MILLIS_PER_SECOND: i64 = 1_000;
