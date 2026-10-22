//! Crate-wide constants, centralized so tunable values live in one module rather than scattered
//! across files. The `metrics.rs` `LazyLock` registry/handles are runtime-initialized globals,
//! not value constants, so they stay with the metrics subsystem.

use std::time::Duration;

// --- kafka consumer ---

/// Offset-commit retry attempts before the error surfaces.
pub(crate) const COMMIT_RETRIES: usize = 3;

/// Resume-poll cadence after a revoke drain. Cold path, so the value only bounds resume latency —
/// not correctness or throughput — hence a constant, not a config knob.
pub(crate) const RESUME_POLL_INTERVAL: Duration = Duration::from_millis(50);

// --- pipeline ---

/// Consecutive transport (consume) failures tolerated before the pipeline stops.
pub(crate) const MAX_TRANSPORT_FAILURES: u32 = 6;

// --- store (rocksdb) ---

/// Column-family name prefix; the per-`id_type` state CF is `state.<id_type>`.
pub(crate) const STATE_COLUMN_FAMILY_PREFIX: &str = "state.";

/// Entries per RocksDB write batch.
pub(crate) const ROCKS_WRITE_CHUNK_SIZE: usize = 1024;

// --- store (cql) ---

/// Cap on the per-pod write-token map backing cross-writer detection; cleared wholesale past this
/// size so it never grows unbounded (a brief detection gap after a clear is acceptable).
pub(crate) const WRITE_TOKEN_MAP_CAP: usize = 100_000;

// --- inspect ---

/// Tracing target for the inspect renderer; matches the module path (used in `RUST_LOG`).
pub const INSPECT_TARGET: &str = "stitcher::inspect";

/// Placeholder written over PII fields in inspect output.
pub(crate) const REDACTION_MASK: &str = "***";

/// Deterministic-sampling hash → f64 in [0,1): right-shift keeps the top 52 bits of the u64 hash,
/// OR-ed into [`HASH_F64_ONE_BITS`] to land in [1,2), then subtract 1.0.
pub(crate) const HASH_F64_SHIFT: u32 = u64::BITS - f64::MANTISSA_DIGITS;

/// Exponent field (bit pattern of `1.0`) for floats in [1,2); see [`HASH_F64_SHIFT`].
pub(crate) const HASH_F64_ONE_BITS: u64 = f64::to_bits(1.0);

// --- builtins ---

/// Nanoseconds per second (time parsing works in epoch-nanosecond units).
pub(crate) const NANOS_PER_SEC: i64 = 1_000_000_000;
