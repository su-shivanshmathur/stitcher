//! The stateful `Processor` trait: build state. Output is a [`crate::projection::Projection`].

use serde::Serialize;

use crate::merge::Merge;

/// Composite grouping key, e.g. `payment_id-merchant_id`.
pub type Key = String;

/// Delta sign stamped onto emitted records (`sign_flag = ±1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sign {
    /// The merged (new) state.
    Plus,
    /// The previously stored (old) state.
    Minus,
}

impl Sign {
    /// `+1` / `-1`.
    #[must_use]
    pub const fn value(self) -> i64 {
        match self {
            Self::Plus => 1,
            Self::Minus => -1,
        }
    }

    /// `"+1"` / `"-1"` label (e.g. for envelope ids).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Plus => "+1",
            Self::Minus => "-1",
        }
    }
}

/// The source record that caused a delta: the greatest `(topic, partition, offset)` coordinate (by
/// that total order) feeding the key's state in the batch. The full coordinate — not a bare offset —
/// is globally unique even if a key's records span partitions/topics (offsets are per-partition).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Causation {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// One outbound Kafka record + a delivery envelope (emitted as Kafka headers) for downstream
/// dedup/correlation of the ±1 deltas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutMsg {
    pub topic: String,
    pub key: Key,
    pub payload: Vec<u8>,
    /// Dedup id: stable hash of `(stream, topic, aggregate_key, causation(topic/partition/offset),
    /// row_id, sign)` — the stable aggregate key (not the mutable projected key) anchors identity;
    /// `stream` + `row_id` keep same-topic streams and explode rows distinct; the full causation
    /// coordinate keeps cross-partition records distinct. Stable across a same-batch replay.
    pub event_id: String,
    /// Shared by the retract and emit of one row: the same hash minus `sign`. A `-1`/`+1` pair with
    /// one `correlation_id` is ATOMIC downstream — if the `+1` is dropped by `event_id` dedup (a
    /// same-batch replay), the `-1` MUST be dropped too, or a first-batch replay strands an orphan
    /// retract (at-least-once; see FINDINGS #3).
    pub correlation_id: String,
    /// Schema version (constant `Processor::state_version`, not a per-record counter).
    pub state_version: i64,
    /// Projection wall-clock, epoch seconds.
    pub produced_at: i64,
    /// The source coordinate that caused this delta (see [`Causation`]).
    pub causation: Causation,
}

/// Builds + merges state per record. Batching, store read-modify-write, commit and
/// shutdown live in [`crate::pipeline::run`]; output is a [`crate::projection::Projection`].
pub trait Processor: Send + Sync + 'static {
    /// Aggregation state monoid; must round-trip via [`Self::decode_stored`] / `Serialize`.
    type State: Merge + Default + Serialize + Send + Sync;

    /// Stored-state schema version; a mismatch is treated as absent.
    fn state_version(&self) -> i64;

    /// Store `id_type` discriminator.
    fn id_type(&self) -> &str;

    /// Decode stored state bytes (version already matched); `None` ⇒ absent.
    fn decode_stored(&self, blob: &[u8]) -> Option<Self::State>;

    /// Extract the grouping key; `None` = filtered by design.
    fn primary_key(&self, raw: &[u8]) -> Option<Key>;

    /// Filter + build the state monoid for one record; `None` = filtered by design.
    fn decode(&self, raw: &[u8]) -> Option<Self::State>;

    /// Decode + key in one pass (override to parse the record once).
    fn decode_with_key(&self, raw: &[u8]) -> Option<(Key, Self::State)> {
        Some((self.primary_key(raw)?, self.decode(raw)?))
    }

    /// Migrate a state stored under an older `version` to the current `State`; `None` (default) drops it.
    fn upcast(&self, _old_version: i64, _blob: &[u8]) -> Option<Self::State> {
        None
    }
}
