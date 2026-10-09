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

/// One outbound Kafka record + a delivery envelope (emitted as Kafka headers) for downstream
/// dedup/correlation of the ±1 deltas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutMsg {
    pub topic: String,
    pub key: Key,
    pub payload: Vec<u8>,
    /// Dedup id: stable hash of `(topic, key, causation_offset, sign)`. Stable across a same-batch
    /// replay (neutralizes the crash-before-commit phantom `+1`); see `causation_offset`.
    pub event_id: String,
    /// Shared by the retract and emit of one round: hash of `(topic, key, causation_offset)`.
    pub correlation_id: String,
    /// Schema version (constant `Processor::state_version`, not a per-record counter).
    pub state_version: i64,
    /// Projection wall-clock, epoch seconds.
    pub produced_at: i64,
    /// Greatest source offset feeding this key's state in the batch. Assumes one partition per key
    /// (offsets are per-partition; cross-partition routing breaks uniqueness).
    pub causation_offset: i64,
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
