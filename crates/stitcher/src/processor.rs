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

/// The source record that caused a delta: the greatest `(topic, partition, offset)` for
/// the key's state in the batch — unique even across partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Causation {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// One sink record + envelope headers for downstream dedup/correlation.
#[derive(Clone, Debug)]
pub struct OutMsg {
    pub topic: String,
    pub key: Key,
    pub payload: Vec<u8>,
    /// FNV-1a-128 of `(stream, topic, aggregate_key, causation, row_id, payload content, sign)`.
    /// Content hashes ON TOP of the causation coordinate: genuine re-emits keep distinct ids,
    /// a same-batch replay reuses one. Anchored on the stable aggregate key.
    pub event_id: String,
    /// The retract + emit of one row share it (same parts minus `sign`, hashing the surviving
    /// content): the pair is ATOMIC downstream — drop both or neither, or a replay strands an
    /// orphan retract.
    pub correlation_id: String,
    pub state_version: i64,
    pub produced_at: i64,
    pub causation: Causation,
}

/// Builds + merges state per record; batching/commit/shutdown live in [`crate::pipeline::run`].
pub trait Processor: Send + Sync + 'static {
    /// Must round-trip via [`Self::decode_stored`] / `Serialize`.
    type State: Merge + Default + Serialize + Send + Sync;

    /// Mismatch ⇒ treated as absent.
    fn state_version(&self) -> i64;

    fn id_type(&self) -> &str;

    /// Version already matched; `None` ⇒ absent.
    fn decode_stored(&self, blob: &[u8]) -> Option<Self::State>;

    /// `None` = filtered by design.
    fn primary_key(&self, raw: &[u8]) -> Option<Key>;

    /// `None` = filtered by design.
    fn decode(&self, raw: &[u8]) -> Option<Self::State>;

    /// Override to parse the record once.
    fn decode_with_key(&self, raw: &[u8]) -> Option<(Key, Self::State)> {
        Some((self.primary_key(raw)?, self.decode(raw)?))
    }

    /// `None` (default) drops it.
    fn upcast(&self, _old_version: i64, _blob: &[u8]) -> Option<Self::State> {
        None
    }
}
