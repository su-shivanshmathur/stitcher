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
}

/// One outbound Kafka record produced by a [`crate::projection::Projection`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutMsg {
    pub topic: String,
    pub key: Key,
    pub payload: Vec<u8>,
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
