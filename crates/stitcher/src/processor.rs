//! Core plugin traits (PLAN §Core abstractions): stateful `Processor` + stateless `Transformer`.

use serde::Serialize;

use crate::merge::Merge;

/// Composite grouping key (`"a-b-c-d"`), e.g. `payment_id-merchant_id-profile_id-org_id`.
pub type Key = String;

/// Delta sign stamped onto outgoing records (`sign_flag = ±1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sign {
    /// The merged (new) state.
    Plus,
    /// The previously stored (old) state, emitted when a key already existed.
    Minus,
}

impl Sign {
    /// Numeric `sign_flag` value in emitted JSON (`+1` / `-1`).
    #[must_use]
    pub const fn value(self) -> i64 {
        match self {
            Self::Plus => 1,
            Self::Minus => -1,
        }
    }
}

/// One outbound Kafka record produced by [`Processor::encode`] / [`Transformer::transform`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutMsg {
    /// Sink topic.
    pub topic: String,
    /// Kafka message key (drives partitioning; preserves per-key ordering).
    pub key: Key,
    /// Serialized payload (JSON).
    pub payload: Vec<u8>,
}

/// A stateful stream processor: the fill-in interface a user implements (PLAN §18).
/// All framework machinery (batching, store read-modify-write, delta emission, Kafka
/// I/O, commit, shutdown) lives in [`crate::pipeline::run`].
pub trait Processor: Send + Sync + 'static {
    /// Aggregation state monoid (was "Session"). Must round-trip stored state bytes
    /// via [`Self::decode_stored`] / `Serialize`.
    type State: Merge + Default + Serialize + Send + Sync;

    /// Stored-state schema version (e.g. `3`); mismatched versions are treated as
    /// absent. A method (not an associated const) so config-driven processors can
    /// read it from their schema.
    fn state_version(&self) -> i64;

    /// Cassandra `id_type` discriminator, e.g. `"payment_merchant_profile_organization"`.
    fn id_type(&self) -> &str;

    /// Decode previously stored state bytes (the version already matched); `None` ⇒
    /// treat as absent. Typed states parse with `serde_json`; config-driven
    /// `MergeValue` state is ambiguous without the schema (a number is a counter or
    /// a leaf), so those processors decode through their compiled program.
    fn decode_stored(&self, blob: &[u8]) -> Option<Self::State>;

    /// Extract the grouping key; `None` = filtered by design (== `_primaryKey`).
    fn primary_key(&self, raw: &[u8]) -> Option<Key>;

    /// Filter + extract the monoid for one record (== `_decodeLog` + `makeIntent`).
    /// `None` = filtered by design.
    fn decode(&self, raw: &[u8]) -> Option<Self::State>;

    /// Fan a state out into sink records, stamping `sign_flag` (== `_encodeForKafka`).
    /// Returning an empty vector gates production (== `isStateProper`).
    fn encode(&self, state: &Self::State, sign: Sign) -> Vec<OutMsg>;

    /// Decode + key in one pass. Default parses twice via [`Self::primary_key`] +
    /// [`Self::decode`]; hot paths should override to parse the record once.
    fn decode_with_key(&self, raw: &[u8]) -> Option<(Key, Self::State)> {
        Some((self.primary_key(raw)?, self.decode(raw)?))
    }
}

/// A stateless record → records transformer (PLAN §18).
pub trait Transformer: Send + Sync + 'static {
    /// Map one input record to zero or more sink records; empty = drop.
    fn transform(&self, raw: &[u8]) -> Vec<OutMsg>;
}
