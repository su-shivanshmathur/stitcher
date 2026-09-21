//! Mergeable state primitives (`LatestBy`, `OmmitableHashMap`, `Last`, `Sum`)
//! (PLAN §12). `old.merge(new)` throughout: `self` is the older value.
//! [`MergeValue`] is the dynamic, schema-agnostic tree the config-driven interpreter
//! runs on (lantern #37).

use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_json::Value;

/// Associative merge of two states; `self` is the older value, `newer` the incoming one
/// (associativity; right identity with `Default`).
pub trait Merge {
    /// Merge `self` (old) with `newer` (incoming), returning the merged state.
    #[must_use]
    fn merge(self, newer: Self) -> Self;
}

/// Keep the value with the strictly larger comparator; a **tie keeps the newer** value.
/// Serialized as `{"comparator": …, "payload": …}` to byte-match stored v3 state.
/// `Default` is epoch 0 for time comparators.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestBy<C: Ord, A> {
    /// Merge key; larger wins (ties keep the newer value).
    pub comparator: C,
    /// Carried data.
    pub payload: A,
}

impl<C: Ord, A> Merge for LatestBy<C, A> {
    fn merge(self, newer: Self) -> Self {
        if self.comparator > newer.comparator {
            self
        } else {
            newer
        }
    }
}

impl<C: Ord + Default, A: Default> Default for LatestBy<C, A> {
    fn default() -> Self {
        Self {
            comparator: C::default(),
            payload: A::default(),
        }
    }
}

impl<C: Ord + Default + PartialEq, A: Default + PartialEq> LatestBy<C, A> {
    /// True when this equals the identity value (used for `omitNothingFields`-style serde).
    #[must_use]
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Union of two maps, merging values on key collision (`unionWith`). Deletion/eviction is
/// NOT part of the port (maps only grow). Serialized as a plain JSON map.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyedMap<K: Eq + Hash, V>(pub HashMap<K, V>);

impl<K: Eq + Hash, V: Merge> Merge for KeyedMap<K, V> {
    fn merge(mut self, newer: Self) -> Self {
        for (entry_key, entry_value) in newer.0 {
            // Reuse the LHS allocation: remove + insert instead of building a fresh map.
            let combined = match self.0.remove(&entry_key) {
                Some(old) => old.merge(entry_value),
                None => entry_value,
            };
            self.0.insert(entry_key, combined);
        }
        self
    }
}

impl<K: Eq + Hash, V> KeyedMap<K, V> {
    /// True when the map is empty (used for `omitNothingFields`-style serde).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Keep the rightmost `Some` (== `Data.Monoid.Last`). Serialized transparently.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LastWrite<T>(pub Option<T>);

impl<T> Merge for LastWrite<T> {
    fn merge(self, newer: Self) -> Self {
        Self(newer.0.or(self.0))
    }
}

impl<T> LastWrite<T> {
    /// True when `None` (used for `omitNothingFields`-style serde).
    #[must_use]
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

/// Keep the **leftmost** (first-written) `Some` — a write-once / sticky field:
/// once assigned it never changes. The dual of [`LastWrite`]. Because the
/// pipeline persists before committing offsets, the first assignment is stable
/// across restarts and replay. Serialized transparently.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FirstWrite<T>(pub Option<T>);

impl<T> Merge for FirstWrite<T> {
    fn merge(self, newer: Self) -> Self {
        Self(self.0.or(newer.0)) // keep the older value if present (first-write wins)
    }
}

impl<T> FirstWrite<T> {
    /// True when unset (used for `omitNothingFields`-style serde).
    #[must_use]
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

/// Sum two counts (== `Data.Monoid.Sum Int`). Serialized transparently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Counter(pub i64);

impl Merge for Counter {
    fn merge(self, newer: Self) -> Self {
        Self(self.0.saturating_add(newer.0))
    }
}

impl Counter {
    /// True when zero (used for `omitNothingFields`-style serde).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl<T: Merge> Merge for Option<T> {
    fn merge(self, newer: Self) -> Self {
        match (self, newer) {
            (Some(old), Some(new)) => Some(old.merge(new)),
            (old, new) => old.or(new),
        }
    }
}

/// Dynamic state tree: one variant per schema node kind, mirroring the typed primitives
/// exactly (`self` = old, `newer` = incoming; lantern #37):
/// - [`MergeValue::Null`] is the identity (a `last` node's absent value, or no state).
/// - [`MergeValue::LatestBy`] keeps the strictly larger comparator; ties keep the newer.
/// - [`MergeValue::Map`] is `unionWith` (object and `keyed_map` nodes unify: identical
///   merge semantics, `BTreeMap` for deterministic serialization order — typed maps are
///   `HashMap`s with unspecified order; emitted message *sets* are equal either way).
/// - [`MergeValue::Leaf`] is last-write-wins (a `last` node's present value).
/// - [`MergeValue::Counter`] saturating-adds.
///
/// Mismatched variants (schema drift between stored and live config) keep the newer —
/// impossible in the typed world, where the schema pins one variant per field.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum MergeValue {
    /// No state (== `LastWrite(None)` / an all-identity field map).
    #[default]
    Null,
    /// `latest_by` node (== [`LatestBy`]).
    LatestBy {
        /// Merge key; larger wins (ties keep the newer value).
        comparator: i64,
        /// Carried data.
        payload: Value,
    },
    /// `keyed_map` / object node (== [`KeyedMap`]).
    Map(BTreeMap<String, Self>),
    /// `last` node's present value (== `LastWrite(Some)`).
    Leaf(Value),
    /// `once` node's present value (== [`FirstWrite`]`(Some)`): first-write wins,
    /// so merging keeps the older side. Distinct from [`MergeValue::Leaf`]
    /// (last-write) precisely in its merge direction.
    Once(Value),
    /// `counter` node (== [`Counter`]).
    Counter(i64),
}

impl MergeValue {
    /// True when this is the merge identity for its node kind (a field carrying only
    /// this is omitted from the serialized state, matching the typed processors'
    /// `skip_serializing_if` attributes).
    #[must_use]
    pub fn is_identity(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Counter(n) => *n == 0,
            Self::Map(m) => m.is_empty(),
            Self::LatestBy {
                comparator,
                payload,
            } => *comparator == 0 && payload.is_null(),
            Self::Leaf(_) | Self::Once(_) => false,
        }
    }

}

impl Merge for MergeValue {
    fn merge(self, newer: Self) -> Self {
        match (self, newer) {
            (Self::Null, other) | (other, Self::Null) => other,
            (
                Self::LatestBy {
                    comparator: old_comparator,
                    payload: old_payload,
                },
                Self::LatestBy {
                    comparator: new_comparator,
                    payload: new_payload,
                },
            ) => {
                if old_comparator > new_comparator {
                    Self::LatestBy {
                        comparator: old_comparator,
                        payload: old_payload,
                    }
                } else {
                    Self::LatestBy {
                        comparator: new_comparator,
                        payload: new_payload,
                    }
                }
            }
            (Self::Map(mut old_entries), Self::Map(new_entries)) => {
                for (entry_key, entry_value) in new_entries {
                    // Reuse the LHS allocation: remove + insert instead of building a fresh map.
                    let merged_entry = match old_entries.remove(&entry_key) {
                        Some(existing) => existing.merge(entry_value),
                        None => entry_value,
                    };
                    old_entries.insert(entry_key, merged_entry);
                }
                Self::Map(old_entries)
            }
            (Self::Leaf(_), newer @ Self::Leaf(_)) => newer,
            // first-write wins: keep the older (left) value
            (old @ Self::Once(_), Self::Once(_)) => old,
            (Self::Counter(old_count), Self::Counter(new_count)) => {
                Self::Counter(old_count.saturating_add(new_count))
            }
            // variant mismatch (schema drift): newer wins
            (_, newer) => newer,
        }
    }
}

impl Serialize for MergeValue {
    /// Config-free canonical form, shape-compatible with the typed processors: a field
    /// map omits identity entries (absent on the wire ⇔ identity on decode, so the
    /// round trip is stable), `LatestBy` renders `{"comparator": …, "payload": …}`,
    /// and an empty state renders `{}` like an all-default typed struct.
    ///
    /// Typed processors skip identities only at the field level; this impl skips at
    /// every map level. The two differ solely for an identity `LatestBy` nested in a
    /// `keyed_map` — impossible while admitted records yield a non-null payload for
    /// that node, and round-trip-stable regardless (absent ⇔ identity).
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => {
                let map = serializer.serialize_map(Some(0))?;
                map.end()
            }
            Self::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (entry_key, entry_value) in entries {
                    if !entry_value.is_identity() {
                        map.serialize_entry(entry_key, entry_value)?;
                    }
                }
                map.end()
            }
            Self::LatestBy {
                comparator,
                payload,
            } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("comparator", comparator)?;
                map.serialize_entry("payload", payload)?;
                map.end()
            }
            Self::Leaf(v) | Self::Once(v) => v.serialize(serializer),
            Self::Counter(n) => serializer.serialize_i64(*n),
        }
    }
}

