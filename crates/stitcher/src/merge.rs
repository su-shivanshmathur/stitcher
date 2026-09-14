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
        for (k, v) in newer.0 {
            // Reuse the LHS allocation: remove + insert instead of building a fresh map.
            let combined = match self.0.remove(&k) {
                Some(old) => old.merge(v),
                None => v,
            };
            self.0.insert(k, combined);
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
            (Some(a), Some(b)) => Some(a.merge(b)),
            (a, b) => a.or(b),
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
            Self::Leaf(_) => false,
        }
    }

    /// Serialize with NO identity-skip: every map entry rendered, like the typed
    /// processors' field values (their skip is field-level only, and callers of this
    /// gate on identity first). Encode payloads use this; stored state uses `Serialize`.
    #[must_use]
    pub fn to_value_full(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::LatestBy {
                comparator,
                payload,
            } => {
                serde_json::json!({ "comparator": comparator, "payload": payload })
            }
            Self::Map(m) => Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), v.to_value_full()))
                    .collect(),
            ),
            Self::Leaf(v) => v.clone(),
            Self::Counter(n) => Value::from(*n),
        }
    }
}

impl Merge for MergeValue {
    fn merge(self, newer: Self) -> Self {
        match (self, newer) {
            (Self::Null, n) | (n, Self::Null) => n,
            (
                Self::LatestBy {
                    comparator: c1,
                    payload: p1,
                },
                Self::LatestBy {
                    comparator: c2,
                    payload: p2,
                },
            ) => {
                if c1 > c2 {
                    Self::LatestBy {
                        comparator: c1,
                        payload: p1,
                    }
                } else {
                    Self::LatestBy {
                        comparator: c2,
                        payload: p2,
                    }
                }
            }
            (Self::Map(mut old), Self::Map(new)) => {
                for (k, v) in new {
                    // Reuse the LHS allocation: remove + insert instead of building a fresh map.
                    let combined = match old.remove(&k) {
                        Some(prev) => prev.merge(v),
                        None => v,
                    };
                    old.insert(k, combined);
                }
                Self::Map(old)
            }
            (Self::Leaf(_), n @ Self::Leaf(_)) => n,
            (Self::Counter(a), Self::Counter(b)) => Self::Counter(a.saturating_add(b)),
            // variant mismatch (schema drift): newer wins
            (_, n) => n,
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
            Self::Map(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    if !v.is_identity() {
                        map.serialize_entry(k, v)?;
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
            Self::Leaf(v) => v.serialize(serializer),
            Self::Counter(n) => serializer.serialize_i64(*n),
        }
    }
}
