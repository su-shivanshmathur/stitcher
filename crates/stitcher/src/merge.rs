//! Mergeable state primitives + [`MergeValue`], the dynamic tree the config interpreter
//! runs on. `self` is the older value throughout.

use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_json::Value;

/// Associative merge; `self` is the older value.
pub trait Merge {
    #[must_use]
    fn merge(self, newer: Self) -> Self;
}

/// Larger comparator wins; a tie keeps the newer. Serialized `{"comparator": …, "payload": …}`
/// to byte-match stored v3 state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestBy<C: Ord, A> {
    pub comparator: C,
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
    /// Identity value (omitted from serialized state).
    #[must_use]
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Union of two maps, merging values on key collision; maps only grow.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyedMap<K: Eq + Hash, V>(pub HashMap<K, V>);

impl<K: Eq + Hash, V: Merge> Merge for KeyedMap<K, V> {
    fn merge(mut self, newer: Self) -> Self {
        for (entry_key, entry_value) in newer.0 {
            // reuse the LHS allocation via remove + insert
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
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Last-write-wins.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LastWrite<T>(pub Option<T>);

impl<T> Merge for LastWrite<T> {
    fn merge(self, newer: Self) -> Self {
        Self(newer.0.or(self.0))
    }
}

impl<T> LastWrite<T> {
    #[must_use]
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

/// Write-once: first `Some` wins — persist-before-commit makes it stable across replay.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FirstWrite<T>(pub Option<T>);

impl<T> Merge for FirstWrite<T> {
    fn merge(self, newer: Self) -> Self {
        Self(self.0.or(newer.0)) // first-write wins
    }
}

impl<T> FirstWrite<T> {
    #[must_use]
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

/// Sum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Counter(pub i64);

impl Merge for Counter {
    fn merge(self, newer: Self) -> Self {
        Self(self.0.saturating_add(newer.0))
    }
}

impl Counter {
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

/// Dynamic state tree; mismatched variants on merge (schema drift) keep the newer.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum MergeValue {
    /// The merge identity.
    #[default]
    Null,
    LatestBy {
        comparator: i64,
        payload: Value,
    },
    /// `BTreeMap` for deterministic serialization order.
    Map(BTreeMap<String, Self>),
    /// Last-write-wins.
    Leaf(Value),
    /// First-write wins (merge direction distinguishes it from [`MergeValue::Leaf`]).
    Once(Value),
    Counter(i64),
}

impl MergeValue {
    /// Identity ⇒ omitted from the serialized state.
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
                    // reuse the LHS allocation via remove + insert
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
    /// Identity entries omitted at every map level (round-trip stable); `LatestBy` renders
    /// `{"comparator": …, "payload": …}`; an empty state renders `{}`.
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
