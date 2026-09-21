//! YAML schema model (PLAN §Schema shape).

use std::collections::BTreeMap;

use serde::Deserialize;

/// Top-level schema document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    /// Aggregate name (`snake_case`) → struct name in `UpperCamel`.
    pub aggregate: String,
    /// Stored-state version (e.g. 3).
    pub version: i64,
    /// Store `id_type` discriminator.
    pub id_type: String,
    /// Key template with `{log.path}` segments, e.g.
    /// `"{log.payment_id}-{log.merchant_id}"`. A hole may carry `|`
    /// alternatives (`{log.payment_id|log.payment_intent_id}`) that resolve to
    /// the first present value — for identity components that live at
    /// different paths across event types.
    pub primary_key: String,
    /// Record admission filter.
    #[serde(default)]
    pub decode_filter: DecodeFilter,
    /// State fields, keyed by name.
    pub fields: BTreeMap<String, Field>,
}

/// Record admission filter.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeFilter {
    /// Required non-empty paths; an entry may carry `|` alternatives
    /// (`"log.payment_id|log.payment_intent_id"`) of which any one present
    /// satisfies the requirement.
    #[serde(default)]
    pub require: Vec<String>,
    /// Reject records whose required string fields contain this substring.
    #[serde(default)]
    pub reject_if_contains: Option<String>,
    /// Allowed `log_type` values (read from `log_type_path`).
    #[serde(default)]
    pub log_type_in: Vec<String>,
    /// Path the `log_type` value is read from (default `log_type` — the
    /// producer's top-level discriminator sits next to the `log` payload).
    pub log_type_path: Option<String>,
    /// Tenant id path (compared against the processor's `tenant_ids`; default
    /// `log.tenant_id`).
    pub tenant_path: Option<String>,
}

/// State field node.
#[derive(Debug, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case", deny_unknown_fields)]
pub enum Field {
    /// Keep payload with the max comparator.
    LatestBy {
        /// Predicate gating this field's extraction.
        when: Option<String>,
        /// Comparator expression (coerced to epoch nanos).
        comparator: String,
        /// Payload expression.
        payload: String,
    },
    /// Map with per-key merged values; absent key → insert (maps only grow).
    KeyedMap {
        /// Predicate gating entry creation.
        when: Option<String>,
        /// Key expression (meaningful-only).
        key: String,
        /// Value node.
        value: Box<Self>,
    },
    /// Keep the rightmost meaningful value (`Last`).
    Last {
        /// Predicate gating extraction.
        when: Option<String>,
        /// Value expression.
        value: String,
    },
    /// Keep the FIRST meaningful value ever seen (`Once` — write-once / sticky):
    /// once assigned it never changes. Persisted, so the assignment is stable
    /// across restarts (e.g. an A/B variant stamped onto every later event).
    Once {
        /// Predicate gating extraction.
        when: Option<String>,
        /// Value expression (the value to freeze on first sight).
        value: String,
    },
    /// `Sum` counter; contributes 1 per admitted record.
    Counter {
        /// Optional gating predicate.
        when: Option<String>,
    },
    /// Nested mergeable struct.
    Nested {
        /// Sub-fields.
        fields: BTreeMap<String, Self>,
    },
}

