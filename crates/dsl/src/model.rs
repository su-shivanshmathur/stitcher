//! YAML schema model.

use std::collections::BTreeMap;

use serde::Deserialize;

/// Top-level schema document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    /// Aggregate name (`snake_case`) → struct name in `UpperCamel`.
    pub aggregate: String,
    /// Stored-state version.
    pub version: i64,
    pub id_type: String,
    /// Key template with `{log.path}` holes; a hole may carry `|` alternatives
    /// (`{log.a|log.b}`) that resolve to the first present value.
    pub primary_key: String,
    #[serde(default)]
    pub decode_filter: DecodeFilter,
    pub fields: BTreeMap<String, Field>,
}

/// Record admission filter.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeFilter {
    /// Required non-empty paths; `|` alternatives pass if any one is present.
    #[serde(default)]
    pub require: Vec<String>,
    /// Reject records whose required string fields contain this substring.
    #[serde(default)]
    pub reject_if_contains: Option<String>,
    /// Allowed `log_type` values (read from `log_type_path`).
    #[serde(default)]
    pub log_type_in: Vec<String>,
    /// Path `log_type` is read from (default `log_type`).
    pub log_type_path: Option<String>,
    /// Tenant id path (compared against processor `tenant_ids`; default `log.tenant_id`).
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
        /// A path or `$`; omitted ⇒ `$` (the whole record).
        #[serde(default)]
        payload: Option<String>,
    },
    /// Map with per-key merged values; absent key → insert (maps only grow).
    KeyedMap {
        /// Predicate gating entry creation.
        when: Option<String>,
        /// Key expression (meaningful-only).
        key: String,
        value: Box<Self>,
    },
    /// Keep the rightmost meaningful value.
    Last {
        /// Predicate gating extraction.
        when: Option<String>,
        value: String,
    },
    /// Write-once: keeps the first meaningful value; persisted, so stable across restarts.
    Once {
        /// Predicate gating extraction.
        when: Option<String>,
        value: String,
    },
    /// `Sum` counter; contributes 1 per admitted record.
    Counter {
        /// Optional gating predicate.
        when: Option<String>,
    },
    /// Nested mergeable struct.
    Nested { fields: BTreeMap<String, Self> },
}
