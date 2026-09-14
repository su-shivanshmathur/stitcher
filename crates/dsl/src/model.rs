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
    /// `"{log.payment_id}-{log.merchant_id}"`.
    pub primary_key: String,
    /// Record admission filter.
    #[serde(default)]
    pub decode_filter: DecodeFilter,
    /// State fields, keyed by name.
    pub fields: BTreeMap<String, Field>,
    /// Output sinks (ordered).
    #[serde(default)]
    pub sinks: Vec<Sink>,
}

/// Record admission filter.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeFilter {
    /// Required non-empty paths.
    #[serde(default)]
    pub require: Vec<String>,
    /// Reject records whose required string fields contain this substring.
    #[serde(default)]
    pub reject_if_contains: Option<String>,
    /// Allowed `log.log_type` values.
    #[serde(default)]
    pub log_type_in: Vec<String>,
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

/// One output sink.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sink {
    /// Sink name (diagnostics).
    pub name: String,
    /// Output topic.
    pub topic: String,
    /// State field feeding this sink.
    pub field: String,
    /// Map fields: one message per map entry.
    #[serde(default)]
    pub fan_out: bool,
    /// JSON path inside the field payload used as the Kafka record key.
    pub key_path: Option<String>,
    /// Retention in days (informational; the pipeline filters by topic config).
    pub retention_days: Option<i64>,
    /// Timestamp field for retention.
    pub retention_key: Option<String>,
}
