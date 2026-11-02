//! YAML schema model.

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    /// Aggregate name (`snake_case`) → struct name in `UpperCamel`.
    pub aggregate: String,
    pub version: i64,
    pub id_type: String,
    /// Key template with `{log.path}` holes; `|` alternatives resolve first-present.
    pub primary_key: String,
    #[serde(default, rename = "accept")]
    pub decode_filter: DecodeFilter,
    #[serde(rename = "state")]
    pub fields: BTreeMap<String, Field>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeFilter {
    /// Required non-empty paths; `|` alternatives pass if any one is present.
    #[serde(default)]
    pub require: Vec<String>,
    /// Reject records whose required string fields contain this substring.
    #[serde(default)]
    pub reject_if_contains: Option<String>,
    /// Allowed event-type values (read from `event_type_path`).
    #[serde(default, rename = "event_type_in")]
    pub log_type_in: Vec<String>,
    /// Path the event type is read from (default `log_type`).
    #[serde(rename = "event_type_path")]
    pub log_type_path: Option<String>,
    /// Tenant id path (compared against processor `tenant_ids`; default `log.tenant_id`).
    pub tenant_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
pub enum Field {
    /// Keep the one payload with the maximum `comparator` (argmax by epoch nanos).
    LatestBy {
        when: Option<String>,
        /// Coerced to epoch nanos.
        comparator: String,
        /// A path or `$`; omitted ⇒ `$` (the whole record).
        #[serde(default)]
        payload: Option<String>,
    },
    /// Growing map: each key gets its own `latest_by` slot; absent key → insert (maps only grow).
    #[serde(rename = "grouped_map")]
    KeyedMap {
        when: Option<String>,
        /// Meaningful-only.
        key: String,
        value: Box<Self>,
    },
    /// Last value by arrival order (no comparator; overwrites on every admitted record).
    #[serde(rename = "last_seen")]
    Last { when: Option<String>, value: String },
    /// First meaningful value, sticky across restart/replay (write-once).
    #[serde(rename = "write_once")]
    Once { when: Option<String>, value: String },
    /// Sum counter; contributes +1 per admitted record.
    Counter { when: Option<String> },
    /// Nested mergeable struct.
    Nested { fields: BTreeMap<String, Self> },
}
