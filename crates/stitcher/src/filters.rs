//! Retention filtering (PLAN §Payment-intent semantics): keep a record iff
//! `now_s − record_s < retention_days × 86400`.

use std::collections::HashMap;

use crate::{config::Filter, json_util, processor::OutMsg};

/// Raw check on epoch-seconds timestamps.
#[must_use]
pub fn within_retention(record_secs: i64, retention_days: i64, now_secs: i64) -> bool {
    let window = retention_days.saturating_mul(86_400);
    now_secs.saturating_sub(record_secs) < window
}

/// Per-sink-topic retention rules, resolved once per run (not per batch).
#[derive(Debug, Default, Clone)]
pub struct RetentionTable(HashMap<String, (i64, String)>);

impl RetentionTable {
    /// Build from the `[filters.<name>]` config sections (keyed by sink topic).
    #[must_use]
    pub fn from_filters(filters: &HashMap<String, Filter>) -> Self {
        Self(
            filters
                .values()
                .map(|f| (f.topic.clone(), (f.retention_days, f.retention_key.clone())))
                .collect(),
        )
    }

    /// Retention gate for one outbound message. Topics without a rule, unparseable
    /// payloads and missing keys are RETAINED (no silent data loss).
    #[must_use]
    pub fn keep(&self, msg: &OutMsg, now_secs: i64) -> bool {
        let Some((days, key)) = self.0.get(&msg.topic) else {
            return true;
        };
        let Ok(json) = serde_json::from_slice::<serde_json::Value>(&msg.payload) else {
            return true;
        };
        let Some(secs) = json_util::get_path(&json, key).and_then(json_util::as_i64) else {
            return true;
        };
        within_retention(secs, *days, now_secs)
    }
}
