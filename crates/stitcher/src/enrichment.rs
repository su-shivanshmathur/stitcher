//! Enrichment join-map: `ArcSwap`-held JSON, reloaded in a background task (PLAN §14,
//! `--config-file` / `--sleep-in-sec`).

use std::sync::Arc;

use arc_swap::ArcSwap;
use error_stack::ResultExt;

use crate::{
    config,
    errors::{StitcherError, StitcherResult},
    metrics,
};

/// Shared, hot-swappable join-space. Empty `Value::Null` when no file is configured.
#[derive(Clone)]
pub struct Enrichment {
    state: Arc<ArcSwap<serde_json::Value>>,
    /// Tables captured by the single boot read — the set transformer `lookup`s are validated
    /// against and every reload must retain. Empty when no file is configured.
    boot_tables: Vec<String>,
}

/// Why a hot-reload was rejected (the previous map is kept).
enum ReloadRejection {
    /// The reloaded value is not a JSON object.
    NotObject,
    /// The reload dropped a table present at boot.
    MissingBootTable,
    /// A table's value is not a JSON object (so `lookup` would resolve to null).
    NonObjectTable,
}

impl ReloadRejection {
    fn reason(self) -> &'static str {
        match self {
            Self::NotObject => "not_object",
            Self::MissingBootTable => "missing_boot_table",
            Self::NonObjectTable => "non_object_table",
        }
    }
}

/// Why a join-map failed the shape contract.
enum ShapeError {
    /// The root value is not a JSON object.
    RootNotObject,
    /// A table's value is not a JSON object (a string-keyed `lookup` can never resolve in an array or scalar).
    TableNotObject(String),
}

impl std::fmt::Display for ShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RootNotObject => write!(f, "the join-map root must be a JSON object"),
            Self::TableNotObject(name) => {
                write!(f, "table {name:?} must be a JSON object")
            }
        }
    }
}

impl Enrichment {
    /// Parse join-map bytes as JSON (shared by the sync boot/CLI read and the async reload read).
    fn parse_file(path: &str, bytes: &[u8]) -> StitcherResult<serde_json::Value> {
        serde_json::from_slice(bytes)
            .change_context(StitcherError::Enrichment(format!("parse {path} as JSON")))
    }

    /// Synchronous load for the boot path and the `check-transformer` CLI (neither is latency
    /// sensitive, and the CLI has no runtime to offload onto).
    fn load_file(path: &str) -> StitcherResult<serde_json::Value> {
        let bytes = std::fs::read(path)
            .change_context(StitcherError::Enrichment(format!("read {path}")))?;
        Self::parse_file(path, &bytes)
    }

    /// Async load for the background reloader: `tokio::fs` so the periodic read never blocks a
    /// runtime worker (`std::fs::read` would, even at the 12h cadence).
    async fn load_file_async(path: &str) -> StitcherResult<serde_json::Value> {
        let bytes = tokio::fs::read(path)
            .await
            .change_context(StitcherError::Enrichment(format!("read {path}")))?;
        Self::parse_file(path, &bytes)
    }

    /// The shape contract shared by boot and reload: the root must be a JSON object AND every
    /// table value must be an object (so a string-keyed `lookup` can resolve). Returns the
    /// table names.
    fn table_names_of(map: &serde_json::Value) -> Result<Vec<String>, ShapeError> {
        let tables = map.as_object().ok_or(ShapeError::RootNotObject)?;
        tables
            .iter()
            .find_map(|(name, value)| {
                (!value.is_object()).then(|| ShapeError::TableNotObject(name.clone()))
            })
            .map_or_else(|| Ok(tables.keys().cloned().collect()), Err)
    }

    /// Load the map once, then keep it fresh in the background. A missing/blank
    /// `config_file` disables it; reload failures keep the previous map (stale beats absent).
    pub fn spawn_reloader(cfg: &config::Enrichment) -> StitcherResult<Self> {
        if cfg.config_file.is_empty() {
            return Ok(Self {
                state: Arc::new(ArcSwap::from(Arc::new(serde_json::Value::Null))),
                boot_tables: Vec::new(),
            });
        }
        let initial = Self::load_file(&cfg.config_file)?;
        // The boot contract IS the reload contract: the shape is enforced HERE too (not only on
        // reloads), so startup can't install a map whose lookups would silently resolve to null
        // — and a map accepted at boot can never be rejected for shape by an identical reload.
        // The tables present at boot are the validated contract (transformer `lookup`s were
        // checked against them); a reload must not drop any.
        let boot_tables = Self::table_names_of(&initial).map_err(|e| {
            error_stack::report!(StitcherError::Enrichment(format!(
                "enrichment file {}: {e}",
                cfg.config_file
            )))
        })?;
        let required: std::collections::BTreeSet<String> = boot_tables.iter().cloned().collect();
        let this = Self {
            state: Arc::new(ArcSwap::from(Arc::new(initial))),
            boot_tables,
        };
        if cfg.reload_secs > 0 {
            let state = Arc::clone(&this.state);
            let path = cfg.config_file.clone();
            let period = std::time::Duration::from_secs(cfg.reload_secs);
            tokio::spawn(async move {
                // Start one period out: the boot read above already loaded the map, so a plain
                // `interval` (first tick fires immediately) would redundantly reload it at t=0.
                let start = tokio::time::Instant::now() + period;
                let mut tick = tokio::time::interval_at(start, period);
                loop {
                    tick.tick().await;
                    match Self::load_file_async(&path).await {
                        Ok(fresh) => match Self::check_reload(&fresh, &required) {
                            Ok(()) => {
                                state.store(Arc::new(fresh));
                                tracing::info!(path, "enrichment map reloaded");
                            }
                            Err(rejection) => {
                                metrics::enrichment_reload_rejected();
                                tracing::warn!(
                                    path,
                                    reason = rejection.reason(),
                                    "enrichment reload rejected; keeping previous map"
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(path, error = ?e, "enrichment reload failed; keeping previous map");
                        }
                    }
                }
            });
        }
        Ok(this)
    }

    /// A reload is accepted only if it keeps the shape contract — root and EVERY table value
    /// objects, including newly added tables — AND retains every boot table (so a validated
    /// `lookup` keeps resolving).
    fn check_reload(
        fresh: &serde_json::Value,
        required: &std::collections::BTreeSet<String>,
    ) -> Result<(), ReloadRejection> {
        match Self::table_names_of(fresh) {
            Err(ShapeError::RootNotObject) => Err(ReloadRejection::NotObject),
            Err(ShapeError::TableNotObject(_)) => Err(ReloadRejection::NonObjectTable),
            Ok(names) => {
                let present: std::collections::BTreeSet<String> = names.into_iter().collect();
                if present.is_superset(required) {
                    Ok(())
                } else {
                    Err(ReloadRejection::MissingBootTable)
                }
            }
        }
    }

    /// Top-level table names in the configured join-map, for validating transformer
    /// `lookup(table, …)` references in `check-transformer` (CI — the boot path uses the
    /// instance's boot set). `None` ⇒ no enrichment file configured (skip the check);
    /// `Some` (possibly empty) ⇒ the configured table set. The shape contract (root + every
    /// table value objects) is enforced here so CI catches a map whose lookups could only
    /// ever resolve to null.
    pub fn table_names(cfg: &config::Enrichment) -> StitcherResult<Option<Vec<String>>> {
        if cfg.config_file.is_empty() {
            return Ok(None);
        }
        let map = Self::load_file(&cfg.config_file)?;
        Self::table_names_of(&map).map(Some).map_err(|e| {
            error_stack::report!(StitcherError::Enrichment(format!(
                "enrichment file {}: {e}",
                cfg.config_file
            )))
        })
    }

    /// The boot table set, captured from the single construction-time read that seeded the
    /// runtime's map: transformer validation checks `lookup` references against it, and
    /// reloads must retain it. Empty when no file is configured (a known-empty set — every
    /// `lookup` fails validation).
    #[must_use]
    pub fn boot_tables(&self) -> &[String] {
        &self.boot_tables
    }

    /// Read the whole join-map (immutable snapshot).
    #[must_use]
    pub fn snapshot(&self) -> Arc<serde_json::Value> {
        self.state.load_full()
    }
}
