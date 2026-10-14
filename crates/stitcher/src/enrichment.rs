//! Enrichment join-map: `ArcSwap`-held JSON, reloaded in a background task.

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
    /// against and every reload must retain.
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

    /// Synchronous load for the boot path and the `check-transformer` CLI.
    fn load_file(path: &str) -> StitcherResult<serde_json::Value> {
        let bytes = std::fs::read(path)
            .change_context(StitcherError::Enrichment(format!("read {path}")))?;
        Self::parse_file(path, &bytes)
    }

    /// Async load for the reloader (`tokio::fs`, so the periodic read never blocks a worker).
    async fn load_file_async(path: &str) -> StitcherResult<serde_json::Value> {
        let bytes = tokio::fs::read(path)
            .await
            .change_context(StitcherError::Enrichment(format!("read {path}")))?;
        Self::parse_file(path, &bytes)
    }

    /// The shape contract shared by boot and reload: the root and every table value must be
    /// objects (so a string-keyed `lookup` can resolve). Returns the table names.
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
        // The boot contract IS the reload contract: shape is enforced here too, and the
        // tables present at boot are what validated `lookup`s must keep resolving.
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
                // start one period out: the boot read above already loaded the map
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

    /// A reload is accepted only if it keeps the shape contract AND retains every boot table.
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

    /// Top-level table names for validating transformer `lookup` references in
    /// `check-transformer` (CI). `None` ⇒ no enrichment file configured (skip the check).
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

    /// The boot table set (empty when no file is configured — a known-empty set, so every
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
