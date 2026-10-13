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
}

/// Why a hot-reload was rejected (the previous map is kept).
enum ReloadRejection {
    /// The reloaded value is not a JSON object.
    NotObject,
    /// The reload dropped a table present at boot.
    MissingBootTable,
}

impl ReloadRejection {
    fn reason(self) -> &'static str {
        match self {
            Self::NotObject => "not_object",
            Self::MissingBootTable => "missing_boot_table",
        }
    }
}

impl Enrichment {
    fn load_file(path: &str) -> StitcherResult<serde_json::Value> {
        let bytes = std::fs::read(path)
            .change_context(StitcherError::Enrichment(format!("read {path}")))?;
        serde_json::from_slice(&bytes)
            .change_context(StitcherError::Enrichment(format!("parse {path} as JSON")))
    }

    /// Load the map once, then keep it fresh in the background. A missing/blank
    /// `config_file` disables it; reload failures keep the previous map (stale beats absent).
    pub fn spawn_reloader(cfg: &config::Enrichment) -> StitcherResult<Self> {
        if cfg.config_file.is_empty() {
            return Ok(Self {
                state: Arc::new(ArcSwap::from(Arc::new(serde_json::Value::Null))),
            });
        }
        let initial = Self::load_file(&cfg.config_file)?;
        // The tables present at boot are the validated contract (transformer `lookup`s were checked
        // against them). A reload must not drop any, or a validated lookup would silently return null.
        let required: std::collections::BTreeSet<String> = initial
            .as_object()
            .map(|tables| tables.keys().cloned().collect())
            .unwrap_or_default();
        let this = Self {
            state: Arc::new(ArcSwap::from(Arc::new(initial))),
        };
        if cfg.reload_secs > 0 {
            let state = Arc::clone(&this.state);
            let path = cfg.config_file.clone();
            let period = std::time::Duration::from_secs(cfg.reload_secs);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    match Self::load_file(&path) {
                        Ok(fresh) => match Self::check_reload(&fresh, &required) {
                            Ok(()) => {
                                state.store(Arc::new(fresh));
                                tracing::info!(path, "enrichment map reloaded");
                            }
                            Err(rejection) => {
                                metrics::enrichment_reload_rejected();
                                tracing::warn!(path, reason = rejection.reason(), "enrichment reload rejected; keeping previous map");
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

    /// A reload is accepted only if it is a JSON object that still contains every boot table.
    fn check_reload(
        fresh: &serde_json::Value,
        required: &std::collections::BTreeSet<String>,
    ) -> Result<(), ReloadRejection> {
        match fresh.as_object() {
            None => Err(ReloadRejection::NotObject),
            Some(tables) => required
                .iter()
                .all(|table| tables.contains_key(table))
                .then_some(())
                .ok_or(ReloadRejection::MissingBootTable),
        }
    }

    /// Top-level table names in the configured join-map, for validating transformer
    /// `lookup(table, …)` references at boot. `None` ⇒ no enrichment file configured (skip the
    /// check); `Some` (possibly empty) ⇒ the configured table set. A non-object file is an error.
    pub fn table_names(cfg: &config::Enrichment) -> StitcherResult<Option<Vec<String>>> {
        if cfg.config_file.is_empty() {
            return Ok(None);
        }
        let map = Self::load_file(&cfg.config_file)?;
        map.as_object()
            .map(|tables| Some(tables.keys().cloned().collect()))
            .ok_or_else(|| {
                error_stack::report!(StitcherError::Enrichment(format!(
                    "enrichment file {} must be a JSON object",
                    cfg.config_file
                )))
            })
    }

    /// Look a key up in the join-map (top-level object members only).
    #[must_use]
    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.state.load().get(key).cloned()
    }

    /// Read the whole join-map (immutable snapshot).
    #[must_use]
    pub fn snapshot(&self) -> Arc<serde_json::Value> {
        self.state.load_full()
    }
}
