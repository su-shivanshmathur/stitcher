//! Enrichment join-map: `ArcSwap`-held JSON, reloaded in a background task (PLAN §14,
//! `--config-file` / `--sleep-in-sec`).

use std::sync::Arc;

use arc_swap::ArcSwap;
use error_stack::ResultExt;

use crate::{
    config,
    errors::{StitcherError, StitcherResult},
};

/// Shared, hot-swappable join-space. Empty `Value::Null` when no file is configured.
#[derive(Clone)]
pub struct Enrichment {
    state: Arc<ArcSwap<serde_json::Value>>,
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
                        Ok(fresh) => {
                            state.store(Arc::new(fresh));
                            tracing::info!(path, "enrichment map reloaded");
                        }
                        Err(e) => {
                            tracing::warn!(path, error = ?e, "enrichment reload failed; keeping previous map");
                        }
                    }
                }
            });
        }
        Ok(this)
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
