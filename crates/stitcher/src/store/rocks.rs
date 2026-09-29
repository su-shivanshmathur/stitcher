//! `RocksDB` local cache: a single column family for the processor's `id_type` (fixed at
//! startup), keyed by the entity key — mirrors the remote `(id_type, id)` primary key, so
//! an entity has ONE cache home regardless of which topic/partition delivered it. The CF
//! is created at open (under a `state.` prefix, so it never aliases the default CF or the
//! old `topic:partition` layout) and inherits `ttl_secs`; its contents are wiped on revoke
//! (ownership may move to another instance; the remote store is authoritative). Writes are
//! durable-without-WAL; blocking driver calls run on `spawn_blocking`.

use std::{collections::HashMap, sync::Arc};

use error_stack::ResultExt;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tokio::task::spawn_blocking;

use super::{RebalanceEvent, Store};
use crate::{
    codec, config,
    errors::{StitcherError, StitcherResult},
    metrics,
    processor::Key,
};

/// Prefix isolating the state CF from the default CF and the legacy `topic:partition` CFs.
const STATE_CF_PREFIX: &str = "state.";

/// `RocksDB` cache store — one CF (`state.<id_type>`) for the process's state.
pub struct RocksStore {
    db: Arc<DB>,
    /// The single state CF name, `state.<id_type>`.
    state_cf: String,
}

impl std::fmt::Debug for RocksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksStore").finish_non_exhaustive()
    }
}

impl RocksStore {
    /// Open (or create) the DB at `cfg.path`; the `id_type` CF is created here with the TTL.
    pub fn open(cfg: &config::RocksCfg, id_type: &str) -> StitcherResult<Self> {
        let path = cfg.path.clone();
        let ttl_secs = cfg.ttl_secs;
        let state_cf = format!("{STATE_CF_PREFIX}{id_type}");
        let span = tracing::info_span!("rocksdb_open", path = %path, cf = %state_cf);
        let _enter = span.enter();
        Self::open_inner(&path, ttl_secs, state_cf)
    }

    fn open_inner(path: &str, ttl_secs: u64, state_cf: String) -> StitcherResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // larger write buffers + WAL disabled per-put (cache only)
        opts.set_write_buffer_size(128 * 1024 * 1024);

        // Every CF must be listed at open. Include default, whatever exists on disk, and
        // the state CF (so it's created here and inherits the TTL below).
        let default = rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string();
        let existing = DB::list_cf(&opts, path).unwrap_or_default();
        let mut all: Vec<String> = vec![default.clone(), state_cf.clone()];
        all.extend(
            existing
                .into_iter()
                .filter(|name| name != &default && name != &state_cf),
        );
        let cfds: Vec<ColumnFamilyDescriptor> = all
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name.clone(), Options::default()))
            .collect();

        let db = if ttl_secs > 0 {
            DB::open_cf_descriptors_with_ttl(
                &opts,
                path,
                cfds,
                std::time::Duration::from_secs(ttl_secs),
            )
        } else {
            DB::open_cf_descriptors(&opts, path, cfds)
        }
        .change_context(StitcherError::Rocks(format!("open {path}")))?;

        // Migration: drop any CF that is neither the default nor our state CF — i.e. the
        // legacy `topic:partition` cache, or a stale state CF for a different id_type.
        for name in &all {
            if name != &default && name != &state_cf {
                db.drop_cf(name)
                    .change_context(StitcherError::Rocks(format!("drop legacy cf {name}")))?;
            }
        }

        Ok(Self {
            db: Arc::new(db),
            state_cf,
        })
    }

    async fn get_many_inner(&self, keys: &[Key]) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        let n_keys = keys.len();
        super::traced(
            "rocksdb",
            metrics::store_get_seconds,
            |found: &HashMap<Key, (i64, Vec<u8>)>, secs| {
                tracing::debug!(
                    keys = n_keys,
                    found = found.len(),
                    elapsed_ms = secs * 1e3,
                    "rocksdb: get"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let cf_name = self.state_cf.clone();
                let keys = keys.to_vec();
                let out =
                    spawn_blocking(move || -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
                        let mut found = HashMap::with_capacity(keys.len());
                        let Some(cf) = db.cf_handle(&cf_name) else {
                            return Ok(found);
                        };
                        for key in &keys {
                            if let Some(raw) = db
                                .get_cf(&cf, key.as_bytes())
                                .change_context(StitcherError::Rocks("get_cf".to_string()))?
                            {
                                if let Some((version, json)) = codec::unframe(&raw) {
                                    found.insert(key.clone(), (version, json.to_vec()));
                                }
                                // unframeable bytes = corrupt cache entry → treat as absent
                            }
                        }
                        Ok(found)
                    })
                    .await
                    .change_context(StitcherError::Rocks("spawn_blocking join".to_string()))??;
                Ok(out)
            },
        )
        .await
    }

    async fn put_inner(&self, key: &Key, version: i64, blob: &[u8]) -> StitcherResult<()> {
        super::traced(
            "rocksdb",
            metrics::store_put_seconds,
            |&(), secs| {
                tracing::debug!(
                    key = %key.as_str(),
                    version,
                    blob_bytes = blob.len(),
                    cf = %self.state_cf,
                    elapsed_ms = secs * 1e3,
                    "rocksdb: put_cf (wal disabled)"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let cf_name = self.state_cf.clone();
                let key_bytes = key.as_bytes().to_vec();
                let value = codec::frame(version, blob);
                spawn_blocking(move || -> StitcherResult<()> {
                    let cf = db.cf_handle(&cf_name).ok_or_else(|| {
                        error_stack::report!(StitcherError::Rocks(format!("cf {cf_name} missing")))
                    })?;
                    let mut wopts = rocksdb::WriteOptions::default();
                    wopts.disable_wal(true); // cache only; remote is authoritative
                    db.put_cf_opt(&cf, key_bytes, value, &wopts)
                        .change_context(StitcherError::Rocks("put_cf".to_string()))?;
                    Ok(())
                })
                .await
                .change_context(StitcherError::Rocks("spawn_blocking join".to_string()))??;
                Ok(())
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Store for RocksStore {
    async fn get_many(
        &self,
        _id_type: &str, // fixed at open; `RocksStore` is single-id_type per process
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        self.get_many_inner(keys).await
    }

    async fn put(
        &self,
        _id_type: &str, // fixed at open; `RocksStore` is single-id_type per process
        key: &Key,
        version: i64,
        blob: &[u8],
    ) -> StitcherResult<()> {
        self.put_inner(key, version, blob).await
    }

    /// Wipes the local cache on revoke. Called after the drain wait in `pre_rebalance`;
    /// normally no batch is in flight, but on drain timeout one may still be running.
    /// In the single-instance case a late put writes the same bytes to both local and
    /// remote, so the cache stays consistent — the stale-read risk is multi-instance
    /// (already out of scope). The CF structure is preserved; only its contents are
    /// range-deleted.
    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()> {
        if let RebalanceEvent::Revoke(_) = ev {
            if let Some(cf) = self.db.cf_handle(&self.state_cf) {
                // Range `["", 0xFF)` covers every key: 0xFF is not a valid UTF-8 byte
                // (max encoding byte is 0xF4 for U+10FFFF), so no entity key sorts ≥ it.
                let (lo, hi): (&[u8], &[u8]) = (&[], &[0xFF]);
                self.db
                    .delete_range_cf(&cf, lo, hi)
                    .change_context(StitcherError::Rocks("delete_range_cf".to_string()))?;
            }
        }
        Ok(())
    }

    async fn cleanup(&self) -> StitcherResult<()> {
        self.db
            .flush()
            .change_context(StitcherError::Rocks("flush".to_string()))?;
        Ok(())
    }
}
