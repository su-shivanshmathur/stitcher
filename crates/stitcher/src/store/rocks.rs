//! `RocksDB` local cache: one column family per `id_type` (the state's identity, stable
//! for the process lifetime), keyed by the entity key — mirrors the remote `(id_type, id)`
//! primary key, so an entity has ONE cache home regardless of which topic/partition
//! delivered it. Dropped on revoke (ownership may move to another instance; the remote
//! store is authoritative). Writes are durable-without-WAL; blocking driver calls run on
//! `spawn_blocking`.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, PoisonError, RwLock},
};

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

/// `RocksDB` cache store.
pub struct RocksStore {
    db: Arc<DB>,
    /// `id_type` CF names created so far (under the write lock, to prevent double-create;
    /// dropped on revoke) — lets a `cf_handle` presence check stay a cheap read.
    created_id_type_cfs: RwLock<HashSet<String>>,
}

impl std::fmt::Debug for RocksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksStore").finish_non_exhaustive()
    }
}

impl RocksStore {
    /// Open (or create) the DB at `cfg.path` with all pre-existing CFs.
    pub fn open(cfg: &config::RocksCfg) -> StitcherResult<Self> {
        let path = cfg.path.clone();
        let ttl_secs = cfg.ttl_secs;
        let span = tracing::info_span!("rocksdb_open", path = %path);
        let _enter = span.enter();
        Self::open_inner(&path, ttl_secs)
    }

    fn open_inner(path: &str, ttl_secs: u64) -> StitcherResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // larger write buffers + WAL disabled per-put (cache only)
        opts.set_write_buffer_size(128 * 1024 * 1024);

        // `list_cf` already includes the default CF on an existing DB, and is empty on a
        // fresh one — prepend the default then de-dup so we never build two descriptors.
        let default = rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string();
        let existing = DB::list_cf(&opts, path).unwrap_or_default();
        let mut all: Vec<String> = vec![default.clone()];
        all.extend(existing.into_iter().filter(|name| name != &default));
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

        Ok(Self {
            db: Arc::new(db),
            created_id_type_cfs: RwLock::new(HashSet::new()),
        })
    }

    /// Ensure the `id_type` CF exists. Double-checked locking: the presence check and the
    /// create + insert run under the write lock, so two batches racing on a new `id_type`
    /// can't both call `create_cf` (which RocksDB rejects as already-exists).
    fn ensure_id_type_cf(&self, id_type: &str) -> StitcherResult<()> {
        if self
            .created_id_type_cfs
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(id_type)
        {
            return Ok(());
        }
        let mut created = self
            .created_id_type_cfs
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if created.contains(id_type) {
            return Ok(()); // another task created it while we waited for the write lock
        }
        if self.db.cf_handle(id_type).is_none() {
            self.db
                .create_cf(id_type, &Options::default())
                .change_context(StitcherError::Rocks(format!("create_cf {id_type}")))?;
        }
        created.insert(id_type.to_string());
        Ok(())
    }

    /// Drop every cached CF and forget it (recreated lazily). Each CF is dropped from the
    /// DB *before* it is removed from the set, so a failed drop leaves the set consistent
    /// with the DB (the CF stays tracked). Only called during the rebalance drain, so no
    /// get/put is in flight.
    fn drop_all_cfs(&self) -> StitcherResult<()> {
        let mut created = self
            .created_id_type_cfs
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let names: Vec<String> = created.iter().cloned().collect();
        for name in names {
            if self.db.cf_handle(&name).is_some() {
                self.db
                    .drop_cf(&name)
                    .change_context(StitcherError::Rocks(format!("drop_cf {name}")))?;
            }
            created.remove(&name);
        }
        Ok(())
    }

    async fn get_many_inner(
        &self,
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        self.ensure_id_type_cf(id_type)?;
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
                let cf_name = id_type.to_string();
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

    async fn put_inner(
        &self,
        id_type: &str,
        key: &Key,
        version: i64,
        blob: &[u8],
    ) -> StitcherResult<()> {
        self.ensure_id_type_cf(id_type)?;
        super::traced(
            "rocksdb",
            metrics::store_put_seconds,
            |&(), secs| {
                tracing::debug!(
                    key = %key.as_str(),
                    version,
                    blob_bytes = blob.len(),
                    cf = %id_type,
                    elapsed_ms = secs * 1e3,
                    "rocksdb: put_cf (wal disabled)"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let cf_name = id_type.to_string();
                let key_bytes = key.as_bytes().to_vec();
                let value = codec::frame(version, blob);
                // The CF can't vanish under us: `drop_all_cfs` only runs during the
                // rebalance drain, which waits for in-flight batches to finish first.
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
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        self.get_many_inner(id_type, keys).await
    }

    async fn put(&self, id_type: &str, key: &Key, version: i64, blob: &[u8]) -> StitcherResult<()> {
        self.put_inner(id_type, key, version, blob).await
    }

    /// Only called during the rebalance drain (no batch in flight), so clearing the cache
    /// can't race a get/put. A revoke may move an entity's ownership to another instance,
    /// so the local cache is discarded to avoid stale reads after re-assign.
    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()> {
        if let RebalanceEvent::Revoke(_) = ev {
            self.drop_all_cfs()?;
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
