//! `RocksDB` local cache: one column family per `topic-partition` (dropped on revoke);
//! reads fan out across all CFs (first hit wins); writes durable-without-WAL — the remote
//! store is the source of truth. Blocking driver calls run on `spawn_blocking`.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use error_stack::ResultExt;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tokio::task::spawn_blocking;

use super::{PartitionRef, RebalanceEvent, Store};
use crate::{
    codec, config,
    errors::{StitcherError, StitcherResult},
    metrics,
    processor::Key,
};

/// `RocksDB` cache store.
pub struct RocksStore {
    db: Arc<DB>,
    /// Names of CFs we are responsible for (default excluded); handle lookup stays
    /// fresh by querying the DB — CF churn is a rebalance-time event, not per message.
    known_cfs: std::sync::RwLock<HashSet<String>>,
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

        let existing = DB::list_cf(&opts, path).unwrap_or_default();
        let mut all: Vec<String> = vec![rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string()];
        all.extend(existing);
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
            known_cfs: std::sync::RwLock::new(HashSet::new()),
        })
    }

    fn set_known_cfs(&self, parts: &[PartitionRef], insert: bool) {
        let mut known = self
            .known_cfs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for part in parts {
            let name = part.cf_name();
            if insert {
                known.insert(name);
            } else {
                known.remove(&name);
            }
        }
    }

    fn create_cf_sync(db: &DB, name: &str) -> StitcherResult<()> {
        db.create_cf(name, &Options::default())
            .change_context(StitcherError::Rocks(format!("create_cf {name}")))?;
        Ok(())
    }

    fn drop_cf_sync(db: &DB, name: &str) -> StitcherResult<()> {
        db.drop_cf(name)
            .change_context(StitcherError::Rocks(format!("drop_cf {name}")))?;
        Ok(())
    }

    async fn get_many_inner(&self, keys: &[Key]) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        let n_keys = keys.len();
        super::traced(
            "rocksdb",
            metrics::store_get_seconds,
            |found, secs| {
                tracing::debug!(
                    keys = n_keys,
                    found = found.len(),
                    elapsed_ms = secs * 1e3,
                    "rocksdb: get across column families"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let cf_names: HashSet<String> = self
                    .known_cfs
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let keys = keys.to_vec();
                let out =
                    spawn_blocking(move || -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
                        let mut found = HashMap::with_capacity(keys.len());
                        // resolve CF handles once per batch, not per (key, CF) pair
                        let cfs: Vec<_> = cf_names
                            .iter()
                            .filter_map(|name| db.cf_handle(name))
                            .collect();
                        for key in &keys {
                            for cf in &cfs {
                                if let Some(raw) = db
                                    .get_cf(cf, key.as_bytes())
                                    .change_context(StitcherError::Rocks("get_cf".to_string()))?
                                {
                                    if let Some((version, json)) = codec::unframe(&raw) {
                                        found.insert(key.clone(), (version, json.to_vec()));
                                        break; // first CF hit wins
                                    }
                                    // unframeable bytes = corrupt cache entry → treat as absent
                                }
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
        key: &Key,
        version: i64,
        blob: &[u8],
        part: &PartitionRef,
    ) -> StitcherResult<()> {
        super::traced(
            "rocksdb",
            metrics::store_put_seconds,
            |&(), secs| {
                tracing::debug!(
                    key = %key.as_str(),
                    version,
                    blob_bytes = blob.len(),
                    cf = %part.cf_name(),
                    elapsed_ms = secs * 1e3,
                    "rocksdb: put_cf (wal disabled)"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let cf_name = part.cf_name();
                let key_bytes = key.as_bytes().to_vec();
                let value = codec::frame(version, blob);
                spawn_blocking(move || -> StitcherResult<()> {
                    let cf = if let Some(cf) = db.cf_handle(&cf_name) {
                        cf
                    } else {
                        Self::create_cf_sync(&db, &cf_name)?;
                        db.cf_handle(&cf_name).ok_or_else(|| {
                            error_stack::report!(StitcherError::Rocks(format!(
                                "cf {cf_name} missing after create"
                            )))
                        })?
                    };
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
        _id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        self.get_many_inner(keys).await
    }

    async fn put(
        &self,
        _id_type: &str,
        key: &Key,
        version: i64,
        blob: &[u8],
        part: &PartitionRef,
    ) -> StitcherResult<()> {
        self.put_inner(key, version, blob, part).await
    }

    /// Synchronous CF maintenance — safe to call from rdkafka's rebalance callbacks.
    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()> {
        match ev {
            RebalanceEvent::Assign(parts) => {
                for part in parts {
                    let name = part.cf_name();
                    if self.db.cf_handle(&name).is_none() {
                        Self::create_cf_sync(&self.db, &name)?;
                    }
                }
                self.set_known_cfs(parts, true);
            }
            RebalanceEvent::Revoke(parts) => {
                for part in parts {
                    let name = part.cf_name();
                    if self.db.cf_handle(&name).is_some() {
                        // Revoked CFs are dropped — state re-syncs from the remote store
                        // on re-assign.
                        Self::drop_cf_sync(&self.db, &name)?;
                    }
                }
                self.set_known_cfs(parts, false);
            }
        }
        metrics::assigned_partitions(
            self.known_cfs
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
        );
        Ok(())
    }

    async fn cleanup(&self) -> StitcherResult<()> {
        self.db
            .flush()
            .change_context(StitcherError::Rocks("flush".to_string()))?;
        Ok(())
    }
}
