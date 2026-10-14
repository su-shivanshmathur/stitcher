//! `RocksDB` local cache: one column family per `id_type`, keyed by entity key (mirrors
//! the remote `(id_type, id)` PK). Wiped on revoke — the remote is authoritative. Writes
//! skip the WAL; blocking calls run on `spawn_blocking`.

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

const STATE_COLUMN_FAMILY_PREFIX: &str = "state.";

pub struct RocksStore {
    db: Arc<DB>,
    state_column_family: String,
}

impl std::fmt::Debug for RocksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksStore").finish_non_exhaustive()
    }
}

impl RocksStore {
    pub fn open(cfg: &config::RocksCfg, id_type: &str) -> StitcherResult<Self> {
        let path = cfg.path.clone();
        let ttl_secs = cfg.ttl_secs;
        let state_column_family = format!("{STATE_COLUMN_FAMILY_PREFIX}{id_type}");
        let span =
            tracing::info_span!("rocksdb_open", path = %path, column_family = %state_column_family);
        let _enter = span.enter();
        Self::open_inner(&path, ttl_secs, state_column_family)
    }

    fn open_inner(path: &str, ttl_secs: u64, state_column_family: String) -> StitcherResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(128 * 1024 * 1024);

        // every CF must be listed at open: default, on-disk, and ours (created here with the TTL)
        let default = rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string();
        let existing = DB::list_cf(&opts, path).unwrap_or_default();
        let mut all: Vec<String> = vec![default.clone(), state_column_family.clone()];
        all.extend(
            existing
                .into_iter()
                .filter(|name| name != &default && name != &state_column_family),
        );
        let descriptors: Vec<ColumnFamilyDescriptor> = all
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name.clone(), Options::default()))
            .collect();

        let db = if ttl_secs > 0 {
            DB::open_cf_descriptors_with_ttl(
                &opts,
                path,
                descriptors,
                std::time::Duration::from_secs(ttl_secs),
            )
        } else {
            DB::open_cf_descriptors(&opts, path, descriptors)
        }
        .change_context(StitcherError::Rocks(format!("open {path}")))?;

        // drop legacy `topic:partition` / stale id_type CFs
        for name in &all {
            if name != &default && name != &state_column_family {
                db.drop_cf(name)
                    .change_context(StitcherError::Rocks(format!(
                        "drop legacy column family {name}"
                    )))?;
            }
        }

        Ok(Self {
            db: Arc::new(db),
            state_column_family,
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
                let column_family_name = self.state_column_family.clone();
                let keys = keys.to_vec();
                let out =
                    spawn_blocking(move || -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
                        let mut found = HashMap::with_capacity(keys.len());
                        let Some(column_family) = db.cf_handle(&column_family_name) else {
                            return Ok(found);
                        };
                        for key in &keys {
                            if let Some(raw) = db
                                .get_cf(&column_family, key.as_bytes())
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

    async fn put_many_inner(&self, entries: &[(Key, Vec<u8>)], version: i64) -> StitcherResult<()> {
        const CHUNK_SIZE: usize = 1024;
        let n_entries = entries.len();
        // pre-encode before entering spawn_blocking
        let encoded: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(key, blob)| (key.as_bytes().to_vec(), codec::frame(version, blob)))
            .collect();
        super::traced(
            "rocksdb",
            metrics::store_put_seconds,
            |&(), secs| {
                tracing::debug!(
                    entries = n_entries,
                    elapsed_ms = secs * 1e3,
                    "rocksdb: put_many (write_batch, wal disabled)"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let column_family_name = self.state_column_family.clone();
                spawn_blocking(move || -> StitcherResult<()> {
                    let column_family = db.cf_handle(&column_family_name).ok_or_else(|| {
                        error_stack::report!(StitcherError::Rocks(format!(
                            "column family {column_family_name} missing"
                        )))
                    })?;
                    let mut wopts = rocksdb::WriteOptions::default();
                    wopts.disable_wal(true);
                    for chunk in encoded.chunks(CHUNK_SIZE) {
                        let mut batch = rocksdb::WriteBatch::default();
                        for (key_bytes, value) in chunk {
                            batch.put_cf(&column_family, key_bytes, value);
                        }
                        db.write_opt(batch, &wopts)
                            .change_context(StitcherError::Rocks("write_batch".to_string()))?;
                    }
                    Ok(())
                })
                .await
                .change_context(StitcherError::Rocks("spawn_blocking join".to_string()))??;
                Ok(())
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
                    column_family = %self.state_column_family,
                    elapsed_ms = secs * 1e3,
                    "rocksdb: put_cf (wal disabled)"
                );
            },
            async {
                let db = Arc::clone(&self.db);
                let column_family_name = self.state_column_family.clone();
                let key_bytes = key.as_bytes().to_vec();
                let value = codec::frame(version, blob);
                spawn_blocking(move || -> StitcherResult<()> {
                    let column_family = db.cf_handle(&column_family_name).ok_or_else(|| {
                        error_stack::report!(StitcherError::Rocks(format!(
                            "column family {column_family_name} missing"
                        )))
                    })?;
                    let mut wopts = rocksdb::WriteOptions::default();
                    wopts.disable_wal(true);
                    db.put_cf_opt(&column_family, key_bytes, value, &wopts)
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
        _id_type: &str, // fixed at open; single id_type per process
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        self.get_many_inner(keys).await
    }

    async fn put(
        &self,
        _id_type: &str, // fixed at open; single id_type per process
        key: &Key,
        version: i64,
        blob: &[u8],
    ) -> StitcherResult<()> {
        self.put_inner(key, version, blob).await
    }

    async fn put_many(
        &self,
        _id_type: &str,
        entries: &[(Key, Vec<u8>)],
        version: i64,
    ) -> StitcherResult<()> {
        self.put_many_inner(entries, version).await
    }

    /// Wipe contents on revoke (CF kept); on drain timeout a late single-instance put is
    /// idempotent.
    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()> {
        if let RebalanceEvent::Revoke(_) = ev {
            if let Some(column_family) = self.db.cf_handle(&self.state_column_family) {
                // `["", 0xFF)` covers every key — 0xFF is never a valid UTF-8 byte
                let (lo, hi): (&[u8], &[u8]) = (&[], &[0xFF]);
                self.db
                    .delete_range_cf(&column_family, lo, hi)
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
