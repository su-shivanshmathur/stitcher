//! The `Store` abstraction + composed local/remote implementation.

use std::{collections::HashMap, future::Future, sync::Arc};

use futures::{StreamExt, TryStreamExt};

use crate::{config, errors::StitcherResult, processor::Key};

#[cfg(feature = "cql")]
pub mod cql;
#[cfg(feature = "rocks")]
pub mod rocks;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartitionRef {
    pub topic: String,
    pub partition: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebalanceEvent {
    Assign(Vec<PartitionRef>),
    Revoke(Vec<PartitionRef>),
}

/// Read-modify-write store; values are `(version, json-bytes)`.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn get_many(
        &self,
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>>;

    async fn put(&self, id_type: &str, key: &Key, version: i64, blob: &[u8]) -> StitcherResult<()>;

    /// Sequential fallback; backends override for efficiency.
    async fn put_many(
        &self,
        id_type: &str,
        entries: &[(Key, Vec<u8>)],
        version: i64,
    ) -> StitcherResult<()> {
        for (key, blob) in entries {
            self.put(id_type, key, version, blob).await?;
        }
        Ok(())
    }

    /// `RocksDB` creates/drops CFs.
    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()>;

    async fn cleanup(&self) -> StitcherResult<()>;
}

/// Run the op, then log + metric — a log line proves the work finished.
pub(crate) async fn traced<T, F, L>(
    backend: &'static str,
    metric: fn(&'static str, f64),
    log: L,
    operation: F,
) -> StitcherResult<T>
where
    F: Future<Output = StitcherResult<T>>,
    L: FnOnce(&T, f64),
{
    let started = std::time::Instant::now();
    let result = operation.await;
    let elapsed_secs = started.elapsed().as_secs_f64();
    if let Ok(outcome) = &result {
        metric(backend, elapsed_secs);
        log(outcome, elapsed_secs);
    }
    result
}

/// Local-first cache + authoritative remote (read local-first, write-through both).
pub struct ComposedStore<L: Store, R: Store> {
    pub local: L,
    pub remote: R,
    pub write_concurrency: usize,
}

impl<L: Store, R: Store> ComposedStore<L, R> {
    /// Futures materialized into a `Vec` — iterator lifetimes fight `buffer_unordered` in `async_trait`.
    async fn remote_put_many(
        &self,
        id_type: &str,
        entries: &[(Key, Vec<u8>)],
        version: i64,
    ) -> StitcherResult<()> {
        let futs: Vec<_> = entries
            .iter()
            .map(|(k, b)| self.remote.put(id_type, k, version, b))
            .collect();
        futures::stream::iter(futs)
            .buffer_unordered(self.write_concurrency)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<L: Store, R: Store> Store for ComposedStore<L, R> {
    async fn get_many(
        &self,
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        let mut found = self.local.get_many(id_type, keys).await?;
        let missing: Vec<Key> = keys
            .iter()
            .filter(|k| !found.contains_key(*k))
            .cloned()
            .collect();
        if !missing.is_empty() {
            found.extend(self.remote.get_many(id_type, &missing).await?);
        }
        Ok(found)
    }

    async fn put(&self, id_type: &str, key: &Key, version: i64, blob: &[u8]) -> StitcherResult<()> {
        // failure of either fails the batch
        let ((), ()) = tokio::try_join!(
            self.local.put(id_type, key, version, blob),
            self.remote.put(id_type, key, version, blob)
        )?;
        Ok(())
    }

    async fn put_many(
        &self,
        id_type: &str,
        entries: &[(Key, Vec<u8>)],
        version: i64,
    ) -> StitcherResult<()> {
        let ((), ()) = tokio::try_join!(
            self.local.put_many(id_type, entries, version),
            self.remote_put_many(id_type, entries, version),
        )?;
        Ok(())
    }

    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()> {
        self.local.on_rebalance(ev).await?;
        self.remote.on_rebalance(ev).await
    }

    async fn cleanup(&self) -> StitcherResult<()> {
        self.local.cleanup().await?;
        self.remote.cleanup().await
    }
}

#[cfg(all(feature = "rocks", feature = "cql"))]
pub async fn build_store(cfg: &config::Settings, id_type: &str) -> StitcherResult<Arc<dyn Store>> {
    let local = rocks::RocksStore::open(&cfg.store.rocksdb, id_type)?;
    let remote = cql::CqlStore::connect(&cfg.store.cql, cfg.read_concurrency).await?;
    Ok(Arc::new(ComposedStore {
        local,
        remote,
        write_concurrency: cfg.read_concurrency,
    }))
}
