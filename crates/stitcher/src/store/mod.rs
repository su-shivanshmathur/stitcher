//! The `Store` abstraction + composed local/remote implementation (PLAN §Core abstractions).

use std::{collections::HashMap, future::Future, sync::Arc};

use crate::{config, errors::StitcherResult, processor::Key};

#[cfg(feature = "cql")]
pub mod cql;
#[cfg(feature = "dynamodb")]
pub mod dynamo;
#[cfg(feature = "rocks")]
pub mod rocks;

/// A Kafka topic-partition, mapped to a `RocksDB` column family.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartitionRef {
    /// Source topic.
    pub topic: String,
    /// Partition id.
    pub partition: i32,
}

impl PartitionRef {
    /// `RocksDB` column family name.
    #[must_use]
    pub fn cf_name(&self) -> String {
        format!("{}:{}", self.topic, self.partition)
    }
}

/// Consumer-group rebalance signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebalanceEvent {
    /// Partitions assigned.
    Assign(Vec<PartitionRef>),
    /// Partitions revoked.
    Revoke(Vec<PartitionRef>),
}

/// Read-modify-write state store. Values are `(version, json-bytes)`; framing details are a
/// backend concern (`RocksDB`: 8-byte LE header, CQL: version column) (PLAN §13).
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Fetch states for `keys`; absent keys are simply not in the map.
    async fn get_many(
        &self,
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>>;

    /// Persist a merged state. `part` selects the `RocksDB` CF; remote backends ignore it.
    async fn put(
        &self,
        id_type: &str,
        key: &Key,
        version: i64,
        blob: &[u8],
        part: &PartitionRef,
    ) -> StitcherResult<()>;

    /// Rebalance hook (`RocksDB` creates/drops CFs).
    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()>;

    /// Flush + release resources on shutdown.
    async fn cleanup(&self) -> StitcherResult<()>;
}

/// Execute a store operation, *then* log it — one shared site for every backend
/// (cql, rocksdb, …): the caller's debug line and the duration metric fire only
/// after the future resolves successfully, so a log line proves the work
/// finished (a pre-operation log cannot). Failures propagate undecorated; the
/// backend's error context already names the operation.
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

/// Local-first cache + authoritative remote (PLAN §5: read local-first, write-through both).
pub struct ComposedStore<L: Store, R: Store> {
    /// Local cache (`RocksDB`).
    pub local: L,
    /// Authoritative remote store (CQL / stub).
    pub remote: R,
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

    async fn put(
        &self,
        id_type: &str,
        key: &Key,
        version: i64,
        blob: &[u8],
        part: &PartitionRef,
    ) -> StitcherResult<()> {
        // dual write; failure of either fails the batch (PLAN §Failure modes)
        let ((), ()) = tokio::try_join!(
            self.local.put(id_type, key, version, blob, part),
            self.remote.put(id_type, key, version, blob, part)
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

/// Build the configured store (PLAN §2: CQL now, `DynamoDB` stubbed).
#[cfg(all(feature = "rocks", feature = "cql"))]
pub async fn build_store(cfg: &config::Settings) -> StitcherResult<Arc<dyn Store>> {
    // only used by the unbuilt-backend arm below
    #[cfg(all(feature = "rocks", feature = "cql", not(feature = "dynamodb")))]
    use crate::errors::StitcherError;
    let local = rocks::RocksStore::open(&cfg.store.rocksdb)?;
    match cfg.store.backend {
        config::Backend::Cql => {
            let remote = cql::CqlStore::connect(&cfg.store.cql, cfg.read_concurrency).await?;
            Ok(Arc::new(ComposedStore { local, remote }))
        }
        #[cfg(feature = "dynamodb")]
        config::Backend::Dynamodb => {
            let remote = dynamo::DynamoStore::new(&cfg.store);
            Ok(Arc::new(ComposedStore { local, remote }))
        }
        #[cfg(not(feature = "dynamodb"))]
        config::Backend::Dynamodb => Err(error_stack::report!(StitcherError::Unsupported(
            "dynamodb — rebuild with --features dynamodb (stub backend)"
        ))),
    }
}
