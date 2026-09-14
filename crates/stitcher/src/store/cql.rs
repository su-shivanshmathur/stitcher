//! CQL backend via the `scylla` driver (covers ScyllaDB and Cassandra, PLAN §3).
//! Table: `id, id_type, version, state(blob=JSON)`; Quorum consistency (PLAN §13).

use std::{collections::HashMap, sync::Arc, time::Instant};

use error_stack::ResultExt;
use futures::{StreamExt, TryStreamExt};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    statement::{prepared::PreparedStatement, Consistency},
};

use super::{RebalanceEvent, Store};
use crate::{
    config,
    errors::{StitcherError, StitcherResult},
    metrics,
    processor::Key,
};

/// Reject anything but plain identifiers so config strings can't become CQL injection.
fn validate_ident(kind: &str, ident: &str) -> StitcherResult<()> {
    let ok = !ident.is_empty() && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(error_stack::report!(StitcherError::Cql(format!(
            "invalid {kind} identifier {ident:?} (allowed: [a-zA-Z0-9_])"
        ))))
    }
}

/// `ScyllaDB` / Cassandra store.
pub struct CqlStore {
    session: Arc<Session>,
    select: PreparedStatement,
    insert: PreparedStatement,
    read_concurrency: usize,
}

impl std::fmt::Debug for CqlStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqlStore").finish_non_exhaustive()
    }
}

impl CqlStore {
    /// Connect + prepare statements; fails fast on unreachable contact points.
    pub async fn connect(cfg: &config::CqlCfg, read_concurrency: usize) -> StitcherResult<Self> {
        validate_ident("keyspace", &cfg.keyspace)?;
        validate_ident("table", &cfg.table)?;

        let mut builder = SessionBuilder::new();
        for host in &cfg.hosts {
            builder = builder.known_node(format!("{}:{}", host, cfg.port));
        }
        if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
            builder = builder.user(user.clone(), pass.expose().clone());
        }

        let session = builder.build().await.map_err(|e| {
            error_stack::report!(StitcherError::Cql(format!(
                "connect {}: {e}",
                cfg.hosts.join(",")
            )))
        })?;
        let session = Arc::new(session);

        // Fail fast: a trivial health query now beats a half-broken pipeline later.
        session
            .query_unpaged("SELECT now() FROM system.local", ())
            .await
            .map_err(|e| error_stack::report!(StitcherError::Cql(format!("health check: {e}"))))?;

        let table = format!("{}.{}", cfg.keyspace, cfg.table);
        let mut select = session
            .prepare(format!(
                "SELECT version, state FROM {table} WHERE id_type = ? AND id = ?"
            ))
            .await
            .map_err(|e| {
                error_stack::report!(StitcherError::Cql(format!("prepare select: {e}")))
            })?;
        select.set_consistency(Consistency::Quorum);

        let mut insert = session
            .prepare(format!(
                "INSERT INTO {table} (id, id_type, version, state) VALUES (?, ?, ?, ?)"
            ))
            .await
            .map_err(|e| {
                error_stack::report!(StitcherError::Cql(format!("prepare insert: {e}")))
            })?;
        insert.set_consistency(Consistency::Quorum);

        Ok(Self {
            session,
            select,
            insert,
            read_concurrency,
        })
    }

    async fn read_one(
        &self,
        id_type: &str,
        key: Key,
    ) -> StitcherResult<Option<(Key, (i64, Vec<u8>))>> {
        let result = self
            .session
            .execute_unpaged(&self.select, (id_type, key.as_str()))
            .await
            .change_context(StitcherError::Cql("execute select".to_string()))?;
        let rows = result
            .into_rows_result()
            .change_context(StitcherError::Cql("rows result".to_string()))?;
        let mut typed = rows
            .rows::<(i64, Vec<u8>)>()
            .map_err(|e| error_stack::report!(StitcherError::Cql(format!("typed rows: {e}"))))?;
        typed
            .next()
            .transpose()
            .map_err(|e| error_stack::report!(StitcherError::Cql(format!("decode row: {e}"))))
            .map(|row| row.map(|(version, state)| (key, (version, state))))
    }
}

#[async_trait::async_trait]
impl Store for CqlStore {
    async fn get_many(
        &self,
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        let started = Instant::now();
        let pairs: Vec<(Key, (i64, Vec<u8>))> = futures::stream::iter(keys.iter().cloned())
            .map(|key| async move { self.read_one(id_type, key).await })
            .buffer_unordered(self.read_concurrency)
            .try_collect::<Vec<Option<(Key, (i64, Vec<u8>))>>>()
            .await?
            .into_iter()
            .flatten()
            .collect();
        metrics::store_get_seconds("cql", started.elapsed().as_secs_f64());
        Ok(pairs.into_iter().collect())
    }

    async fn put(
        &self,
        id_type: &str,
        key: &Key,
        version: i64,
        blob: &[u8],
        _part: &super::PartitionRef,
    ) -> StitcherResult<()> {
        let started = Instant::now();
        self.session
            .execute_unpaged(&self.insert, (key.as_str(), id_type, version, blob))
            .await
            .change_context(StitcherError::Cql("execute insert".to_string()))?;
        metrics::store_put_seconds("cql", started.elapsed().as_secs_f64());
        Ok(())
    }

    async fn on_rebalance(&self, _ev: &RebalanceEvent) -> StitcherResult<()> {
        Ok(()) // remote store is partition-agnostic
    }

    async fn cleanup(&self) -> StitcherResult<()> {
        Ok(()) // session closes on drop
    }
}
