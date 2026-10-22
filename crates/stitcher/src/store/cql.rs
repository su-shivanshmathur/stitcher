//! CQL backend (ScyllaDB / Cassandra): Quorum consistency over `id, id_type, version, state`.
//! An optional `write_token bigint` column enables observe-only cross-writer detection: a key
//! read back carrying a token this pod did not write means another instance wrote it.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use error_stack::ResultExt;
use futures::{StreamExt, TryStreamExt};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder, Compression},
    errors::TranslationError,
    policies::address_translator::{AddressTranslator, UntranslatedPeer},
    statement::{prepared::PreparedStatement, Consistency},
};

use super::{RebalanceEvent, Store};
use crate::{
    config,
    constants::WRITE_TOKEN_MAP_CAP,
    errors::{StitcherError, StitcherResult},
    metrics,
    processor::Key,
};

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

struct PeerAddressMap(HashMap<SocketAddr, SocketAddr>);

#[async_trait::async_trait]
impl AddressTranslator for PeerAddressMap {
    // bare elision mirrors the upstream trait; explicit lifetimes trip async_trait (E0195)
    #[allow(elided_lifetimes_in_paths)]
    async fn translate_address(
        &self,
        peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        match self.0.get(&peer.untranslated_address()) {
            Some(&to) => Ok(to),
            None => Ok(peer.untranslated_address()),
        }
    }
}

/// Per-pod last write-token per key. A key read back with a token this pod did not write means
/// another instance wrote it — a cross-writer (co-partitioning) violation. Observe-only; the map
/// is capped (cleared wholesale past the cap) so it never grows unbounded.
#[derive(Default)]
struct WriteTracker {
    seen: Mutex<HashMap<Key, i64>>,
}

impl WriteTracker {
    // Recover the guard on poison — a best-effort observability map, never worth a panic.
    fn seen_map(&self) -> std::sync::MutexGuard<'_, HashMap<Key, i64>> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn note_write(&self, key: &Key, token: i64) {
        let mut seen = self.seen_map();
        if seen.len() >= WRITE_TOKEN_MAP_CAP {
            seen.clear();
        }
        seen.insert(key.clone(), token);
    }

    fn observe_read(&self, key: &Key, token: Option<i64>) {
        if let Some(token) = token {
            let mut seen = self.seen_map();
            if seen.get(key).is_some_and(|&mine| mine != token) {
                metrics::cross_writer_conflict();
                tracing::warn!(
                    key = %key.as_str(),
                    "cql: cross-writer conflict — key last written by another instance"
                );
            }
            if seen.len() >= WRITE_TOKEN_MAP_CAP {
                seen.clear();
            }
            seen.insert(key.clone(), token);
        }
    }
}

// Per-call random-ish write token (same zero-dep entropy source as inspect sampling).
fn new_write_token() -> i64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0);
    i64::from_ne_bytes(hasher.finish().to_ne_bytes())
}

async fn prepare_quorum(session: &Session, cql: String) -> StitcherResult<PreparedStatement> {
    let mut stmt = session
        .prepare(cql.clone())
        .await
        .map_err(|e| error_stack::report!(StitcherError::Cql(format!("prepare {cql}: {e}"))))?;
    stmt.set_consistency(Consistency::Quorum);
    Ok(stmt)
}

pub struct CqlStore {
    session: Arc<Session>,
    select: PreparedStatement,
    insert: PreparedStatement,
    read_concurrency: usize,
    // `None` when the `write_token` column is absent — cross-writer detection is then off.
    write_tracker: Option<WriteTracker>,
}

impl std::fmt::Debug for CqlStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqlStore").finish_non_exhaustive()
    }
}

impl CqlStore {
    pub async fn connect(cfg: &config::CqlCfg, read_concurrency: usize) -> StitcherResult<Self> {
        validate_ident("keyspace", &cfg.keyspace)?;
        validate_ident("table", &cfg.table)?;

        let mut builder = SessionBuilder::new().compression(Some(Compression::Lz4));
        for host in &cfg.hosts {
            builder = builder.known_node(format!("{}:{}", host, cfg.port));
        }
        if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
            builder = builder.user(user.clone(), pass.expose().clone());
        }
        if !cfg.address_translation.is_empty() {
            let mut rules = HashMap::new();
            for (from, to) in &cfg.address_translation {
                let from: SocketAddr = from.parse().map_err(|e| {
                    error_stack::report!(StitcherError::Cql(format!(
                        "address_translation: invalid source {from:?}: {e}"
                    )))
                })?;
                let to: SocketAddr = to.parse().map_err(|e| {
                    error_stack::report!(StitcherError::Cql(format!(
                        "address_translation: invalid target {to:?}: {e}"
                    )))
                })?;
                rules.insert(from, to);
            }
            builder = builder.address_translator(Arc::new(PeerAddressMap(rules)));
        }

        let session = builder.build().await.map_err(|e| {
            error_stack::report!(StitcherError::Cql(format!(
                "connect {}: {e}",
                cfg.hosts.join(",")
            )))
        })?;
        let session = Arc::new(session);

        session
            .query_unpaged("SELECT now() FROM system.local", ())
            .await
            .map_err(|e| error_stack::report!(StitcherError::Cql(format!("health check: {e}"))))?;

        let table = format!("{}.{}", cfg.keyspace, cfg.table);
        // Prefer the write-token statements (cross-writer detection); fall back to the plain
        // schema when the column is absent, so detection is opt-in via a single ALTER TABLE
        // rather than a hard startup dependency.
        let token_select = prepare_quorum(
            &session,
            format!("SELECT version, state, write_token FROM {table} WHERE id_type = ? AND id = ?"),
        )
        .await;
        let token_insert = prepare_quorum(
            &session,
            format!(
                "INSERT INTO {table} (id, id_type, version, state, write_token) \
                 VALUES (?, ?, ?, ?, ?)"
            ),
        )
        .await;
        let (select, insert, write_tracker) = match (token_select, token_insert) {
            (Ok(select), Ok(insert)) => {
                tracing::info!("cql: write_token column present — cross-writer detection enabled");
                (select, insert, Some(WriteTracker::default()))
            }
            (select_result, insert_result) => {
                let reason = select_result
                    .err()
                    .or_else(|| insert_result.err())
                    .map_or_else(|| "unknown".to_string(), |e| e.to_string());
                tracing::warn!(
                    error = %reason,
                    "cql: write_token statements unavailable (column absent or prepare failed) — \
                     cross-writer detection disabled (ALTER TABLE {table} ADD write_token bigint)"
                );
                let select = prepare_quorum(
                    &session,
                    format!("SELECT version, state FROM {table} WHERE id_type = ? AND id = ?"),
                )
                .await?;
                let insert = prepare_quorum(
                    &session,
                    format!("INSERT INTO {table} (id, id_type, version, state) VALUES (?, ?, ?, ?)"),
                )
                .await?;
                (select, insert, None)
            }
        };

        Ok(Self {
            session,
            select,
            insert,
            read_concurrency,
            write_tracker,
        })
    }

    async fn read_one(
        &self,
        id_type: &str,
        key: Key,
    ) -> StitcherResult<Option<(Key, (i64, Vec<u8>))>> {
        let key_str = key.as_str().to_owned();
        super::traced(
            "cql",
            metrics::store_get_seconds,
            |found, secs| {
                tracing::debug!(
                    id_type,
                    key = %key_str,
                    found = found.is_some(),
                    elapsed_ms = secs * 1e3,
                    "cql: SELECT version, state"
                );
            },
            async move {
                let result = self
                    .session
                    .execute_unpaged(&self.select, (id_type, key.as_str()))
                    .await
                    .change_context(StitcherError::Cql("execute select".to_string()))?;
                let rows = result
                    .into_rows_result()
                    .change_context(StitcherError::Cql("rows result".to_string()))?;
                let found = match &self.write_tracker {
                    Some(tracker) => rows
                        .rows::<(i64, Vec<u8>, Option<i64>)>()
                        .map_err(|e| {
                            error_stack::report!(StitcherError::Cql(format!("typed rows: {e}")))
                        })?
                        .next()
                        .transpose()
                        .map_err(|e| {
                            error_stack::report!(StitcherError::Cql(format!("decode row: {e}")))
                        })?
                        .map(|(version, state, token)| {
                            tracker.observe_read(&key, token);
                            (key, (version, state))
                        }),
                    None => rows
                        .rows::<(i64, Vec<u8>)>()
                        .map_err(|e| {
                            error_stack::report!(StitcherError::Cql(format!("typed rows: {e}")))
                        })?
                        .next()
                        .transpose()
                        .map_err(|e| {
                            error_stack::report!(StitcherError::Cql(format!("decode row: {e}")))
                        })?
                        .map(|(version, state)| (key, (version, state))),
                };
                Ok(found)
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Store for CqlStore {
    async fn get_many(
        &self,
        id_type: &str,
        keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        let pairs: Vec<(Key, (i64, Vec<u8>))> = futures::stream::iter(keys.iter().cloned())
            .map(|key| async move { self.read_one(id_type, key).await })
            .buffer_unordered(self.read_concurrency)
            .try_collect::<Vec<Option<(Key, (i64, Vec<u8>))>>>()
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(pairs.into_iter().collect())
    }

    async fn put(&self, id_type: &str, key: &Key, version: i64, blob: &[u8]) -> StitcherResult<()> {
        super::traced(
            "cql",
            metrics::store_put_seconds,
            |&(), secs| {
                tracing::debug!(
                    id_type,
                    key = %key.as_str(),
                    version,
                    blob_bytes = blob.len(),
                    elapsed_ms = secs * 1e3,
                    "cql: INSERT (id, id_type, version, state)"
                );
            },
            async {
                match &self.write_tracker {
                    Some(tracker) => {
                        let token = new_write_token();
                        self.session
                            .execute_unpaged(
                                &self.insert,
                                (key.as_str(), id_type, version, blob, token),
                            )
                            .await
                            .change_context(StitcherError::Cql("execute insert".to_string()))?;
                        tracker.note_write(key, token);
                    }
                    None => {
                        self.session
                            .execute_unpaged(&self.insert, (key.as_str(), id_type, version, blob))
                            .await
                            .change_context(StitcherError::Cql("execute insert".to_string()))?;
                    }
                }
                Ok(())
            },
        )
        .await
    }

    async fn on_rebalance(&self, ev: &RebalanceEvent) -> StitcherResult<()> {
        // Mirror RocksDB's revoke wipe: drop stale write-tokens so a cleanly handed-off key is not
        // misread as a cross-writer conflict when it is later re-assigned back to this pod.
        if let (RebalanceEvent::Revoke(_), Some(tracker)) = (ev, &self.write_tracker) {
            tracker.seen_map().clear();
        }
        Ok(())
    }

    async fn cleanup(&self) -> StitcherResult<()> {
        Ok(())
    }
}
