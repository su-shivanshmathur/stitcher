//! The streaming core: per batch — decode/fold by key → merge with stored state → emit
//! `-1`/`+1` deltas → retention-filter → produce → persist → **commit** (at-least-once).
//! Shutdown drains in place: SIGTERM ends the source; the in-flight batch still finishes.

use std::{collections::HashMap, sync::Arc, time::Instant};

use futures::{Stream, StreamExt, TryStreamExt};
use rdkafka::message::Message;

use error_stack::ResultExt;

use crate::{
    config::Settings,
    enrichment::Enrichment,
    errors::{StitcherError, StitcherResult},
    inspect, kafka,
    kafka::{consumer::OwnedRecord, producer::KafkaProducer},
    merge::Merge,
    metrics,
    processor::{Key, OutMsg, Processor, Sign},
    projection::{Projection, ProjectionContext},
    store::Store,
    util,
};

/// One consumed batch straight off the rdkafka stream (payloads not yet owned).
type RawBatch<'a> = Vec<rdkafka::error::KafkaResult<rdkafka::message::BorrowedMessage<'a>>>;

/// Total `Kafka → store → Kafka` pipeline for a stateful [`Processor`], driving each state
/// change through `projection` (the output seam — [`crate::projection::StateLogger`] by
/// default, or a plugged-in consumer such as the `transformer` crate).
pub async fn run<P: Processor>(
    proc: P,
    projection: Box<dyn Projection>,
    settings: Settings,
) -> StitcherResult<()> {
    // Dry-run (inspect tap) survives an unreachable store: old state just reads as absent.
    let store: Arc<dyn Store> = if settings.debug.dry_run {
        match crate::store::build_store(&settings, proc.id_type()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "inspect dry-run: store unavailable; continuing without stored state"
                );
                Arc::new(NoStore)
            }
        }
    } else {
        crate::store::build_store(&settings, proc.id_type()).await?
    };
    // Ops HTTP server reuses the store connection just built (/metrics, /health, /state).
    crate::server::spawn(
        &settings.server.host,
        settings.server.port,
        Arc::clone(&store),
        proc.id_type(),
    )?;
    let consumer = kafka::consumer::build(&settings, Arc::clone(&store))?;
    let producer = kafka::producer::build(&settings)?;
    let enrichment = Enrichment::spawn_reloader(&settings.enrichment)?;
    let guard = consumer.guard();
    let inspector = inspect::Inspect::from_cfg(&settings.debug);
    let ctx = BatchCtx {
        producer: &producer,
        inspector: &inspector,
        projection: projection.as_ref(),
        enrichment: &enrichment,
        read_concurrency: settings.read_concurrency,
        dry_run: settings.debug.dry_run,
    };

    let (shutdown_req, batches) = spawn_source(&consumer, &settings);
    let mut batches = Box::pin(batches);

    let mut transport_failures: u32 = 0;

    while let Some(batch) = StreamExt::next(&mut batches).await {
        // `_batch` decrements `in_flight` when dropped — every exit path below
        // (early `?`, shutdown timeout, loop end) releases the rebalance drain.
        let Some(_batch) = guard.begin_batch() else {
            // Rebalance-revoke while a batch was in flight: the owned, unprocessed records
            // must NOT be skipped — a later batch's per-partition watermark would cover
            // them. The replay-safe action is to stop here — the unprocessed range is
            // re-consumed from the last committed watermark after restart/rejoin.
            tracing::warn!(
                "rebalance drain active; dropping uncommitted batch and stopping (replay-safe)"
            );
            break;
        };
        let started = Instant::now();
        // capture the commit watermark before the batch is moved into the work
        let watermarks = kafka::consumer::batch_watermarks(&batch);
        let work = Box::pin(process_batch(&proc, store.as_ref(), &ctx, batch));
        let result = match shutdown_grace(&settings, *shutdown_req.borrow()) {
            Some(grace) => {
                if let Ok(r) = tokio::time::timeout(grace, work).await {
                    r
                } else {
                    return Err(error_stack::report!(StitcherError::ShutdownTimeout))
                        .attach_printable("batch exceeded shutdown_grace_secs");
                }
            }
            None => work.await,
        };
        let outcome = result?; // store/DLQ failure: no commit → at-least-once replay
                               // The batch's records are fully processed + persisted → committing is safe even
                               // when a transport error rode along (anything not yet consumed is unaffected).
                               // Dry-run (inspect tap) commits NOTHING — a re-runnable, non-destructive read.
        if !ctx.dry_run {
            consumer.commit(&watermarks).await?;
        }
        metrics::batch_process_seconds(started.elapsed().as_secs_f64());
        metrics::batches();

        if !on_transport_error(outcome.transport_error, &mut transport_failures).await {
            break;
        }
    }

    producer
        .flush(std::time::Duration::from_secs(10))
        .attach_printable("producer flush on shutdown")?;
    store.cleanup().await?;
    if ctx.dry_run {
        tracing::info!(
            "inspect dry-run complete: consumed, decoded, merged, printed — \
             nothing written, produced or committed"
        );
    } else {
        tracing::info!("shutdown complete: consumed drained, state persisted, offsets committed");
    }
    Ok(())
}


// ---------------------------------------------------------------------------
// shared plumbing
// ---------------------------------------------------------------------------

/// Signal task + consumed, signalled, batched source stream. Stops PULLING once
/// signalled; `chunks_timeout` still flushes the final partial batch (drain-in-place).
fn spawn_source<'a>(
    consumer: &'a kafka::consumer::KafkaConsumer,
    settings: &Settings,
) -> (
    tokio::sync::watch::Receiver<bool>,
    impl Stream<Item = RawBatch<'a>> + 'a,
) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    spawn_signal_task(shutdown_tx);
    let messages = consumer
        .stream()
        .take_until(Box::pin(wait_for_shutdown(shutdown_rx.clone())));
    let batches = tokio_stream::StreamExt::chunks_timeout(
        Box::pin(messages),
        settings.batch.count,
        std::time::Duration::from_millis(settings.batch.window_ms),
    );
    (shutdown_rx, batches)
}

/// Own the payloads (`BorrowedMessage` can't cross an await); transport errors are
/// reported (not thrown) so finished work still commits.
fn collect_records(batch: RawBatch<'_>) -> (Vec<OwnedRecord>, Option<String>) {
    let mut records = Vec::with_capacity(batch.len());
    let mut transport_err = None;
    for item in batch {
        match item {
            Ok(msg) => {
                metrics::messages_consumed(msg.topic());
                records.push(OwnedRecord::from_borrowed(&msg));
            }
            Err(e) => {
                metrics::errors_total("consume");
                transport_err.get_or_insert(e.to_string());
            }
        }
    }
    metrics::batch_size(records.len());
    (records, transport_err)
}

/// Consume → now latency of the oldest record in the batch (PLAN §Metrics).
fn oldest_lag_secs(records: &[OwnedRecord]) -> Option<f64> {
    let oldest_ms = records.iter().filter_map(|r| r.timestamp_ms).min()?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let lag_ms = now_ms.saturating_sub(u128::try_from(oldest_ms).unwrap_or(0));
    #[allow(clippy::as_conversions)] // u128ms→f64s: precision fine for a lag gauge
    let lag_secs = (lag_ms as f64) / 1000.0;
    Some(lag_secs)
}

/// Transport-error ladder: exponential backoff (2^failures secs, capped), reset on a
/// clean batch, graceful stop after MAX rounds. Returns `false` to stop consuming.
async fn on_transport_error(err: Option<String>, failures: &mut u32) -> bool {
    /// Bounded transport-error tolerance (PLAN §Failure modes).
    const MAX_TRANSPORT_FAILURES: u32 = 6;
    let Some(err) = err else {
        *failures = 0;
        return true;
    };
    *failures = failures.saturating_add(1);
    if *failures > MAX_TRANSPORT_FAILURES {
        tracing::error!(
            error = %err,
            failures = *failures,
            "transport errors persisted through backoff; shutting down gracefully"
        );
        return false;
    }
    let backoff = std::time::Duration::from_secs(1_u64 << (*failures).min(6));
    tracing::warn!(error = %err, ?backoff, "transport error; backing off");
    tokio::time::sleep(backoff).await;
    true
}

fn shutdown_grace(settings: &Settings, requested: bool) -> Option<std::time::Duration> {
    if requested {
        settings
            .shutdown_grace_secs
            .map(std::time::Duration::from_secs)
    } else {
        None
    }
}

async fn wait_for_shutdown(mut rx: tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            break; // sender dropped: exit the stream
        }
    }
    tracing::info!("shutdown signal received; draining in place");
}

fn spawn_signal_task(tx: tokio::sync::watch::Sender<bool>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            if let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! {
                    _ = term.recv() => {},
                    _ = tokio::signal::ctrl_c() => {},
                }
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx.send(true);
    });
}

// ---------------------------------------------------------------------------
// batch work
// ---------------------------------------------------------------------------

/// Per-run collaborators threaded into [`process_batch`] (keeps its signature small and
/// clippy-friendly).
struct BatchCtx<'a> {
    /// Sink producer (+ DLQ).
    producer: &'a KafkaProducer,
    /// Inspect tap.
    inspector: &'a inspect::Inspect,
    /// Output seam: each state change is projected here (default logs, transformer fans out).
    projection: &'a dyn Projection,
    /// Enrichment join map (handed to the projection via [`ProjectionContext`]).
    enrichment: &'a Enrichment,
    /// Store read fan-out width.
    read_concurrency: usize,
    /// Inspect dry-run: no DLQ, no produce, no persist (the caller skips the commit).
    dry_run: bool,
}

/// Serialize a state to a JSON value for the transform; a serialize failure (unreachable
/// for these types) logs and yields `Null` so the pipeline stays panic-free.
fn state_to_value<S: serde::Serialize>(state: &S) -> serde_json::Value {
    match serde_json::to_value(state) {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(%error, "transform: state serialize failed; skipping projections");
            serde_json::Value::Null
        }
    }
}

/// Outcome of one batch: processed work is persisted; a transport error is reported here
/// (not as `Err`) so the caller can still commit the finished work.
#[derive(Debug, Default)]
struct BatchOutcome {
    /// First Kafka transport error seen while consuming this batch, if any.
    transport_error: Option<String>,
}

/// One batch end-to-end (PLAN §Pipeline per batch); `ctx.dry_run` skips every side effect.
async fn process_batch<P: Processor>(
    proc: &P,
    store: &dyn Store,
    ctx: &BatchCtx<'_>,
    batch: RawBatch<'_>,
) -> StitcherResult<BatchOutcome> {
    // 1. own the payloads (BorrowedMessage can't cross an await)
    let (records, transport_err) = collect_records(batch);
    tracing::debug!(records = records.len(), "batch received");
    if let Some(lag_secs) = oldest_lag_secs(&records) {
        metrics::end_to_end_lag_seconds(lag_secs);
    }

    // 2. fold into per-key states (remove+insert: no clones in the hot fold)
    let mut states: HashMap<Key, P::State> = HashMap::new();
    for rec in &records {
        let decoded = proc.decode_with_key(&rec.payload);
        // (a) inspect: incoming record, with its key when decodable (INSPECT.md)
        ctx.inspector.incoming(
            &rec.topic,
            rec.partition,
            rec.offset,
            decoded.as_ref().map(|(k, _)| k.as_str()),
            &rec.payload,
        );
        match decoded {
            Some((key, state)) => {
                metrics::messages_decoded();
                tracing::debug!(
                    key = %key.as_str(),
                    topic = %rec.topic,
                    partition = rec.partition,
                    offset = rec.offset,
                    "record decoded"
                );
                match states.remove(&key) {
                    Some(old) => {
                        states.insert(key, old.merge(state));
                    }
                    None => {
                        states.insert(key, state);
                    }
                }
            }
            None => classify_and_route(ctx.producer, rec, ctx.dry_run).await?,
        }
    }

    if !states.is_empty() {
        // 3. stored states (concurrent remote reads inside the backend)
        let keys: Vec<Key> = states.keys().cloned().collect();
        tracing::debug!(keys = keys.len(), "reading stored states");
        let stored = store.get_many(proc.id_type(), &keys).await?;

        // 4. merge + encode deltas; 5. retention filter
        let now = util::now_secs();
        let mut msgs: Vec<OutMsg> = Vec::new();
        let mut states_to_persist: Vec<(Key, Vec<u8>)> = Vec::with_capacity(keys.len());
        let empty = P::State::default(); // inspect rendering of an absent old state (mempty)
        for (key, local) in states {
            let stored_state = match stored.get(&key) {
                Some((version, blob)) if *version == proc.state_version() => {
                    match proc.decode_stored(blob) {
                        Some(old) => Some(old),
                        None => {
                            metrics::errors_total("decode_state");
                            tracing::warn!("corrupt stored state; treating as absent");
                            None
                        }
                    }
                }
                Some((version, blob)) => proc.upcast(*version, blob), // older version: migrate, or (default) drop
                None => None,
            };
            let had_stored = stored_state.is_some();
            // (b) inspect: `old` is rendered BEFORE `merge` consumes it (INSPECT.md)
            let old_repr = ctx
                .inspector
                .state_enabled_for(&key)
                .then(|| match &stored_state {
                    Some(old) => ctx.inspector.render_state(old),
                    None => ctx.inspector.render_state(&empty),
                });
            let pctx = ProjectionContext {
                now_secs: now,
                enrichment: ctx.enrichment,
            };
            let merged = match stored_state {
                Some(old) => {
                    metrics::states_merged();
                    // -1 delta on the previously stored state
                    msgs.extend(ctx.projection.project(&state_to_value(&old), Sign::Minus, &pctx));
                    old.merge(local)
                }
                None => local,
            };
            if let Some(old_repr) = old_repr {
                ctx.inspector.emit_state(
                    &key,
                    proc.state_version(),
                    &old_repr,
                    &ctx.inspector.render_state(&merged),
                );
            }
            // +1 delta: serialize merged → Value once, reuse it for the persist blob
            let merged_value = state_to_value(&merged);
            msgs.extend(ctx.projection.project(&merged_value, Sign::Plus, &pctx));
            if ctx.dry_run {
                continue; // inspect tap: no persist
            }
            let blob = serde_json::to_vec(&merged_value)
                .change_context(StitcherError::Codec("serialize merged state".into()))?;
            tracing::debug!(key = %key.as_str(), had_stored, blob_bytes = blob.len(), "state persisted");
            states_to_persist.push((key, blob));
        }
        // retention is the projection's concern; produce everything it returned
        ctx.inspector.outgoing(&msgs);

        if !ctx.dry_run {
            // 6. produce first (consumers see deltas only if the state will also persist —
            //    but persist-failure aborts the batch pre-commit, so replay re-produces:
            //    at-least-once everywhere)
            tracing::debug!(messages = msgs.len(), "producing sink records");
            ctx.producer.send_all(&msgs).await?;

            // 7. dual-store persist (concurrent within the batch; concurrent dual-write
            //    inside Store::put)
            futures::stream::iter(
                states_to_persist
                    .into_iter()
                    .map(|(key, blob)| async move {
                        store
                            .put(proc.id_type(), &key, proc.state_version(), &blob)
                            .await
                    }),
            )
            .buffer_unordered(ctx.read_concurrency)
            .try_collect::<Vec<()>>()
            .await?;
        }
    }

    Ok(BatchOutcome {
        transport_error: transport_err,
    })
}

/// Filtered-by-design vs malformed: malformed → DLQ, both counted (in dry-run the DLQ
/// write is skipped but the record is still classified + counted).
async fn classify_and_route(
    producer: &KafkaProducer,
    rec: &OwnedRecord,
    dry_run: bool,
) -> StitcherResult<()> {
    let valid_json = !rec.payload.is_empty()
        && serde_json::from_slice::<serde::de::IgnoredAny>(&rec.payload).is_ok();
    if valid_json {
        metrics::messages_filtered("by_design");
        tracing::debug!(
            topic = %rec.topic,
            partition = rec.partition,
            offset = rec.offset,
            "record filtered by design (failed processor admission)"
        );
    } else {
        metrics::messages_skipped("malformed");
        if dry_run {
            tracing::warn!(
                topic = %rec.topic,
                partition = rec.partition,
                offset = rec.offset,
                "malformed record (dry-run: DLQ write skipped)"
            );
        } else {
            tracing::debug!(
                topic = %rec.topic,
                partition = rec.partition,
                offset = rec.offset,
                "malformed record routed to DLQ"
            );
            producer
                .send_dlq(
                    &rec.topic,
                    rec.partition,
                    rec.offset,
                    "malformed payload",
                    &rec.payload,
                )
                .await?; // commit is GATED on this write (PLAN §23)
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// no-op store (dry-run fallback + transformer rebalance hook)
// ---------------------------------------------------------------------------

struct NoStore;

#[async_trait::async_trait]
impl Store for NoStore {
    async fn get_many(
        &self,
        _id_type: &str,
        _keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        Ok(HashMap::new())
    }
    async fn put(
        &self,
        _id_type: &str,
        _key: &Key,
        _version: i64,
        _blob: &[u8],
    ) -> StitcherResult<()> {
        Ok(())
    }
    async fn on_rebalance(&self, _ev: &crate::store::RebalanceEvent) -> StitcherResult<()> {
        Ok(())
    }
    async fn cleanup(&self) -> StitcherResult<()> {
        Ok(())
    }
}
