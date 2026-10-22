//! Manual post-persist commits (at-least-once); a revoke drains the in-flight batch, then
//! wipes the local cache.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use error_stack::ResultExt;
use rdkafka::{
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    topic_partition_list::TopicPartitionList,
    ClientConfig, ClientContext, Message, Offset,
};

use crate::{
    config,
    constants::{COMMIT_RETRIES, RESUME_POLL_INTERVAL},
    errors::{StitcherError, StitcherResult},
    metrics,
    store::{PartitionRef, RebalanceEvent, Store},
};

#[derive(Clone)]
pub struct RebalanceGuard {
    drained_flag: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
    drain_timeout: Duration,
}

impl RebalanceGuard {
    /// Token decrements on drop; `None` while a revoke drain is pending.
    #[must_use]
    pub fn begin_batch(&self) -> Option<BatchToken<'_>> {
        if self.drained_flag.load(Ordering::SeqCst) {
            return None;
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        // Re-check: a revoke may have landed between the check and the increment.
        if self.drained_flag.load(Ordering::SeqCst) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(BatchToken(self))
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.drained_flag.load(Ordering::SeqCst)
    }

    /// Park until a revoke's drain + reassign settles (`post_rebalance` clears the flag) so the
    /// pipeline can resume instead of exiting. Cold path (rebalances are rare), so a coarse poll
    /// beats entangling a `watch` with the `in_flight` SeqCst drain handshake.
    pub async fn wait_for_resume(&self) {
        while self.drained_flag.load(Ordering::SeqCst) {
            tokio::time::sleep(RESUME_POLL_INTERVAL).await;
        }
    }
}

/// RAII — early returns / dropped futures can't strand `in_flight > 0` and hang the drain.
#[must_use]
pub struct BatchToken<'a>(&'a RebalanceGuard);

impl Drop for BatchToken<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct StitcherCtx {
    store: Arc<dyn Store>,
    guard: RebalanceGuard,
}

impl ClientContext for StitcherCtx {
    fn stats(&self, statistics: rdkafka::statistics::Statistics) {
        for (topic_name, topic) in &statistics.topics {
            for (partition, part) in &topic.partitions {
                metrics::consumer_lag(topic_name, *partition, part.consumer_lag);
            }
        }
    }
}

impl ConsumerContext for StitcherCtx {
    /// Broker ack of the async commit — the call only queues it.
    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &TopicPartitionList,
    ) {
        let rendered = offsets
            .elements()
            .iter()
            .map(|e| {
                format!(
                    "{}:{}@{}",
                    e.topic(),
                    e.partition(),
                    e.offset().to_raw().unwrap_or(-1)
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        match result {
            Ok(()) => tracing::debug!(offsets = %rendered, "offsets committed (broker ack)"),
            Err(e) => {
                tracing::warn!(error = %e, offsets = %rendered, "offset commit rejected by broker")
            }
        }
    }
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(tpl) = rebalance {
            self.guard.drained_flag.store(true, Ordering::SeqCst);
            // wait (bounded) for the in-flight batch, then wipe
            let started = Instant::now();
            while self.guard.in_flight.load(Ordering::SeqCst) > 0
                && started.elapsed() < self.guard.drain_timeout
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            if started.elapsed() >= self.guard.drain_timeout {
                tracing::warn!(
                    secs = self.guard.drain_timeout.as_secs(),
                    "rebalance drain timed out; wiping revoked column-family contents anyway"
                );
            }
            let parts = tpl_parts(tpl);
            if let Err(e) =
                futures::executor::block_on(self.store.on_rebalance(&RebalanceEvent::Revoke(parts)))
            {
                tracing::error!(error = ?e, "failed to wipe revoked column-family contents");
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                let parts = tpl_parts(tpl);
                if let Err(e) = futures::executor::block_on(
                    self.store.on_rebalance(&RebalanceEvent::Assign(parts)),
                ) {
                    tracing::error!(error = ?e, "store rebalance (assign) hook failed");
                }
                self.guard.drained_flag.store(false, Ordering::SeqCst);
            }
            Rebalance::Revoke(tpl) => {
                tracing::info!(partitions = tpl.count(), "partitions revoked");
            }
            Rebalance::Error(err) => {
                tracing::warn!(error = %err, "rebalance error");
                self.guard.drained_flag.store(false, Ordering::SeqCst);
            }
        }
    }
}

fn tpl_parts(tpl: &TopicPartitionList) -> Vec<PartitionRef> {
    tpl.elements()
        .iter()
        .map(|e| PartitionRef {
            topic: e.topic().to_string(),
            partition: e.partition(),
        })
        .collect()
}

pub struct KafkaConsumer {
    consumer: StreamConsumer<StitcherCtx>,
    guard: RebalanceGuard,
}

impl KafkaConsumer {
    pub fn stream(
        &self,
    ) -> impl futures::Stream<Item = rdkafka::error::KafkaResult<rdkafka::message::BorrowedMessage<'_>>>
    {
        self.consumer.stream()
    }

    #[must_use]
    pub fn guard(&self) -> RebalanceGuard {
        self.guard.clone()
    }

    /// Offsets are never stored implicitly, so they come only from finished work. Async;
    /// retry ×3.
    pub async fn commit(&self, watermarks: &[PartitionWatermark]) -> StitcherResult<()> {
        if watermarks.is_empty() {
            return Ok(()); // nothing processed in this batch
        }
        let mut offsets_to_commit = TopicPartitionList::with_capacity(watermarks.len());
        for (topic, partition, next_offset) in watermarks {
            offsets_to_commit
                .add_partition_offset(topic, *partition, Offset::from_raw(*next_offset))
                .change_context(StitcherError::Kafka("build commit watermark".to_string()))?;
        }
        let mut last_err: Option<String> = None;
        for attempt in 1..=COMMIT_RETRIES {
            match self
                .consumer
                .commit(&offsets_to_commit, rdkafka::consumer::CommitMode::Async)
            {
                Ok(()) => {
                    tracing::debug!(partitions = watermarks.len(), "offset commit queued");
                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    tracing::warn!(attempt, error = %e, "offset commit failed; retrying");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(error_stack::report!(StitcherError::Kafka(format!(
            "offset commit failed after {COMMIT_RETRIES} attempts: {}",
            last_err.unwrap_or_default()
        ))))
    }
}

/// `(topic, partition, next_offset_to_consume)`.
pub type PartitionWatermark = (String, i32, i64);

/// Max offset + 1 per (topic, partition); error records don't advance it (they replay).
#[must_use]
pub fn batch_watermarks(
    records: &[rdkafka::error::KafkaResult<rdkafka::message::BorrowedMessage<'_>>],
) -> Vec<PartitionWatermark> {
    let mut next_offset_by_partition: std::collections::BTreeMap<(String, i32), i64> =
        std::collections::BTreeMap::new();
    for msg in records.iter().flatten() {
        next_offset_by_partition
            .entry((msg.topic().to_string(), msg.partition()))
            .and_modify(|next| *next = (*next).max(msg.offset() + 1))
            .or_insert(msg.offset() + 1);
    }
    next_offset_by_partition
        .into_iter()
        .map(|((topic, partition), next_offset)| (topic, partition, next_offset))
        .collect()
}

pub fn build(cfg: &config::Settings, store: Arc<dyn Store>) -> StitcherResult<KafkaConsumer> {
    let guard = RebalanceGuard {
        drained_flag: Arc::new(AtomicBool::new(false)),
        in_flight: Arc::new(AtomicUsize::new(0)),
        drain_timeout: Duration::from_secs(cfg.rebalance_drain_secs),
    };
    let context = StitcherCtx {
        store,
        guard: guard.clone(),
    };

    let mut conf = ClientConfig::new();
    // dry-run: fresh random group so the tap never disturbs real offsets
    let group_id = if cfg.debug.dry_run {
        format!(
            "{}-inspect-{}",
            cfg.source_kafka.consumer_group,
            crate::util::now_nanos().max(0)
        )
    } else {
        cfg.source_kafka.consumer_group.clone()
    };
    conf.set("bootstrap.servers", cfg.source_kafka.brokers.join(","))
        .set("group.id", &group_id)
        // at-least-once protocol knobs — explicit watermark commits, no implicit offset
        // storage, no partition-EOF errors
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", &cfg.source_kafka.auto_offset_reset)
        .set(
            "statistics.interval.ms",
            cfg.source_kafka.statistics_interval_ms.to_string(),
        );
    for (k, v) in cfg.source_kafka.extra.iter() {
        conf.set(k, v);
    }

    let consumer: StreamConsumer<StitcherCtx> = conf
        .create_with_context(context)
        .change_context(StitcherError::Kafka("create consumer".to_string()))?;

    let topics: Vec<&str> = cfg.source_kafka.topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topics)
        .change_context(StitcherError::Kafka(format!(
            "subscribe {} topics",
            topics.len()
        )))?;

    Ok(KafkaConsumer { consumer, guard })
}

#[derive(Clone, Debug)]
pub struct OwnedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

impl OwnedRecord {
    /// `None` payload → empty bytes (routed to DLQ).
    #[must_use]
    pub fn from_borrowed(msg: &rdkafka::message::BorrowedMessage<'_>) -> Self {
        Self {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis(),
            key: msg.key().map(<[u8]>::to_vec),
            payload: msg.payload().map_or_else(Vec::new, <[u8]>::to_vec),
        }
    }
}
