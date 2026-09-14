//! Kafka consumer with rebalance-aware context: manual commits after persist
//! (at-least-once); a revoke drains the in-flight batch, then drops revoked RocksDB CFs;
//! rdkafka statistics feed the `consumer_lag` gauges.

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
    ClientConfig, ClientContext, Message,
};

use crate::{
    config,
    errors::{StitcherError, StitcherResult},
    metrics,
    store::{PartitionRef, RebalanceEvent, Store},
};

/// Shared pipeline ↔ rebalance-callback coordination.
#[derive(Clone)]
pub struct RebalanceGuard {
    drained_flag: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
    drain_timeout: Duration,
}

impl RebalanceGuard {
    /// Mark a batch as in-flight (returns `false` while a revoke drain is pending — caller
    /// must not start new work in that window).
    #[must_use]
    pub fn begin_batch(&self) -> bool {
        if self.drained_flag.load(Ordering::SeqCst) {
            return false;
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        // Re-check: a revoke may have landed between the check and the increment.
        if self.drained_flag.load(Ordering::SeqCst) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// Mark a batch finished (store persisted + offsets queued for commit).
    pub fn end_batch(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    /// True while a revoke is waiting for the pipeline to drain.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.drained_flag.load(Ordering::SeqCst)
    }
}

/// rdkafka client context wiring stats + rebalance into the store.
pub struct StitcherCtx {
    store: Arc<dyn Store>,
    guard: RebalanceGuard,
}

impl ClientContext for StitcherCtx {
    /// rdkafka ships parsed statistics (`statistics.interval.ms` set at build).
    fn stats(&self, statistics: rdkafka::statistics::Statistics) {
        for (topic_name, topic) in &statistics.topics {
            for (partition, part) in &topic.partitions {
                metrics::consumer_lag(topic_name, *partition, part.consumer_lag);
            }
        }
    }
}

impl ConsumerContext for StitcherCtx {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(tpl) = rebalance {
            self.guard.drained_flag.store(true, Ordering::SeqCst);
            // Wait (bounded) for the in-flight batch to finish, then drop revoked CFs.
            let started = Instant::now();
            while self.guard.in_flight.load(Ordering::SeqCst) > 0
                && started.elapsed() < self.guard.drain_timeout
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            if started.elapsed() >= self.guard.drain_timeout {
                tracing::warn!(
                    secs = self.guard.drain_timeout.as_secs(),
                    "rebalance drain timed out; dropping revoked CFs anyway"
                );
            }
            let parts = tpl_parts(tpl);
            if let Err(e) =
                futures::executor::block_on(self.store.on_rebalance(&RebalanceEvent::Revoke(parts)))
            {
                tracing::error!(error = ?e, "failed to drop revoked column families");
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
                    tracing::error!(error = ?e, "failed to create assigned column families");
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

fn tpl_parts(tpl: &rdkafka::TopicPartitionList) -> Vec<PartitionRef> {
    tpl.elements()
        .iter()
        .map(|e| PartitionRef {
            topic: e.topic().to_string(),
            partition: e.partition(),
        })
        .collect()
}

/// The pipeline-facing consumer handle.
pub struct KafkaConsumer {
    consumer: StreamConsumer<StitcherCtx>,
    guard: RebalanceGuard,
}

impl KafkaConsumer {
    /// Subscribed stream of messages.
    pub fn stream(
        &self,
    ) -> impl futures::Stream<Item = rdkafka::error::KafkaResult<rdkafka::message::BorrowedMessage<'_>>>
    {
        self.consumer.stream()
    }

    /// Coordination handle for the pipeline loop.
    #[must_use]
    pub fn guard(&self) -> RebalanceGuard {
        self.guard.clone()
    }

    /// Async commit of current positions, retry ×3.
    pub async fn commit(&self) -> StitcherResult<()> {
        const RETRIES: usize = 3;
        let mut last_err: Option<String> = None;
        for attempt in 1..=RETRIES {
            match self
                .consumer
                .commit_consumer_state(rdkafka::consumer::CommitMode::Async)
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(e.to_string());
                    tracing::warn!(attempt, error = %e, "offset commit failed; retrying");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(error_stack::report!(StitcherError::Kafka(format!(
            "offset commit failed after {RETRIES} attempts: {}",
            last_err.unwrap_or_default()
        ))))
    }
}

/// Build + subscribe the consumer.
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
    // Dry-run (inspect tap, INSPECT.md): a fresh random group per run so the tap never
    // disturbs the real pipeline's offsets and can be freely re-run.
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
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("enable.auto.offset.store", "false")
        // fresh consumer groups start from the beginning of the log;
        // overridable via source_kafka.extra
        .set("auto.offset.reset", "earliest")
        .set("statistics.interval.ms", "10000");
    for (k, v) in &cfg.source_kafka.extra {
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

/// One owned message: payload copied across the await boundary (PLAN §The crux).
#[derive(Clone, Debug)]
pub struct OwnedRecord {
    /// Source topic.
    pub topic: String,
    /// Source partition.
    pub partition: i32,
    /// Source offset.
    pub offset: i64,
    /// Kafka timestamp (ms, when present) for backlog/lag metrics.
    pub timestamp_ms: Option<i64>,
    /// Owned payload bytes.
    pub payload: Vec<u8>,
}

impl OwnedRecord {
    /// Copy out of a borrowed message; `None` payload → empty bytes (will be routed to DLQ).
    #[must_use]
    pub fn from_borrowed(msg: &rdkafka::message::BorrowedMessage<'_>) -> Self {
        Self {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis(),
            payload: msg.payload().map_or_else(Vec::new, <[u8]>::to_vec),
        }
    }
}
