//! Prometheus statics scraped via `/metrics`; a construction failure logs once and
//! disables that instrument.

use std::sync::LazyLock;

static REGISTRY: LazyLock<prometheus::Registry> = LazyLock::new(prometheus::Registry::new);

macro_rules! counter_vec {
    ($name:ident, $help:literal, $labels:expr) => {
        static $name: LazyLock<Option<prometheus::IntCounterVec>> = LazyLock::new(|| {
            match prometheus::IntCounterVec::new(
                prometheus::opts!(stringify!($name).to_ascii_lowercase(), $help),
                $labels,
            ) {
                Ok(vec) => {
                    let _ = REGISTRY.register(Box::new(vec.clone()));
                    Some(vec)
                }
                Err(error) => {
            tracing::error!(%error, "metric construction failed");
                    None
                }
            }
        });
    };
}

macro_rules! histogram_vec {
    ($name:ident, $help:literal, $labels:expr) => {
        static $name: LazyLock<Option<prometheus::HistogramVec>> = LazyLock::new(|| {
            match prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(stringify!($name).to_ascii_lowercase(), $help)
                    .buckets(vec![
                        0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0,
                    ]),
                $labels,
            ) {
                Ok(vec) => {
                    let _ = REGISTRY.register(Box::new(vec.clone()));
                    Some(vec)
                }
                Err(error) => {
            tracing::error!(%error, "metric construction failed");
                    None
                }
            }
        });
    };
}

counter_vec!(
    MESSAGES_CONSUMED_TOTAL,
    "Records consumed from source topics",
    &["topic"]
);
counter_vec!(MESSAGES_DECODED_TOTAL, "Records decoded into states", &[]);
counter_vec!(
    MESSAGES_FILTERED_TOTAL,
    "Records filtered by design",
    &["reason"]
);
counter_vec!(
    MESSAGES_SKIPPED_TOTAL,
    "Malformed records routed to the DLQ",
    &["reason"]
);
counter_vec!(
    DLQ_PRODUCED_TOTAL,
    "Records written to the DLQ",
    &["reason"]
);
counter_vec!(
    STATES_MERGED_TOTAL,
    "States merged against stored values",
    &[]
);
counter_vec!(
    ENRICHMENT_RELOAD_REJECTED_TOTAL,
    "Enrichment reloads rejected (not an object, or dropped a table present at boot); previous map kept",
    &[]
);
counter_vec!(
    MESSAGES_PRODUCED_TOTAL,
    "Records produced to sink topics",
    &["topic"]
);
counter_vec!(ERRORS_TOTAL, "Pipeline errors by stage", &["stage"]);
counter_vec!(BATCHES_TOTAL, "Batches processed", &[]);
counter_vec!(
    CROSS_WRITER_CONFLICT_TOTAL,
    "Keys read back carrying another instance's write-token (cross-writer / co-partitioning violation)",
    &[]
);
counter_vec!(
    CROSS_PARTITION_KEY_TOTAL,
    "Same session key folded from more than one source partition within a batch",
    &[]
);
counter_vec!(
    PARTITION_KEY_MISMATCH_TOTAL,
    "Records whose Kafka key differs from the computed session key (producer not co-partitioning by session key)",
    &[]
);
histogram_vec!(BATCH_SIZE, "Records per batch", &[]);
histogram_vec!(STORE_GET_SECONDS, "Store read latency", &["backend"]);
histogram_vec!(STORE_PUT_SECONDS, "Store write latency", &["backend"]);
histogram_vec!(PRODUCE_SECONDS, "Sink produce latency", &[]);
histogram_vec!(BATCH_PROCESS_SECONDS, "Whole-batch wall time", &[]);
histogram_vec!(END_TO_END_LAG_SECONDS, "Consume-to-persist latency", &[]);

static ASSIGNED_PARTITIONS: LazyLock<Option<prometheus::IntGauge>> = LazyLock::new(|| {
    match prometheus::IntGauge::new("assigned_partitions", "Currently assigned partitions") {
        Ok(gauge) => {
            let _ = REGISTRY.register(Box::new(gauge.clone()));
            Some(gauge)
        }
        Err(error) => {
            tracing::error!(%error, "metric construction failed");
            None
        }
    }
});

static CONSUMER_LAG: LazyLock<Option<prometheus::IntGaugeVec>> = LazyLock::new(|| {
    match prometheus::IntGaugeVec::new(
        prometheus::opts!("consumer_lag", "Per-partition consumer lag"),
        &["topic", "partition"],
    ) {
        Ok(vec) => {
            let _ = REGISTRY.register(Box::new(vec.clone()));
            Some(vec)
        }
        Err(error) => {
            tracing::error!(%error, "metric construction failed");
            None
        }
    }
});

// ---------------------------------------------------------------------------
// recording API (stable call sites; statics stay an implementation detail)
// ---------------------------------------------------------------------------

pub fn messages_consumed(topic: &str) {
    if let Some(counter) = &*MESSAGES_CONSUMED_TOTAL {
        counter.with_label_values(&[topic]).inc();
    }
}

pub fn messages_decoded() {
    if let Some(counter) = &*MESSAGES_DECODED_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn messages_filtered(reason: &'static str) {
    if let Some(counter) = &*MESSAGES_FILTERED_TOTAL {
        counter.with_label_values(&[reason]).inc();
    }
}

pub fn messages_skipped(reason: &'static str) {
    if let Some(counter) = &*MESSAGES_SKIPPED_TOTAL {
        counter.with_label_values(&[reason]).inc();
    }
}

pub fn dlq_produced(reason: &'static str) {
    if let Some(counter) = &*DLQ_PRODUCED_TOTAL {
        counter.with_label_values(&[reason]).inc();
    }
}

pub fn states_merged() {
    if let Some(counter) = &*STATES_MERGED_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn enrichment_reload_rejected() {
    if let Some(counter) = &*ENRICHMENT_RELOAD_REJECTED_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn messages_produced(topic: &str) {
    if let Some(counter) = &*MESSAGES_PRODUCED_TOTAL {
        counter.with_label_values(&[topic]).inc();
    }
}

pub fn errors_total(stage: &'static str) {
    if let Some(counter) = &*ERRORS_TOTAL {
        counter.with_label_values(&[stage]).inc();
    }
}

pub fn batches() {
    if let Some(counter) = &*BATCHES_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn cross_writer_conflict() {
    if let Some(counter) = &*CROSS_WRITER_CONFLICT_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn cross_partition_key() {
    if let Some(counter) = &*CROSS_PARTITION_KEY_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn partition_key_mismatch() {
    if let Some(counter) = &*PARTITION_KEY_MISMATCH_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

pub fn batch_size(record_count: usize) {
    if let Some(histogram) = &*BATCH_SIZE {
        #[allow(clippy::as_conversions)] // usize count → f64 observation; loss irrelevant
        let observation = record_count as f64;
        histogram.with_label_values(&[]).observe(observation);
    }
}

pub fn store_get_seconds(backend: &'static str, secs: f64) {
    if let Some(histogram) = &*STORE_GET_SECONDS {
        histogram.with_label_values(&[backend]).observe(secs);
    }
}

pub fn store_put_seconds(backend: &'static str, secs: f64) {
    if let Some(histogram) = &*STORE_PUT_SECONDS {
        histogram.with_label_values(&[backend]).observe(secs);
    }
}

pub fn produce_seconds(secs: f64) {
    if let Some(histogram) = &*PRODUCE_SECONDS {
        histogram.with_label_values(&[]).observe(secs);
    }
}

pub fn batch_process_seconds(secs: f64) {
    if let Some(histogram) = &*BATCH_PROCESS_SECONDS {
        histogram.with_label_values(&[]).observe(secs);
    }
}

pub fn end_to_end_lag_seconds(secs: f64) {
    if let Some(histogram) = &*END_TO_END_LAG_SECONDS {
        histogram.with_label_values(&[]).observe(secs);
    }
}

pub fn assigned_partitions(n: usize) {
    if let Some(gauge) = &*ASSIGNED_PARTITIONS {
        gauge.set(i64::try_from(n).unwrap_or(i64::MAX));
    }
}

pub fn consumer_lag(topic: &str, partition: i32, lag: i64) {
    if let Some(gauge) = &*CONSUMER_LAG {
        gauge
            .with_label_values(&[topic, &partition.to_string()])
            .set(lag);
    }
}

pub(crate) fn registry() -> &'static prometheus::Registry {
    &REGISTRY
}
