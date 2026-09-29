//! Metrics: `prometheus` statics scraped via the actix-web `/metrics` endpoint (PLAN #22).
//! Instruments are `LazyLock<Option<_>>` self-registering statics — a construction
//! failure logs once and disables that instrument (no panic path). The scrape endpoint
//! itself lives in [`crate::server`], which gathers this crate's [`registry`].

use std::sync::LazyLock;

/// The registry every instrument registers itself into (lazily, on first use).
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
    MESSAGES_PRODUCED_TOTAL,
    "Records produced to sink topics",
    &["topic"]
);
counter_vec!(ERRORS_TOTAL, "Pipeline errors by stage", &["stage"]);
counter_vec!(BATCHES_TOTAL, "Batches processed", &[]);
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

/// One record consumed.
pub fn messages_consumed(topic: &str) {
    if let Some(counter) = &*MESSAGES_CONSUMED_TOTAL {
        counter.with_label_values(&[topic]).inc();
    }
}

/// One record decoded into a state.
pub fn messages_decoded() {
    if let Some(counter) = &*MESSAGES_DECODED_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

/// Filtered by design (valid record, wrong `log_type`/ids/tenant).
pub fn messages_filtered(reason: &'static str) {
    if let Some(counter) = &*MESSAGES_FILTERED_TOTAL {
        counter.with_label_values(&[reason]).inc();
    }
}

/// Malformed record routed to the DLQ.
pub fn messages_skipped(reason: &'static str) {
    if let Some(counter) = &*MESSAGES_SKIPPED_TOTAL {
        counter.with_label_values(&[reason]).inc();
    }
}

/// One DLQ record produced.
pub fn dlq_produced(reason: &'static str) {
    if let Some(counter) = &*DLQ_PRODUCED_TOTAL {
        counter.with_label_values(&[reason]).inc();
    }
}

/// One stored+incoming state pair merged.
pub fn states_merged() {
    if let Some(counter) = &*STATES_MERGED_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

/// One record produced to a sink topic.
pub fn messages_produced(topic: &str) {
    if let Some(counter) = &*MESSAGES_PRODUCED_TOTAL {
        counter.with_label_values(&[topic]).inc();
    }
}

/// A pipeline error.
pub fn errors_total(stage: &'static str) {
    if let Some(counter) = &*ERRORS_TOTAL {
        counter.with_label_values(&[stage]).inc();
    }
}

/// A batch processed.
pub fn batches() {
    if let Some(counter) = &*BATCHES_TOTAL {
        counter.with_label_values(&[]).inc();
    }
}

/// Records in the batch.
pub fn batch_size(record_count: usize) {
    if let Some(histogram) = &*BATCH_SIZE {
        #[allow(clippy::as_conversions)] // usize count → f64 observation; loss irrelevant
        let observation = record_count as f64;
        histogram.with_label_values(&[]).observe(observation);
    }
}

/// Store read latency.
pub fn store_get_seconds(backend: &'static str, secs: f64) {
    if let Some(histogram) = &*STORE_GET_SECONDS {
        histogram.with_label_values(&[backend]).observe(secs);
    }
}

/// Store write latency.
pub fn store_put_seconds(backend: &'static str, secs: f64) {
    if let Some(histogram) = &*STORE_PUT_SECONDS {
        histogram.with_label_values(&[backend]).observe(secs);
    }
}

/// Produce latency.
pub fn produce_seconds(secs: f64) {
    if let Some(histogram) = &*PRODUCE_SECONDS {
        histogram.with_label_values(&[]).observe(secs);
    }
}

/// Wall time of one batch.
pub fn batch_process_seconds(secs: f64) {
    if let Some(histogram) = &*BATCH_PROCESS_SECONDS {
        histogram.with_label_values(&[]).observe(secs);
    }
}

/// Consume → persist latency.
pub fn end_to_end_lag_seconds(secs: f64) {
    if let Some(histogram) = &*END_TO_END_LAG_SECONDS {
        histogram.with_label_values(&[]).observe(secs);
    }
}

/// Currently assigned partitions.
pub fn assigned_partitions(n: usize) {
    if let Some(gauge) = &*ASSIGNED_PARTITIONS {
        gauge.set(i64::try_from(n).unwrap_or(i64::MAX));
    }
}

/// Per-partition consumer lag (rdkafka reports `-1` while unknown).
pub fn consumer_lag(topic: &str, partition: i32, lag: i64) {
    if let Some(gauge) = &*CONSUMER_LAG {
        gauge
            .with_label_values(&[topic, &partition.to_string()])
            .set(lag);
    }
}

/// Borrow the shared prometheus registry (used by the HTTP server to gather metrics).
pub(crate) fn registry() -> &'static prometheus::Registry {
    &REGISTRY
}
