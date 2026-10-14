//! librdkafka preserves per-partition order, so `-1` precedes `+1` per key.

use std::time::{Duration, Instant};

use error_stack::ResultExt;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};

use crate::{
    config,
    errors::{StitcherError, StitcherResult},
    metrics,
    processor::OutMsg,
};

pub struct KafkaProducer {
    producer: FutureProducer,
    dlq_topic: String,
    delivery_timeout: Duration,
}

impl KafkaProducer {
    /// A failed delivery fails the call so the batch isn't committed.
    pub async fn send_all(&self, msgs: &[OutMsg]) -> StitcherResult<()> {
        let started = Instant::now();
        let mut pending = Vec::with_capacity(msgs.len());
        for msg in msgs {
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "event_id",
                    value: Some(&msg.event_id),
                })
                .insert(Header {
                    key: "correlation_id",
                    value: Some(&msg.correlation_id),
                })
                .insert(Header {
                    key: "state_version",
                    value: Some(&msg.state_version.to_string()),
                })
                .insert(Header {
                    key: "produced_at",
                    value: Some(&msg.produced_at.to_string()),
                })
                .insert(Header {
                    key: "causation_topic",
                    value: Some(&msg.causation.topic),
                })
                .insert(Header {
                    key: "causation_partition",
                    value: Some(&msg.causation.partition.to_string()),
                })
                .insert(Header {
                    key: "causation_offset",
                    value: Some(&msg.causation.offset.to_string()),
                });
            let record = FutureRecord::to(&msg.topic)
                .key(&msg.key)
                .payload(&msg.payload)
                .headers(headers);
            let delivery = self.producer.send_result(record).map_err(|(e, _)| {
                error_stack::report!(StitcherError::Kafka(format!("enqueue {}: {e}", msg.topic)))
            })?;
            pending.push((msg.topic.clone(), delivery));
        }
        for (topic, delivery) in pending {
            tokio::time::timeout(self.delivery_timeout, delivery)
                .await
                .map_err(|_| {
                    error_stack::report!(StitcherError::Kafka(format!(
                        "deliver {topic}: timed out after {:?}",
                        self.delivery_timeout
                    )))
                })?
                .change_context(StitcherError::Kafka("delivery channel".to_string()))?
                .map_err(|(e, _msg)| {
                    error_stack::report!(StitcherError::Kafka(format!("deliver {topic}: {e}")))
                })?;
            metrics::messages_produced(&topic);
        }
        metrics::produce_seconds(started.elapsed().as_secs_f64());
        Ok(())
    }

    /// Payload rides on the record; never logged.
    pub async fn send_dlq(
        &self,
        source_topic: &str,
        partition: i32,
        offset: i64,
        error: &str,
        raw: &[u8],
    ) -> StitcherResult<()> {
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "error",
                value: Some(error),
            })
            .insert(Header {
                key: "source_topic",
                value: Some(source_topic),
            })
            .insert(Header {
                key: "partition",
                value: Some(&partition.to_string()),
            })
            .insert(Header {
                key: "offset",
                value: Some(&offset.to_string()),
            })
            .insert(Header {
                key: "ts",
                value: Some(&crate::util::now_secs().to_string()),
            });
        let dlq_key = format!("{source_topic}:{partition}:{offset}");
        let record = FutureRecord::to(&self.dlq_topic)
            .key(&dlq_key)
            .payload(raw)
            .headers(headers);
        let delivery = self.producer.send_result(record).map_err(|(e, _)| {
            error_stack::report!(StitcherError::Kafka(format!("dlq enqueue: {e}")))
        })?;
        delivery
            .await
            .change_context(StitcherError::Kafka("dlq delivery channel".to_string()))?
            .map_err(|(e, _)| {
                error_stack::report!(StitcherError::Kafka(format!("dlq deliver: {e}")))
            })?;
        metrics::dlq_produced("malformed");
        tracing::warn!(
            error,
            source_topic,
            partition,
            offset,
            "malformed record routed to DLQ"
        );
        Ok(())
    }

    pub fn flush(&self, timeout: Duration) -> StitcherResult<()> {
        self.producer
            .flush(timeout)
            .change_context(StitcherError::Kafka("flush".to_string()))?;
        Ok(())
    }
}

pub fn build(cfg: &config::Settings) -> StitcherResult<KafkaProducer> {
    let mut conf = ClientConfig::new();
    conf.set("bootstrap.servers", cfg.sink_kafka.brokers.join(","))
        .set("enable.idempotence", "true")
        .set("acks", "all");
    for (k, v) in cfg.sink_kafka.extra.iter() {
        conf.set(k, v);
    }
    let producer: FutureProducer = conf
        .create()
        .change_context(StitcherError::Kafka("create producer".to_string()))?;
    Ok(KafkaProducer {
        producer,
        dlq_topic: cfg.sink_kafka.dlq_topic.clone(),
        delivery_timeout: Duration::from_secs(cfg.sink_kafka.delivery_timeout_secs),
    })
}
