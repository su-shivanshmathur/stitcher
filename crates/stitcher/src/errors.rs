//! Error types for the whole framework (PLAN §17: `thiserror` + `error-stack`, NOT anyhow).

/// Top-level error categories. Call sites wrap these in an [`error_stack::Report`] via
/// `.change_context(...)` and attach printable context with `.attach_printable_lazy(...)`.
#[derive(Debug, thiserror::Error)]
pub enum StitcherError {
    /// Configuration load/validation failure.
    #[error("config: {0}")]
    Config(String),

    /// Kafka client failure (consumer, producer, offset commit, DLQ).
    #[error("kafka: {0}")]
    Kafka(String),

    /// CQL backend failure (`ScyllaDB` / Cassandra).
    #[error("cql: {0}")]
    Cql(String),

    /// Local `RocksDB` cache failure.
    #[error("rocksdb: {0}")]
    Rocks(String),

    /// State framing/serde failure.
    #[error("codec: {0}")]
    Codec(String),

    /// A store backend was selected that is not implemented (e.g. the `DynamoDB` stub).
    #[error("store backend unsupported: {0}")]
    Unsupported(&'static str),

    /// Enrichment join-map load/reload failure.
    #[error("enrichment: {0}")]
    Enrichment(String),

    /// Telemetry (logging/metrics pipeline) initialization failure.
    #[error("telemetry: {0}")]
    Telemetry(String),

    /// Graceful shutdown did not complete within the configured grace period.
    #[error("shutdown timed out")]
    ShutdownTimeout,
}

/// Convenience `Report` alias used across the codebase.
pub type StitcherResult<T> = error_stack::Result<T, StitcherError>;
