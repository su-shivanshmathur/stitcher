//! Error types for the whole framework (`thiserror` + `error-stack`).

#[derive(Debug, thiserror::Error)]
pub enum StitcherError {
    #[error("config: {0}")]
    Config(String),

    #[error("kafka: {0}")]
    Kafka(String),

    #[error("cql: {0}")]
    Cql(String),

    #[error("rocksdb: {0}")]
    Rocks(String),

    #[error("codec: {0}")]
    Codec(String),

    #[error("store backend unsupported: {0}")]
    Unsupported(&'static str),

    #[error("enrichment: {0}")]
    Enrichment(String),

    #[error("telemetry: {0}")]
    Telemetry(String),

    #[error("shutdown timed out")]
    ShutdownTimeout,
}

pub type StitcherResult<T> = error_stack::Result<T, StitcherError>;
