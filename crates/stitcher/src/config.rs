//! Settings: `config/<RUN_ENV>.toml` + optional `--config-path` file + `STITCHER__*` env
//! overrides (PLAN §14); `serde_path_to_error` pinpoints bad fields.

use std::{collections::HashMap, path::PathBuf};

use clap::Parser;
use error_stack::ResultExt;
use serde::Deserialize;

use crate::secret::Secret;

use crate::errors::{StitcherError, StitcherResult};

/// CLI entry: the config path (everything else lives in the TOML/env layers) and the
/// inspect-mode shorthand.
#[derive(Debug, Parser)]
#[command(name = "stitcher", about = "domain-agnostic Kafka stream aggregation")]
pub struct Cli {
    /// Path to a TOML config layered over `config/<RUN_ENV>.toml`.
    #[arg(long = "config-path", value_name = "FILE")]
    pub config_path: Option<PathBuf>,

    /// Inspect mode (INSPECT.md): print incoming records, old→merged states and
    /// outgoing sink records; write/produce/commit NOTHING (fresh consumer group;
    /// safe to re-run).
    #[arg(long)]
    pub inspect: bool,
}

/// Root settings object.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Settings {
    /// Metrics/health HTTP server.
    pub server: Server,
    /// Logging.
    pub log: LogCfg,
    /// Source cluster (consumed topics).
    pub source_kafka: SourceKafka,
    /// Sink cluster (produced deltas + DLQ).
    pub sink_kafka: SinkKafka,
    /// Aggregation batching: count OR time, whichever hits first.
    pub batch: Batch,
    /// State store(s).
    pub store: StoreCfg,
    /// State-machine config for the generic binary (`state.yaml`, lantern #37).
    pub state: Option<StateCfg>,
    /// Enrichment join-map reload.
    pub enrichment: Enrichment,
    /// Per-sink output config, keyed by sink name (`[filters.intent]` etc.).
    pub filters: HashMap<String, Filter>,
    /// Tenant allow-list (== `--tenant-id`).
    pub tenant_ids: Vec<String>,
    /// Inspect mode (dev/staging introspection; INSPECT.md).
    pub debug: DebugCfg,
    /// Optional cap (seconds) on processing the final in-flight batch during shutdown.
    pub shutdown_grace_secs: Option<u64>,
    /// Max concurrent remote reads within a batch.
    pub read_concurrency: usize,
    /// Max seconds a rebalance-revoke waits for the in-flight batch to drain.
    pub rebalance_drain_secs: u64,
}

/// Metrics + health endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Server {
    /// Bind host.
    pub host: String,
    /// Bind port (== `--prometheus-port`).
    pub port: u16,
}

/// Logging (`tracing-subscriber`: JSON or human-readable console; `RUST_LOG` wins).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogCfg {
    /// Global filtering directive, e.g. `"info,libsasl2=warn"`.
    pub level: String,
    /// `json` | `console` (human-readable).
    pub format: LogFormat,
}

/// Console log formatting.
#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Compact single-line JSON.
    #[default]
    Json,
    /// Human-readable multi-line.
    HumanReadable,
}

/// Source Kafka.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SourceKafka {
    /// `bootstrap.servers`.
    pub brokers: Vec<String>,
    /// Topics to subscribe (== `--topics`).
    pub topics: Vec<String>,
    /// Consumer group id (== `--consumer-group-id`).
    pub consumer_group: String,
    /// Where a group without committed offsets starts: `"earliest"` (replay
    /// the log) or `"latest"` (only new records); passed through to
    /// `auto.offset.reset`.
    pub auto_offset_reset: String,
    /// librdkafka statistics interval in ms (feeds the consumer-lag gauges);
    /// `0` disables.
    pub statistics_interval_ms: u64,
    /// Extra librdkafka consumer properties (e.g. `partition.assignment.strategy`).
    pub extra: HashMap<String, String>,
}

/// Sink Kafka.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SinkKafka {
    /// `bootstrap.servers`.
    pub brokers: Vec<String>,
    /// Extra librdkafka producer properties.
    pub extra: HashMap<String, String>,
    /// Dead-letter topic for malformed records (PLAN §23).
    pub dlq_topic: String,
    /// Delivery timeout per record, seconds.
    pub delivery_timeout_secs: u64,
}

/// Batching (== `--aggregation-batch-count` / `--aggregation-window-ms`).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct Batch {
    /// Max records per batch.
    pub count: usize,
    /// Max wait before a partial batch flushes, milliseconds.
    pub window_ms: u64,
}

/// Store selection + backend configs.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StoreCfg {
    /// Which remote backend persists merged state.
    pub backend: Backend,
    /// CQL (`ScyllaDB` / Cassandra).
    pub cql: CqlCfg,
    /// Local `RocksDB` cache.
    pub rocksdb: RocksCfg,
}

/// Remote backend selection.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    /// `ScyllaDB` / Cassandra via the `scylla` CQL driver.
    #[default]
    Cql,
    /// `DynamoDB` (stubbed; PLAN §4).
    Dynamodb,
}

/// CQL connection settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CqlCfg {
    /// Contact points (== `--cassandra-hosts`).
    pub hosts: Vec<String>,
    /// Native transport port.
    pub port: u16,
    /// Keyspace (== `--cassandra-keyspace`).
    pub keyspace: String,
    /// Table (== `--cassandra-table`); columns `id, id_type, version, state`.
    pub table: String,
    /// Optional auth username.
    pub username: Option<String>,
    /// Optional auth password (masked — never logged/debugged in clear).
    pub password: Option<Secret<String>>,
    /// Rewrite discovered peer addresses — for nodes behind a port-forward/NAT that
    /// advertise an unreachable IP (e.g. `"10.89.0.10:9042" = "127.0.0.1:9042"`).
    /// Unmapped peers pass through untouched; empty = no translation.
    pub address_translation: HashMap<String, String>,
}

/// `RocksDB` local cache settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RocksCfg {
    /// On-disk path (== `--rocksdb-path`).
    pub path: String,
    /// Optional TTL for column families, seconds (`0` = no TTL).
    pub ttl_secs: u64,
}

/// State-machine config (the generic `stitcher` binary's compiled schema).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StateCfg {
    /// Path to the `state.yaml` schema (aggregate/key/filter/fields).
    pub config_file: PathBuf,
}

/// Enrichment join-map reload (== `--config-file` / `--sleep-in-sec`).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Enrichment {
    /// JSON join-map file.
    pub config_file: String,
    /// Background reload interval.
    pub reload_secs: u64,
}

/// One sink's emission config.
#[derive(Debug, Clone, Deserialize)]
pub struct Filter {
    /// Sink topic (e.g. `stitcher-intent-events`).
    pub topic: String,
    /// Drop records older than this many days.
    pub retention_days: i64,
    /// JSON field holding the record's epoch-seconds timestamp.
    pub retention_key: String,
}

/// Inspect mode (`stitcher/INSPECT.md`): dev/staging introspection aid. Off by default;
/// `--inspect` is a shorthand for `print_incoming = print_state = print_outgoing =
/// dry_run = true`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DebugCfg {
    /// Log each consumed record: topic/partition/offset/key + redacted value.
    pub print_incoming: bool,
    /// Log old→merged state per key after the fold.
    pub print_state: bool,
    /// Log each outgoing sink record (topic/key + redacted payload), pre-retention.
    pub print_outgoing: bool,
    /// Consume + decode + merge + print, but DON'T persist/produce/commit.
    pub dry_run: bool,
    /// Rendering: `pretty` (indented JSON) or `compact`.
    pub format: InspectFormat,
    /// Fraction of records/states to print, `0.0..=1.0` (flood control).
    pub sample: f64,
    /// Print only records/states whose key contains this substring ("" = all).
    pub only_key_substr: String,
    /// Mask PII fields before printing (payments data!).
    pub redact: bool,
    /// Field names masked (recursively) when `redact` is on.
    pub redact_fields: Vec<String>,
}

/// Inspect output rendering.
#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InspectFormat {
    /// Indented JSON.
    #[default]
    Pretty,
    /// Single-line JSON.
    Compact,
}

impl Default for DebugCfg {
    fn default() -> Self {
        Self {
            print_incoming: false,
            print_state: false,
            print_outgoing: false,
            dry_run: false,
            format: InspectFormat::default(),
            sample: 1.0,
            only_key_substr: String::new(),
            redact: true,
            redact_fields: vec![
                "payment_method_data".to_string(),
                "customer_email".to_string(),
                "billing_details".to_string(),
                "shipping_details".to_string(),
                "client_secret".to_string(),
                "metadata".to_string(),
            ],
        }
    }
}

impl DebugCfg {
    /// True when any inspect tap is enabled (raises the `stitcher::inspect` directive).
    #[must_use]
    pub fn any_print(&self) -> bool {
        self.print_incoming || self.print_state || self.print_outgoing
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            server: Server::default(),
            log: LogCfg::default(),
            source_kafka: SourceKafka::default(),
            sink_kafka: SinkKafka::default(),
            batch: Batch::default(),
            store: StoreCfg::default(),
            state: None,
            enrichment: Enrichment::default(),
            filters: HashMap::new(),
            tenant_ids: Vec::new(),
            debug: DebugCfg::default(),
            shutdown_grace_secs: None,
            read_concurrency: 64,
            rebalance_drain_secs: 600,
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9090,
        }
    }
}

impl Default for LogCfg {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Json,
        }
    }
}

impl Default for SourceKafka {
    fn default() -> Self {
        Self {
            brokers: Vec::new(),
            topics: Vec::new(),
            consumer_group: "stitcher".to_string(),
            auto_offset_reset: "earliest".to_string(),
            statistics_interval_ms: 10_000,
            extra: HashMap::new(),
        }
    }
}

impl Default for SinkKafka {
    fn default() -> Self {
        Self {
            brokers: Vec::new(),
            extra: HashMap::new(),
            dlq_topic: "stitcher-dlq".to_string(),
            delivery_timeout_secs: 30,
        }
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            count: 100_000,
            window_ms: 1,
        }
    }
}

impl Default for CqlCfg {
    fn default() -> Self {
        Self {
            hosts: Vec::new(),
            port: 9042,
            keyspace: "stitcher".to_string(),
            table: "state".to_string(),
            username: None,
            password: None,
            address_translation: HashMap::new(),
        }
    }
}

impl Default for RocksCfg {
    fn default() -> Self {
        Self {
            path: "/tmp/stitcher-rocksdb".to_string(),
            ttl_secs: 0,
        }
    }
}

impl Default for Enrichment {
    fn default() -> Self {
        Self {
            config_file: String::new(),
            reload_secs: 43_200,
        }
    }
}

impl Settings {
    /// Structural validation after layering; messages are specific and actionable.
    pub fn validate(&self) -> StitcherResult<()> {
        let fail = |msg: &str| Err(error_stack::report!(StitcherError::Config(msg.to_string())));

        if self.source_kafka.brokers.is_empty() {
            return fail("source_kafka.brokers must be non-empty");
        }
        if self.source_kafka.topics.is_empty() {
            return fail("source_kafka.topics must be non-empty");
        }
        if self.source_kafka.consumer_group.is_empty() {
            return fail("source_kafka.consumer_group must be non-empty");
        }
        if self.sink_kafka.brokers.is_empty() {
            return fail("sink_kafka.brokers must be non-empty");
        }
        if self.batch.count == 0 {
            return fail("batch.count must be > 0");
        }
        if self.read_concurrency == 0 {
            return fail("read_concurrency must be > 0");
        }
        if !(0.0..=1.0).contains(&self.debug.sample) {
            return fail("debug.sample must be within 0.0..=1.0");
        }
        match self.store.backend {
            Backend::Cql => {
                if self.store.cql.hosts.is_empty() {
                    return fail("store.cql.hosts must be non-empty for backend=cql");
                }
                if self.store.cql.keyspace.is_empty() || self.store.cql.table.is_empty() {
                    return fail("store.cql.keyspace and store.cql.table must be non-empty");
                }
            }
            Backend::Dynamodb => {} // stub: validated when implemented (PLAN §4)
        }
        for (name, filter) in &self.filters {
            if filter.topic.is_empty() {
                return fail("filters entries must set a topic");
            }
            if filter.retention_key.is_empty() {
                let msg = format!("filters.{name}.retention_key must be non-empty");
                return fail(&msg);
            }
            if filter.retention_days <= 0 {
                let msg = format!("filters.{name}.retention_days must be > 0");
                return fail(&msg);
            }
        }
        Ok(())
    }
}

/// Load settings: defaults → `config/<RUN_ENV>.toml` → `--config-path` → `STITCHER__*` env.
pub fn load() -> StitcherResult<Settings> {
    let cli = Cli::parse();
    load_with(cli.config_path.as_deref(), cli.inspect)
}

/// Testable core of [`load`].
pub fn load_with(
    override_path: Option<&std::path::Path>,
    inspect: bool,
) -> StitcherResult<Settings> {
    let ctx = "load settings";
    let run_env = std::env::var("RUN_ENV").unwrap_or_else(|_| "development".to_string());
    let config_dir = std::env::var("CONFIG_DIR").unwrap_or_else(|_| "config".to_string());
    let env_file = PathBuf::from(&config_dir).join(format!("{run_env}.toml"));

    let mut builder =
        config::Config::builder().add_source(config::File::from(env_file).required(false));
    if let Some(path) = override_path {
        builder = builder.add_source(config::File::from(path).required(true));
    }
    let raw = builder
        .add_source(
            config::Environment::with_prefix("STITCHER")
                .separator("__")
                .list_separator(","),
        )
        .build()
        .change_context(StitcherError::Config(ctx.to_string()))
        .attach_printable_lazy(|| format!("RUN_ENV={run_env} config_dir={config_dir}"))?;

    let settings: Settings = serde_path_to_error::deserialize(
        raw.try_deserialize::<serde_json::Value>()
            .change_context(StitcherError::Config(ctx.to_string()))?,
    )
    .map_err(|e| error_stack::report!(StitcherError::Config(format!("{ctx}: {e}"))))?;

    let mut settings = settings;
    if inspect {
        settings.debug.print_incoming = true;
        settings.debug.print_state = true;
        settings.debug.print_outgoing = true;
        settings.debug.dry_run = true;
    }

    settings.validate()?;
    Ok(settings)
}
