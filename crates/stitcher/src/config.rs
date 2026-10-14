//! Settings: `config/<RUN_ENV>.toml` + optional `--config-path` + `STITCHER__*` env overrides.

use std::{collections::HashMap, path::PathBuf};

use clap::{Parser, Subcommand};
use error_stack::ResultExt;
use serde::Deserialize;

use crate::secret::Secret;

use crate::errors::{StitcherError, StitcherResult};

#[derive(Debug, Parser)]
#[command(name = "stitcher", about = "domain-agnostic Kafka stream aggregation")]
pub struct Cli {
    /// Absent ⇒ run the streaming pipeline.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// TOML config layered over `config/<RUN_ENV>.toml`.
    #[arg(long = "config-path", value_name = "FILE")]
    pub config_path: Option<PathBuf>,

    /// Inspect mode: print in/state/out, write/produce/commit nothing (fresh group, re-runnable).
    #[arg(long)]
    pub inspect: bool,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate a `transformer.toml` against a `state.yaml` and exit (CI-runnable).
    CheckTransformer {
        #[arg(long = "state", value_name = "FILE")]
        state: PathBuf,
        #[arg(long = "transformer", value_name = "FILE")]
        transformer: PathBuf,
        /// When given, `lookup` table names are checked against it.
        #[arg(long = "enrichment", value_name = "FILE")]
        enrichment: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub server: Server,
    pub log: LogCfg,
    pub source_kafka: SourceKafka,
    pub sink_kafka: SinkKafka,
    /// Count OR time, whichever hits first.
    pub batch: Batch,
    pub store: StoreCfg,
    /// State-machine config for the generic binary (`state.yaml`).
    pub state: Option<StateCfg>,
    /// Output transformer (`transformer.toml`); absent ⇒ passthrough `encode`.
    pub transform: TransformCfg,
    pub enrichment: Enrichment,
    /// Tenant allow-list.
    pub tenant_ids: Vec<String>,
    pub debug: DebugCfg,
    /// Cap (secs) on the final in-flight batch during shutdown — keep below the orchestrator
    /// kill deadline (timeout ⇒ pre-commit abort ⇒ replay). `None` = unbounded.
    pub shutdown_grace_secs: Option<u64>,
    /// Max concurrent remote reads within a batch.
    pub read_concurrency: usize,
    /// Max seconds a rebalance-revoke waits for the in-flight batch to drain.
    pub rebalance_drain_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Server {
    pub host: String,
    pub port: u16,
    /// Gates `GET /state` via the `api-key` header; unset ⇒ every request 401.
    pub admin_api_key: Option<Secret<String>>,
}

/// `RUST_LOG` wins over this level.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogCfg {
    /// Filtering directive, e.g. `"info,libsasl2=warn"`.
    pub level: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Json,
    HumanReadable,
}

/// Extra librdkafka properties; `Debug` redacts secret-looking values.
#[derive(Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct KafkaExtra(pub HashMap<String, String>);

impl std::ops::Deref for KafkaExtra {
    type Target = HashMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for KafkaExtra {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in &self.0 {
            if key.contains("password") || key.contains("secret") {
                map.entry(key, &"***");
            } else {
                map.entry(key, value);
            }
        }
        map.finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SourceKafka {
    /// `bootstrap.servers`.
    pub brokers: Vec<String>,
    pub topics: Vec<String>,
    pub consumer_group: String,
    /// `"earliest"` | `"latest"` for groups without committed offsets (`auto.offset.reset`).
    pub auto_offset_reset: String,
    /// librdkafka stats interval, ms; `0` disables.
    pub statistics_interval_ms: u64,
    pub extra: KafkaExtra,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SinkKafka {
    /// `bootstrap.servers`.
    pub brokers: Vec<String>,
    pub extra: KafkaExtra,
    /// Dead-letter topic for malformed records.
    pub dlq_topic: String,
    /// Delivery timeout per record, seconds.
    pub delivery_timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct Batch {
    /// Max records per batch.
    pub count: usize,
    /// Max wait before a partial batch flushes, milliseconds.
    pub window_ms: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StoreCfg {
    pub backend: Backend,
    pub cql: CqlCfg,
    pub rocksdb: RocksCfg,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    #[default]
    Cql,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CqlCfg {
    pub hosts: Vec<String>,
    pub port: u16,
    pub keyspace: String,
    /// Columns `id, id_type, version, state`.
    pub table: String,
    pub username: Option<String>,
    /// Masked — never logged/debugged in clear.
    pub password: Option<Secret<String>>,
    /// Rewrite advertised peer addresses (NAT/port-forward).
    pub address_translation: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RocksCfg {
    pub path: String,
    /// Column-family TTL, seconds (`0` = none).
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StateCfg {
    pub config_file: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransformCfg {
    /// Path to `transformer.toml`; absent ⇒ passthrough `encode`.
    pub config_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Enrichment {
    pub config_file: String,
    pub reload_secs: u64,
}

/// `--inspect` is a shorthand for the first four.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DebugCfg {
    pub print_incoming: bool,
    pub print_state: bool,
    pub print_outgoing: bool,
    pub dry_run: bool,
    pub format: InspectFormat,
    /// Fraction to print, `0.0..=1.0`.
    pub sample: f64,
    /// Print only keys containing this substring ("" = all).
    pub only_key_substr: String,
    /// Mask PII fields before printing (payments data!).
    pub redact: bool,
    pub redact_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InspectFormat {
    #[default]
    Pretty,
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
    /// Raises the `stitcher::inspect` log directive.
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
            transform: TransformCfg::default(),
            enrichment: Enrichment::default(),
            tenant_ids: Vec::new(),
            debug: DebugCfg::default(),
            shutdown_grace_secs: Some(20),
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
            admin_api_key: None,
        }
    }
}

impl Default for LogCfg {
    fn default() -> Self {
        Self {
            level: "info,rdkafka=warn,scylla=warn,actix_web=warn,actix_server=warn".to_string(),
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
            extra: KafkaExtra::default(),
        }
    }
}

impl Default for SinkKafka {
    fn default() -> Self {
        Self {
            brokers: Vec::new(),
            extra: KafkaExtra::default(),
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
    pub fn validate(&self) -> StitcherResult<()> {
        // breaks the build if a backend is added
        let Backend::Cql = self.store.backend;

        // first failing check wins
        let checks = [
            (
                self.source_kafka.brokers.is_empty(),
                "source_kafka.brokers must be non-empty",
            ),
            (
                self.source_kafka.topics.is_empty(),
                "source_kafka.topics must be non-empty",
            ),
            (
                self.source_kafka.consumer_group.is_empty(),
                "source_kafka.consumer_group must be non-empty",
            ),
            (
                self.sink_kafka.brokers.is_empty(),
                "sink_kafka.brokers must be non-empty",
            ),
            (self.batch.count == 0, "batch.count must be > 0"),
            (self.read_concurrency == 0, "read_concurrency must be > 0"),
            (
                self.shutdown_grace_secs == Some(0),
                "shutdown_grace_secs must be > 0 when set",
            ),
            (
                !(0.0..=1.0).contains(&self.debug.sample),
                "debug.sample must be within 0.0..=1.0",
            ),
            (
                self.server
                    .admin_api_key
                    .as_ref()
                    .is_some_and(|k| k.expose().is_empty()),
                "server.admin_api_key must not be empty when set",
            ),
            (
                self.store.cql.hosts.is_empty(),
                "store.cql.hosts must be non-empty",
            ),
            (
                self.store.cql.keyspace.is_empty() || self.store.cql.table.is_empty(),
                "store.cql.keyspace and store.cql.table must be non-empty",
            ),
        ];

        checks
            .into_iter()
            .find_map(|(failed, msg)| failed.then_some(msg))
            .map_or(Ok(()), |msg| {
                Err(error_stack::report!(StitcherError::Config(msg.to_string())))
            })
    }
}

#[must_use]
pub fn parse_cli() -> Cli {
    Cli::parse()
}

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
