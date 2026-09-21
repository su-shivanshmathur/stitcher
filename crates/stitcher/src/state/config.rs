//! `state.yaml` loading + ahead-of-time compilation (parse DSL strings once at boot,
//! lantern #37 [pe]): the [`Program`] is everything [`super::interp`] needs at runtime.

use std::collections::BTreeMap;
use std::path::Path;

use error_stack::ResultExt;
use stitcher_dsl::expr::Expr;
use stitcher_dsl::model;

use crate::errors::{StitcherError, StitcherResult};

/// One segment of the pre-parsed key template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeySeg {
    /// Literal text between `{…}` holes.
    Literal(String),
    /// A `{log.path}` hole; alternatives separated by `|` inside the hole
    /// (`{log.payment_id|log.payment_intent_id}`) resolve to the first
    /// present value — for identity components that live at different paths
    /// across event types. All alternatives missing (or non-string) drops
    /// the record (== `_primaryKey`).
    Path(Vec<String>),
}

/// Record admission filter (== `_decodeLog`), compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterProg {
    /// Required non-empty paths; each entry carries `|`-separated alternatives
    /// (any one present satisfies the requirement).
    pub require: Vec<Vec<String>>,
    /// Reject records whose required string fields contain this substring (only
    /// checked while iterating `require` — inert when `require` is empty, like codegen).
    pub reject_if_contains: Option<String>,
    /// Allowed `log_type` values (empty = no check).
    pub log_type_in: Vec<String>,
    /// Paths the `log_type` value is read from; `|`-alternatives resolve to the
    /// first present (a lifecycle event carries `log_type`, an API event `api_flow`).
    pub log_type_path: Vec<String>,
    /// Tenant id paths (first-present-wins across `|`-alternatives, for event
    /// types that carry the tenant at different paths).
    pub tenant_path: Vec<String>,
}

/// `keyed_map` value node (v1: always `latest_by`, gated by the map's own `when`).
#[derive(Clone, Debug, PartialEq)]
pub struct LatestByProg {
    /// Comparator expression (coerced to epoch nanos).
    pub comparator: Expr,
    /// Payload expression.
    pub payload: Expr,
}

/// One state field, compiled.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldProg {
    /// Keep payload with the max comparator.
    LatestBy {
        /// Predicate gating extraction; falsy ⇒ identity contribution.
        when: Option<Expr>,
        /// Comparator expression.
        comparator: Expr,
        /// Payload expression.
        payload: Expr,
    },
    /// Map with per-key merged values; absent key ⇒ insert.
    KeyedMap {
        /// Predicate gating entry creation.
        when: Option<Expr>,
        /// Key expression (meaningful-only).
        key: Expr,
        /// Value node.
        value: Box<LatestByProg>,
    },
    /// Keep the rightmost meaningful value.
    Last {
        /// Predicate gating extraction.
        when: Option<Expr>,
        /// Value expression.
        value: Expr,
    },
    /// Sum counter; contributes 1 per admitted (and gated) record.
    Counter {
        /// Predicate gating the contribution.
        when: Option<Expr>,
    },
}

/// One output sink, compiled (== the codegen `sinks:` entry).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkProg {
    /// Sink name (diagnostics).
    pub name: String,
    /// Output topic.
    pub topic: String,
    /// State field feeding this sink.
    pub field: String,
    /// Map fields: one message per map entry.
    pub fan_out: bool,
    /// JSON path inside the field payload used as the Kafka record key.
    pub key_path: Option<String>,
}

/// A fully compiled state schema.
#[derive(Clone, Debug, PartialEq)]
pub struct Program {
    /// Aggregate name (diagnostics).
    pub aggregate: String,
    /// Stored-state version.
    pub version: i64,
    /// Store `id_type` discriminator.
    pub id_type: String,
    /// Pre-parsed key template segments.
    pub key: Vec<KeySeg>,
    /// Compiled admission filter.
    pub filter: FilterProg,
    /// Compiled state fields (ordered: deterministic state serialization).
    pub fields: BTreeMap<String, FieldProg>,
    /// Compiled output sinks (ordered).
    pub sinks: Vec<SinkProg>,
}

/// Load + compile a `state.yaml` from disk.
pub fn load(path: &Path) -> StitcherResult<Program> {
    let ctx = || format!("load state config {}", path.display());
    let src = std::fs::read_to_string(path)
        .change_context(StitcherError::Config(ctx()))
        .attach_printable_lazy(|| "state.config_file is unreadable".to_string())?;
    let schema: model::Schema = serde_yaml_ng::from_str(&src)
        .change_context(StitcherError::Config(ctx()))
        .attach_printable_lazy(|| "state.config_file is not a valid schema".to_string())?;
    compile(&schema)
        .map_err(|e| error_stack::report!(StitcherError::Config(format!("{}: {e}", ctx()))))
}

/// Schema → [`Program`] (validation mirrors `stitcher_macro`'s codegen checks; sinks
/// stay schema-side metadata for the transformer crate — lantern #36 — so only
/// referenced-field coherence is checked here).
fn compile(schema: &model::Schema) -> Result<Program, String> {
    let err = |m: String| format!("[schema {}] {m}", schema.aggregate);
    if schema.aggregate.is_empty() {
        return Err(err("aggregate name must be non-empty".into()));
    }
    for sink in &schema.sinks {
        let Some(field) = schema.fields.get(&sink.field) else {
            return Err(err(format!(
                "sink {:?} references unknown field {:?}",
                sink.name, sink.field
            )));
        };
        // fan_out iterates map entries; key_path reads `{field}.payload.{p}` — only a
        // non-fan-out `latest_by` sink has that shape (mirrored in codegen `validate`).
        if sink.fan_out && !matches!(field, model::Field::KeyedMap { .. }) {
            return Err(err(format!(
                "sink {:?}: fan_out requires a keyed_map field",
                sink.name
            )));
        }
        if sink.key_path.is_some()
            && (sink.fan_out || !matches!(field, model::Field::LatestBy { .. }))
        {
            return Err(err(format!(
                "sink {:?}: key_path requires a non-fan_out latest_by field",
                sink.name
            )));
        }
        // retention fields are consumed by the pipeline's per-topic config; when stated
        // here we validate coherence so schema and config can't contradict silently.
        match (sink.retention_days, &sink.retention_key) {
            (Some(days), Some(key)) => {
                if days <= 0 || key.is_empty() {
                    return Err(err(format!(
                        "sink {:?}: retention_days must be > 0 and retention_key non-empty",
                        sink.name
                    )));
                }
            }
            (None, None) => {}
            _ => {
                return Err(err(format!(
                    "sink {:?}: retention_days and retention_key must be set together",
                    sink.name
                )))
            }
        }
    }
    // A `|`-separated string → its trimmed, non-empty alternatives (first-present-wins
    // at admission/keying). Shared by `require`, `log_type_path` and `tenant_path`.
    let split_alts = |src: &str| -> Vec<String> {
        src.split('|')
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(str::to_string)
            .collect()
    };
    let filter = FilterProg {
        require: schema
            .decode_filter
            .require
            .iter()
            .map(|entry| split_alts(entry))
            .collect(),
        reject_if_contains: schema.decode_filter.reject_if_contains.clone(),
        log_type_in: schema.decode_filter.log_type_in.clone(),
        log_type_path: split_alts(
            schema.decode_filter.log_type_path.as_deref().unwrap_or("log_type"),
        ),
        tenant_path: split_alts(
            schema.decode_filter.tenant_path.as_deref().unwrap_or("log.tenant_id"),
        ),
    };
    let mut fields = BTreeMap::new();
    for (name, field) in &schema.fields {
        let compiled = compile_field(field).map_err(|e| err(format!("field {name:?}: {e}")))?;
        fields.insert(name.clone(), compiled);
    }
    let sinks = schema
        .sinks
        .iter()
        .map(|s| SinkProg {
            name: s.name.clone(),
            topic: s.topic.clone(),
            field: s.field.clone(),
            fan_out: s.fan_out,
            key_path: s.key_path.clone(),
        })
        .collect();
    Ok(Program {
        aggregate: schema.aggregate.clone(),
        version: schema.version,
        id_type: schema.id_type.clone(),
        key: compile_key(&schema.primary_key),
        filter,
        fields,
        sinks,
    })
}

/// Field node → compiled field (mirror of `stitcher_macro`'s `field_build`).
fn compile_field(field: &model::Field) -> Result<FieldProg, String> {
    match field {
        model::Field::LatestBy {
            when,
            comparator,
            payload,
        } => Ok(FieldProg::LatestBy {
            when: compile_when(when)?,
            comparator: stitcher_dsl::expr::compile(comparator)?,
            payload: compile_payload(payload)?,
        }),
        model::Field::KeyedMap { when, key, value } => {
            let value = match value.as_ref() {
                model::Field::LatestBy {
                    when: None,
                    comparator,
                    payload,
                } => LatestByProg {
                    comparator: stitcher_dsl::expr::compile(comparator)?,
                    payload: compile_payload(payload)?,
                },
                model::Field::LatestBy { when: Some(_), .. } => {
                    return Err(
                        "inner latest_by must not re-declare `when` (gated by the keyed_map)"
                            .into(),
                    )
                }
                other => return Err(format!("unsupported keyed_map value node: {other:?}")),
            };
            Ok(FieldProg::KeyedMap {
                when: compile_when(when)?,
                key: stitcher_dsl::expr::compile(key)?,
                value: Box::new(value),
            })
        }
        model::Field::Last { when, value } => Ok(FieldProg::Last {
            when: compile_when(when)?,
            value: stitcher_dsl::expr::compile(value)?,
        }),
        model::Field::Counter { when } => Ok(FieldProg::Counter {
            when: compile_when(when)?,
        }),
        model::Field::Nested { .. } => {
            Err("nested state fields are not part of DSL v1; use keyed_map / latest_by".into())
        }
    }
}

fn compile_when(when: &Option<String>) -> Result<Option<Expr>, String> {
    match when {
        Some(src) => stitcher_dsl::expr::compile(src).map(Some),
        None => Ok(None),
    }
}

/// Compile a payload expression; omitted (`None`) ⇒ `$` (the whole record, as sent).
fn compile_payload(payload: &Option<String>) -> Result<Expr, String> {
    match payload {
        Some(src) => stitcher_dsl::expr::compile(src),
        None => Ok(Expr::Root),
    }
}

/// `"{log.a}-{log.b}"` → segments (mirror of `stitcher_macro`'s `gen_key_body`,
/// including the unbalanced-`{`-is-literal rule).
fn compile_key(template: &str) -> Vec<KeySeg> {
    let mut segments = Vec::new();
    let mut rest = template;
    loop {
        if let Some(open) = rest.find('{') {
            let (literal, after) = rest.split_at(open);
            if !literal.is_empty() {
                segments.push(KeySeg::Literal(literal.to_string()));
            }
            if let Some(close) = after.find('}') {
                let hole = after.get(1..close).unwrap_or_default();
                let alternative_paths: Vec<String> = hole
                    .split('|')
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .map(str::to_string)
                    .collect();
                segments.push(KeySeg::Path(alternative_paths));
                rest = after.get(close + 1..).unwrap_or_default();
            } else {
                // unbalanced '{' — treat it as a literal and continue after it
                segments.push(KeySeg::Literal("{".to_string()));
                rest = after.get(1..).unwrap_or_default();
            }
        } else {
            if !rest.is_empty() {
                segments.push(KeySeg::Literal(rest.to_string()));
            }
            break;
        }
    }
    segments
}
