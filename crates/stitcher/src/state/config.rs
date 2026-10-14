//! `state.yaml` loading + ahead-of-time compilation into the [`Program`].

use std::collections::BTreeMap;
use std::path::Path;

use error_stack::ResultExt;
use stitcher_dsl::expr::Expr;
use stitcher_dsl::model;

use crate::errors::{StitcherError, StitcherResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeySeg {
    Literal(String),
    /// A `{log.path}` hole; `|`-alternatives resolve to the first present value — all
    /// missing (or non-string) drops the record.
    Path(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterProg {
    /// Required non-empty paths; `|`-alternatives per entry, any one present satisfies.
    pub require: Vec<Vec<String>>,
    /// Reject records whose required string fields contain this substring.
    pub reject_if_contains: Option<String>,
    /// Allowed `log_type` values (empty = no check).
    pub log_type_in: Vec<String>,
    /// Paths the `log_type` value is read from; `|`-alternatives, first present wins.
    pub log_type_path: Vec<String>,
    /// Tenant id paths; `|`-alternatives, first present wins.
    pub tenant_path: Vec<String>,
}

/// `keyed_map` value node (v1: always `latest_by`, gated by the map's own `when`).
#[derive(Clone, Debug, PartialEq)]
pub struct LatestByProg {
    /// Coerced to epoch nanos.
    pub comparator: Expr,
    pub payload: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldProg {
    /// Keep payload with the max comparator.
    LatestBy {
        /// Falsy ⇒ identity contribution.
        when: Option<Expr>,
        comparator: Expr,
        payload: Expr,
    },
    /// Map with per-key merged values; absent key ⇒ insert.
    KeyedMap {
        when: Option<Expr>,
        /// Meaningful-only.
        key: Expr,
        value: Box<LatestByProg>,
    },
    /// Keep the rightmost meaningful value.
    Last { when: Option<Expr>, value: Expr },
    /// Keep the FIRST meaningful value (write-once / sticky).
    Once { when: Option<Expr>, value: Expr },
    /// Sum counter; contributes 1 per admitted (and gated) record.
    Counter { when: Option<Expr> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Program {
    pub aggregate: String,
    pub version: i64,
    pub id_type: String,
    pub key: Vec<KeySeg>,
    pub filter: FilterProg,
    /// Ordered — deterministic state serialization.
    pub fields: BTreeMap<String, FieldProg>,
}

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

pub fn compile(schema: &model::Schema) -> Result<Program, String> {
    let err = |m: String| format!("[schema {}] {m}", schema.aggregate);
    if schema.aggregate.is_empty() {
        return Err(err("aggregate name must be non-empty".into()));
    }
    // `|`-separated string → trimmed non-empty alternatives, first-present-wins
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
            schema
                .decode_filter
                .log_type_path
                .as_deref()
                .unwrap_or("log_type"),
        ),
        tenant_path: split_alts(
            schema
                .decode_filter
                .tenant_path
                .as_deref()
                .unwrap_or("log.tenant_id"),
        ),
    };
    let mut fields = BTreeMap::new();
    for (name, field) in &schema.fields {
        let compiled = compile_field(field).map_err(|e| err(format!("field {name:?}: {e}")))?;
        fields.insert(name.clone(), compiled);
    }
    Ok(Program {
        aggregate: schema.aggregate.clone(),
        version: schema.version,
        id_type: schema.id_type.clone(),
        key: compile_key(&schema.primary_key).map_err(&err)?,
        filter,
        fields,
    })
}

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
        model::Field::Once { when, value } => Ok(FieldProg::Once {
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

/// Omitted (`None`) ⇒ `$` (the whole record).
fn compile_payload(payload: &Option<String>) -> Result<Expr, String> {
    match payload {
        Some(src) => stitcher_dsl::expr::compile(src),
        None => Ok(Expr::Root),
    }
}

/// An empty hole (`{}`) is a compile error: it would drop every record at key time.
fn compile_key(template: &str) -> Result<Vec<KeySeg>, String> {
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
                // unbalanced '{' — literal
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
    let has_empty_hole = segments
        .iter()
        .any(|segment| matches!(segment, KeySeg::Path(alternatives) if alternatives.is_empty()));
    if has_empty_hole {
        Err(format!(
            "primary_key template {template:?} has an empty hole `{{}}` (no path); \
             every record would be dropped"
        ))
    } else {
        Ok(segments)
    }
}
