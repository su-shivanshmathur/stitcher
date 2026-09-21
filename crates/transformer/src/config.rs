//! `transformer.toml` model + ahead-of-time compilation (parse every DSL string once at
//! boot, fail fast). Versioned; a transform change is a deliberate config bump applied on
//! restart (no hot-reload).

use std::{collections::BTreeMap, path::Path};

use error_stack::ResultExt;
use serde::Deserialize;
use stitcher_dsl::expr::{self, Expr};

use super::engine::{CompiledStream, Retention, Transform};
use stitcher::errors::{StitcherError, StitcherResult};
use stitcher::state::config::{FieldProg, Program};

/// Top-level `transformer.toml` document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransformDoc {
    /// Config version (audited; bumped on every transform-logic change).
    version: i64,
    /// Output streams (`[[stream]]`).
    #[serde(default)]
    stream: Vec<StreamDoc>,
}

/// One `[[stream]]` entry.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamDoc {
    name: String,
    topic: String,
    #[serde(default = "default_true")]
    sign_flag: bool,
    #[serde(default)]
    gate: Option<String>,
    #[serde(default)]
    retention: Option<RetentionDoc>,
    #[serde(default)]
    explode: Option<String>,
    #[serde(default)]
    common: BTreeMap<String, String>,
    #[serde(default)]
    fields: BTreeMap<String, String>,
    #[serde(default)]
    element: BTreeMap<String, String>,
    #[serde(default)]
    key: Option<String>,
}

/// Per-stream retention config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionDoc {
    days: i64,
    key: String,
}

const fn default_true() -> bool {
    true
}

/// Load + compile a `transformer.toml`, cross-validated against the state schema `program`.
pub fn load(path: &Path, program: &Program) -> StitcherResult<Transform> {
    let ctx = || format!("load transformer config {}", path.display());
    let doc: TransformDoc = config::Config::builder()
        .add_source(config::File::from(path.to_path_buf()).required(true))
        .build()
        .change_context(StitcherError::Config(ctx()))?
        .try_deserialize()
        .change_context(StitcherError::Config(ctx()))?;
    tracing::info!(
        version = doc.version,
        streams = doc.stream.len(),
        "loaded transformer config"
    );
    compile(doc, program)
        .map_err(|e| error_stack::report!(StitcherError::Config(format!("{}: {e}", ctx()))))
}

fn compile(doc: TransformDoc, program: &Program) -> Result<Transform, String> {
    if doc.version <= 0 {
        return Err("transformer version must be > 0".to_string());
    }
    let streams = doc
        .stream
        .into_iter()
        .map(|stream| compile_stream(stream, program))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Transform::new(streams))
}

fn compile_stream(doc: StreamDoc, program: &Program) -> Result<CompiledStream, String> {
    let name = doc.name.clone();
    let err = move |m: String| format!("[stream {name}] {m}");

    let exploding = doc.explode.is_some();
    if !doc.fields.is_empty() && (exploding || !doc.element.is_empty()) {
        return Err(err(
            "use either `fields` (one row) or `explode` + `element` (per-entry rows)".into(),
        ));
    }
    if !exploding && !doc.element.is_empty() {
        return Err(err("`element` requires `explode`".into()));
    }
    if let Some(retention) = &doc.retention {
        if retention.days <= 0 || retention.key.is_empty() {
            return Err(err(
                "retention.days must be > 0 and retention.key non-empty".into(),
            ));
        }
    }

    // non-explode: `fields` are the row; explode: `element` are the per-entry row
    let row_src = if exploding { &doc.element } else { &doc.fields };
    let compiled = CompiledStream {
        topic: doc.topic,
        sign_flag: doc.sign_flag,
        gate: compile_opt(&doc.gate).map_err(&err)?,
        retention: doc.retention.map(|r| Retention {
            days: r.days,
            key: r.key,
        }),
        explode: compile_opt(&doc.explode).map_err(&err)?,
        common: compile_fields(&doc.common).map_err(&err)?,
        row: compile_fields(row_src).map_err(&err)?,
        key: compile_opt(&doc.key).map_err(&err)?,
    };
    validate_against_schema(&compiled, program).map_err(&err)?;
    Ok(compiled)
}

/// Reject a stream that references state the schema doesn't produce: every path must root at
/// `state.<field>` (a real field) or, inside an explode stream, `each.`; `explode` must target
/// a single `state.<keyed_map>` field.
fn validate_against_schema(stream: &CompiledStream, program: &Program) -> Result<(), String> {
    let exploding = stream.explode.is_some();
    let mut paths: Vec<&str> = Vec::new();
    for expr in stream.gate.iter().chain(&stream.explode).chain(&stream.key) {
        collect_paths(expr, &mut paths);
    }
    for (_, expr) in stream.common.iter().chain(&stream.row) {
        collect_paths(expr, &mut paths);
    }
    for path in paths {
        if let Some(rest) = path.strip_prefix("state.") {
            let head = rest.split('.').next().unwrap_or(rest);
            if !program.fields.contains_key(head) {
                return Err(format!("references unknown state field {head:?}"));
            }
        } else if path == "state" {
            // whole-state reference is allowed
        } else if path == "each" || path.starts_with("each.") {
            if !exploding {
                return Err(format!("`{path}` is only valid in an explode stream"));
            }
        } else {
            return Err(format!("path {path:?} must be rooted at `state.` or `each.`"));
        }
    }

    if let Some(explode) = &stream.explode {
        let field = match explode {
            Expr::Path(p) => p.strip_prefix("state.").filter(|f| !f.contains('.')),
            _ => None,
        }
        .ok_or_else(|| "explode must be a single `state.<field>` path".to_string())?;
        match program.fields.get(field) {
            Some(FieldProg::KeyedMap { .. }) => {}
            Some(_) => return Err(format!("explode field {field:?} must be a keyed_map")),
            None => return Err(format!("explode references unknown state field {field:?}")),
        }
    }
    Ok(())
}

/// Every path root referenced anywhere in an expression tree.
fn collect_paths<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
    match expr {
        Expr::Path(path) => out.push(path),
        Expr::Not(inner) => collect_paths(inner, out),
        Expr::Bin(_, lhs, rhs) => {
            collect_paths(lhs, out);
            collect_paths(rhs, out);
        }
        Expr::Call(_, args) => args.iter().for_each(|arg| collect_paths(arg, out)),
        _ => {}
    }
}

fn compile_fields(fields: &BTreeMap<String, String>) -> Result<Vec<(String, Expr)>, String> {
    fields
        .iter()
        .map(|(name, src)| expr::compile(src).map(|compiled| (name.clone(), compiled)))
        .collect()
}

fn compile_opt(src: &Option<String>) -> Result<Option<Expr>, String> {
    src.as_ref().map(|s| expr::compile(s)).transpose()
}
