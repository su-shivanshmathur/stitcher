//! `transformer.toml` model + ahead-of-time compilation (parse every DSL string once at
//! boot, fail fast). Versioned; a transform change is a deliberate config bump applied on
//! restart (no hot-reload).

use std::{collections::BTreeMap, path::Path};

use error_stack::ResultExt;
use serde::Deserialize;
use stitcher_dsl::expr::{self, Expr};

use super::engine::{CompiledStream, Retention, Transform};
use stitcher::errors::{StitcherError, StitcherResult};
use stitcher::state::config::Program;

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

/// Load + compile a `transformer.toml`, cross-validated against the state schema `program` and the
/// enrichment `tables` (`None` ⇒ no enrichment supplied, `lookup` table names are not checked).
pub fn load(
    path: &Path,
    program: &Program,
    tables: Option<&[String]>,
) -> StitcherResult<Transform> {
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
    compile(doc, program, tables)
        .map_err(|e| error_stack::report!(StitcherError::Config(format!("{}: {e}", ctx()))))
}

fn compile(
    doc: TransformDoc,
    program: &Program,
    tables: Option<&[String]>,
) -> Result<Transform, String> {
    (doc.version > 0)
        .then_some(())
        .ok_or_else(|| "transformer version must be > 0".to_string())
        .and_then(|()| {
            doc.stream
                .into_iter()
                .map(|stream| compile_stream(stream, program, tables))
                .collect::<Result<Vec<_>, _>>()
        })
        .map(Transform::new)
}

/// A structural defect in a stream's shape (`fields` vs `explode`/`element`, retention bounds).
#[derive(Clone, Copy)]
enum StreamShapeError {
    FieldsWithExplode,
    ElementWithoutExplode,
    BadRetention,
}

impl StreamShapeError {
    fn message(self) -> &'static str {
        match self {
            Self::FieldsWithExplode => {
                "use either `fields` (one row) or `explode` + `element` (per-entry rows)"
            }
            Self::ElementWithoutExplode => "`element` requires `explode`",
            Self::BadRetention => "retention.days must be > 0 and retention.key non-empty",
        }
    }
}

fn compile_stream(
    doc: StreamDoc,
    program: &Program,
    tables: Option<&[String]>,
) -> Result<CompiledStream, String> {
    let name = doc.name.clone();
    let err = move |m: String| format!("[stream {name}] {m}");
    let exploding = doc.explode.is_some();

    // structural checks, first failure wins
    let shape_error = [
        (
            StreamShapeError::FieldsWithExplode,
            !doc.fields.is_empty() && (exploding || !doc.element.is_empty()),
        ),
        (
            StreamShapeError::ElementWithoutExplode,
            !exploding && !doc.element.is_empty(),
        ),
        (
            StreamShapeError::BadRetention,
            doc.retention
                .as_ref()
                .is_some_and(|r| r.days <= 0 || r.key.is_empty()),
        ),
    ]
    .into_iter()
    .find_map(|(error, failed)| failed.then_some(error));

    match shape_error {
        Some(error) => Err(err(error.message().to_string())),
        None => {
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
            super::validate::validate(&compiled, program, tables).map_err(&err)?;
            Ok(compiled)
        }
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
