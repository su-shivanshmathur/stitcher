//! `transformer.yaml` model + ahead-of-time compilation: DSL strings parsed once at boot,
//! fail fast. Versioned.

use std::{collections::BTreeMap, path::Path};

use error_stack::ResultExt;
use serde::Deserialize;
use stitcher_dsl::expr::{self, Expr};

use super::engine::{CompiledStream, Retention, Transform};
use stitcher::errors::{StitcherError, StitcherResult};
use stitcher::state::config::Program;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransformDoc {
    /// Bumped on every transform-logic change (audited).
    version: i64,
    #[serde(default)]
    stream: Vec<StreamDoc>,
}

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionDoc {
    days: i64,
    /// Row timestamp field (epoch seconds); must be creation-time-immutable so a row's
    /// −1/+1 pair always ships or is suppressed together.
    key: String,
}

const fn default_true() -> bool {
    true
}

/// Validated against the state schema and enrichment `tables` (`None` ⇒ `lookup` names unchecked).
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
        .and_then(|()| unique_stream_names(&doc.stream))
        .and_then(|()| {
            doc.stream
                .into_iter()
                .map(|stream| compile_stream(stream, program, tables))
                .collect::<Result<Vec<_>, _>>()
        })
        .map(Transform::new)
}

/// Names fold into delta envelope ids, so duplicates would collide two streams' deltas.
fn unique_stream_names(streams: &[StreamDoc]) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    streams
        .iter()
        .find_map(|stream| {
            if stream.name.is_empty() {
                Some("every [[stream]] needs a non-empty name".to_string())
            } else if seen.insert(stream.name.as_str()) {
                None
            } else {
                Some(format!(
                    "duplicate stream name {:?} (names must be unique)",
                    stream.name
                ))
            }
        })
        .map_or(Ok(()), Err)
}

/// A structural defect in a stream's shape (`fields` vs `explode`/`element`, retention bounds).
#[derive(Clone, Copy)]
enum StreamShapeError {
    FieldsWithExplode,
    ElementWithoutExplode,
    BadRetention,
    EmptyTopic,
    SignFlagReserved,
}

impl StreamShapeError {
    fn message(self) -> &'static str {
        match self {
            Self::FieldsWithExplode => {
                "use either `fields` (one row) or `explode` + `element` (per-entry rows)"
            }
            Self::ElementWithoutExplode => "`element` requires `explode`",
            Self::BadRetention => "retention.days must be > 0 and retention.key non-empty",
            Self::EmptyTopic => "topic must be non-empty",
            Self::SignFlagReserved => {
                "field name `sign_flag` is reserved while sign_flag=true (it is stamped onto every row)"
            }
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

    let row_src = if exploding { &doc.element } else { &doc.fields };
    let sign_flag_collision = doc.sign_flag
        && (doc.common.contains_key("sign_flag") || row_src.contains_key("sign_flag"));

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
        (StreamShapeError::EmptyTopic, doc.topic.is_empty()),
        (StreamShapeError::SignFlagReserved, sign_flag_collision),
    ]
    .into_iter()
    .find_map(|(error, failed)| failed.then_some(error));

    match shape_error {
        Some(error) => Err(err(error.message().to_string())),
        None => {
            let compiled = CompiledStream {
                name: doc.name,
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
