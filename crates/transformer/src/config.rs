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
    /// Row timestamp field (epoch seconds) for the retention window. MUST be a creation-time
    /// (immutable) field such as `created_at`: a row's −1 retract and +1 emit share this value, so
    /// retention ships or suppresses the pair together. A mutable key could straddle the window and
    /// strand a half-pair downstream. The same immutability is what makes a gate-close retract
    /// safe to decide on the OLD payload's current freshness (fresh-now ⇒ fresh-at-emit, by
    /// backwards monotonicity). Note: a row that ages out after being emitted is never retracted
    /// (no lone −1) — it lingers, so accumulation-only consumers need their own window.
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
        .and_then(|()| unique_stream_names(&doc.stream))
        .and_then(|()| {
            doc.stream
                .into_iter()
                .map(|stream| compile_stream(stream, program, tables))
                .collect::<Result<Vec<_>, _>>()
        })
        .map(Transform::new)
}

/// Stream names must be non-empty and globally unique: each is folded into the delta envelope ids,
/// so a duplicate would collide two streams' deltas on the same topic.
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

    // the row-field map depends on explode mode (see below); the reserved-name guard scans both it
    // and `common`, since `sign_flag` is stamped onto every emitted row when sign_flag=true.
    let row_src = if exploding { &doc.element } else { &doc.fields };
    let sign_flag_collision = doc.sign_flag
        && (doc.common.contains_key("sign_flag") || row_src.contains_key("sign_flag"));

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
        (StreamShapeError::EmptyTopic, doc.topic.is_empty()),
        (StreamShapeError::SignFlagReserved, sign_flag_collision),
    ]
    .into_iter()
    .find_map(|(error, failed)| failed.then_some(error));

    match shape_error {
        Some(error) => Err(err(error.message().to_string())),
        None => {
            // non-explode: `fields` are the row; explode: `element` are the per-entry row
            // (`row_src` is bound above, next to the reserved-name guard that also reads it).
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
