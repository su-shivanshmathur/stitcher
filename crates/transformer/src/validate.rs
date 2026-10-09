//! Node-type-aware structural validation of a compiled stream against the state schema
//! `Program` (TRANSFORMER_ROADMAP.md Phase 1). Enforces the invariant **state ⊇ transformer**:
//! every path a stream reads must be producible by the schema, checked once at boot (fail fast),
//! never per-record.
//!
//! The frontier is fixed at top-level field keys plus the one envelope level the merge-node type
//! guarantees (`comparator`/`payload` for `latest_by`/`keyed_map`); payload/value interiors are
//! opaque foreign JSON, trusted by convention, not validated. Replaces the old head-only check,
//! which classified only the first path segment and ignored node type.

use stitcher::state::config::{FieldProg, Program};
use stitcher_dsl::expr::{Builtin, Expr};

use super::engine::CompiledStream;

/// What the state schema guarantees about a top-level field's stored shape.
#[derive(Clone, Copy)]
enum FieldShape {
    /// `latest_by`: `{comparator: i64ns, payload: <expr>}`.
    Envelope,
    /// `keyed_map`: `{<key>: {comparator, payload}, …}` — a reduce/explode target.
    Map,
    /// `counter`: an `i64` scalar.
    Scalar,
    /// `last`/`once`: a raw value whose interior shape the schema does not know.
    Opaque,
}

impl FieldShape {
    /// The shape a compiled field node exposes to the transformer.
    fn of(field: &FieldProg) -> Self {
        match field {
            FieldProg::LatestBy { .. } => Self::Envelope,
            FieldProg::KeyedMap { .. } => Self::Map,
            FieldProg::Counter { .. } => Self::Scalar,
            FieldProg::Last { .. } | FieldProg::Once { .. } => Self::Opaque,
        }
    }
}

/// The schema disproves this reference — always a boot error.
#[derive(Clone, Copy)]
enum ContradictReason {
    /// Root field is not in the schema.
    UnknownField,
    /// Reaching past a scalar (a `counter`, or an envelope `comparator`) which has no sub-fields.
    ScalarSubfield,
    /// An envelope's second segment is neither `comparator` nor `payload`.
    BadEnvelopeKey,
    /// `each` used outside an `explode` stream.
    EachOutsideExplode,
    /// A path rooted at neither `state` nor `each`.
    UnrootedPath,
    /// `$` (whole-record) — resolves to null in a transformer; an author error.
    RootInTransform,
    /// `latest`/`first`/`list` over something that is not a `state.<keyed_map>` path.
    ReduceNotMap,
    /// A `get`/`lookup` argument that must be a string literal but is a dynamic expression.
    DynamicBuiltinArg,
    /// A `lookup` table name absent from the enrichment join-map.
    UnknownEnrichmentTable,
}

impl ContradictReason {
    fn describe(self) -> &'static str {
        match self {
            Self::UnknownField => "references an unknown state field",
            Self::ScalarSubfield => {
                "reaches past a scalar (counter/comparator) which has no sub-fields"
            }
            Self::BadEnvelopeKey => {
                "a latest_by/keyed_map entry stores only `comparator` and `payload`"
            }
            Self::EachOutsideExplode => {
                "`each` is valid only in an explode stream's per-row expressions (not the gate)"
            }
            Self::UnrootedPath => "must be rooted at `state.` or `each.`",
            Self::RootInTransform => {
                "`$` resolves to null in a transformer; root at `state` instead"
            }
            Self::ReduceNotMap => {
                "latest()/first()/list() require a `state.<keyed_map>` path argument"
            }
            Self::DynamicBuiltinArg => {
                "must be a string literal (a dynamic value can't be validated at boot)"
            }
            Self::UnknownEnrichmentTable => "is not a table in the enrichment join-map",
        }
    }
}

/// Provable but almost certainly a mistake — a boot error, reported distinctly from a contradiction.
#[derive(Clone, Copy)]
enum LintReason {
    /// A literal key into a `keyed_map` (a dynamic map); use `latest`/`first`/`list` or `explode`.
    LiteralMapKey,
}

impl LintReason {
    fn describe(self) -> &'static str {
        match self {
            Self::LiteralMapKey => {
                "indexes a keyed_map by a literal key; use latest()/first()/list() or `explode`"
            }
        }
    }
}

/// Valid up to the frontier; the interior is foreign JSON trusted by convention (not a boot error).
#[derive(Clone, Copy)]
enum OpaqueReason {
    /// Reaching into a `latest_by`/`keyed_map` `payload` (a foreign event stored as-sent).
    PayloadInterior,
    /// Reaching into a `last`/`once` value whose interior the schema does not know.
    OpaqueFieldInterior,
}

impl OpaqueReason {
    fn describe(self) -> &'static str {
        match self {
            Self::PayloadInterior => {
                "reaches into an opaque stored payload (trusted by convention)"
            }
            Self::OpaqueFieldInterior => {
                "reaches into an opaque last/once value (trusted by convention)"
            }
        }
    }
}

/// A referenced path's standing against the schema. A `Proven` reference yields no `Verdict`.
#[derive(Clone, Copy)]
enum Verdict {
    /// Trusted frontier crossing — surfaced for observability, not an error.
    Opaque(OpaqueReason),
    /// Provable mistake.
    Lint(LintReason),
    /// Schema contradiction.
    Contradicted(ContradictReason),
}

impl Verdict {
    /// Lints and contradictions fail boot; opaque crossings are only surfaced.
    fn is_error(&self) -> bool {
        matches!(self, Self::Contradicted(_) | Self::Lint(_))
    }

    fn describe(&self) -> &'static str {
        match self {
            Self::Opaque(reason) => reason.describe(),
            Self::Lint(reason) => reason.describe(),
            Self::Contradicted(reason) => reason.describe(),
        }
    }
}

/// What a finding points at, for diagnostics.
enum FindingContext {
    /// `$` (whole-record root).
    Root,
    /// A dotted path, as written.
    Path(String),
    /// A `lookup` table name.
    Table(String),
    /// The argument of a `latest`/`first`/`list` reduction.
    ReduceArgument,
    /// The key argument of `get`.
    GetKey,
    /// The table argument of `lookup`.
    LookupTableArg,
}

impl FindingContext {
    fn render(&self) -> String {
        match self {
            Self::Root => "`$`".to_string(),
            Self::Path(name) | Self::Table(name) => format!("`{name}`"),
            Self::ReduceArgument => "`latest/first/list`".to_string(),
            Self::GetKey => "`get` key".to_string(),
            Self::LookupTableArg => "`lookup` table".to_string(),
        }
    }
}

/// One classified reference: what was read (for diagnostics) and its verdict.
struct Finding {
    context: FindingContext,
    verdict: Verdict,
}

impl Finding {
    fn new(context: FindingContext, verdict: Verdict) -> Self {
        Self { context, verdict }
    }

    fn message(&self) -> String {
        format!("{} {}", self.context.render(), self.verdict.describe())
    }
}

/// Validate one compiled stream against the schema. Collects every problem in a single pass and
/// returns them joined; opaque frontier crossings are logged, not failed. `tables` is the
/// enrichment table set: `None` ⇒ no enrichment supplied (skip `lookup` table checks), `Some`
/// (even empty) ⇒ a configured set every `lookup` table must name.
pub(super) fn validate(
    stream: &CompiledStream,
    program: &Program,
    tables: Option<&[String]>,
) -> Result<(), String> {
    let exploding = stream.explode.is_some();
    // The explode target is a dependency precondition: `element`/`each` only make sense over a map.
    let explode_error = explode_target_error(stream.explode.as_ref(), program);
    // The gate is evaluated before the explode (no `each` element), so it never has `each` scope;
    // key/common/row are evaluated per exploded row, so `each` is in scope when exploding.
    let gate_findings = stream
        .gate
        .iter()
        .flat_map(|expr| findings(expr, false, program, tables));
    let row_findings = stream
        .key
        .iter()
        .chain(stream.common.iter().map(|(_, expr)| expr))
        .chain(stream.row.iter().map(|(_, expr)| expr))
        .flat_map(|expr| findings(expr, exploding, program, tables));
    let all_findings: Vec<Finding> = gate_findings.chain(row_findings).collect();

    log_opaque_surface(&all_findings);

    let errors: Vec<String> = explode_error
        .into_iter()
        .chain(
            all_findings
                .iter()
                .filter(|finding| finding.verdict.is_error())
                .map(Finding::message),
        )
        .collect();
    errors
        .is_empty()
        .then_some(())
        .ok_or_else(|| errors.join("; "))
}

/// Classify every path and builtin use in one expression tree.
fn findings(
    expr: &Expr,
    exploding: bool,
    program: &Program,
    tables: Option<&[String]>,
) -> Vec<Finding> {
    match expr {
        Expr::Root => vec![Finding::new(
            FindingContext::Root,
            Verdict::Contradicted(ContradictReason::RootInTransform),
        )],
        Expr::Path(path) => classify_path(path, exploding, program)
            .map(|verdict| Finding::new(FindingContext::Path(path.clone()), verdict))
            .into_iter()
            .collect(),
        Expr::Not(inner) => findings(inner, exploding, program, tables),
        Expr::Bin(_, lhs, rhs) => findings(lhs, exploding, program, tables)
            .into_iter()
            .chain(findings(rhs, exploding, program, tables))
            .collect(),
        Expr::Call(builtin, args) => call_findings(*builtin, args, exploding, program, tables),
        Expr::Str(_) | Expr::Int(_) | Expr::Float(_) | Expr::Bool(_) | Expr::Null => Vec::new(),
    }
}

/// Builtin-shape checks: reductions need a keyed_map path; `get`/`lookup` keys need string
/// literals; everything else is structurally transparent (recurse into its arguments).
fn call_findings(
    builtin: Builtin,
    args: &[Expr],
    exploding: bool,
    program: &Program,
    tables: Option<&[String]>,
) -> Vec<Finding> {
    match builtin {
        Builtin::Latest | Builtin::First | Builtin::List => {
            reduce_findings(args, exploding, program)
        }
        Builtin::Get => get_findings(args, exploding, program, tables),
        Builtin::Lookup => lookup_findings(args, exploding, program, tables),
        _ => args
            .iter()
            .flat_map(|arg| findings(arg, exploding, program, tables))
            .collect(),
    }
}

/// `latest`/`first`/`list` reduce a `state.<keyed_map>`. A hard-error path reports its own, more
/// specific verdict; otherwise (proven or merely opaque) a non-keyed-map path is a shape error — an
/// opaque payload interior like `latest(state.f.payload.items)` is still not a map.
fn reduce_findings(args: &[Expr], exploding: bool, program: &Program) -> Vec<Finding> {
    match args {
        [Expr::Path(path)] => {
            let path_verdict = classify_path(path, exploding, program)
                .map(|verdict| Finding::new(FindingContext::Path(path.clone()), verdict));
            let hard_error = path_verdict
                .as_ref()
                .is_some_and(|finding| finding.verdict.is_error());
            let shape_error = (!hard_error && !is_keyed_map_path(path, program)).then(|| {
                Finding::new(
                    FindingContext::Path(path.clone()),
                    Verdict::Contradicted(ContradictReason::ReduceNotMap),
                )
            });
            path_verdict.into_iter().chain(shape_error).collect()
        }
        _ => vec![Finding::new(
            FindingContext::ReduceArgument,
            Verdict::Contradicted(ContradictReason::ReduceNotMap),
        )],
    }
}

/// `get(value, key)`: the key must be a string literal. When `value` is a path, `get(path, 'k')`
/// is structurally `path.k`, so classify the composed path (counter/envelope/opaque rules all
/// apply); otherwise recurse the value expression.
fn get_findings(
    args: &[Expr],
    exploding: bool,
    program: &Program,
    tables: Option<&[String]>,
) -> Vec<Finding> {
    match args {
        [Expr::Path(path), Expr::Str(key)] => {
            let composed = format!("{path}.{key}");
            classify_path(&composed, exploding, program)
                .map(|verdict| Finding::new(FindingContext::Path(composed), verdict))
                .into_iter()
                .collect()
        }
        [value, key] => {
            let key_error = (!matches!(key, Expr::Str(_))).then(|| {
                Finding::new(
                    FindingContext::GetKey,
                    Verdict::Contradicted(ContradictReason::DynamicBuiltinArg),
                )
            });
            findings(value, exploding, program, tables)
                .into_iter()
                .chain(key_error)
                .collect()
        }
        _ => Vec::new(),
    }
}

/// `lookup(table, key)` joins enrichment, not state: the table must be a string literal; when an
/// enrichment set is supplied (`Some`, even empty) it must name a member; the key is a dynamic join
/// value, so recurse it.
fn lookup_findings(
    args: &[Expr],
    exploding: bool,
    program: &Program,
    tables: Option<&[String]>,
) -> Vec<Finding> {
    match args {
        [table, key] => {
            let table_error = (!matches!(table, Expr::Str(_))).then(|| {
                Finding::new(
                    FindingContext::LookupTableArg,
                    Verdict::Contradicted(ContradictReason::DynamicBuiltinArg),
                )
            });
            let unknown_table = match (table, tables) {
                (Expr::Str(name), Some(known)) if !known.iter().any(|table| table == name) => {
                    Some(Finding::new(
                        FindingContext::Table(name.clone()),
                        Verdict::Contradicted(ContradictReason::UnknownEnrichmentTable),
                    ))
                }
                _ => None,
            };
            table_error
                .into_iter()
                .chain(unknown_table)
                .chain(findings(key, exploding, program, tables))
                .collect()
        }
        _ => Vec::new(),
    }
}

/// The legal roots of a transformer path.
enum PathRoot {
    /// `state.…` — the merged state.
    State,
    /// `each.…` — the current exploded element.
    Each,
}

impl PathRoot {
    fn parse(segment: &str) -> Option<Self> {
        match segment {
            "state" => Some(Self::State),
            "each" => Some(Self::Each),
            _ => None,
        }
    }
}

/// Classify a dotted path by its root and the shape of the field it names.
fn classify_path(path: &str, exploding: bool, program: &Program) -> Option<Verdict> {
    let parts: Vec<&str> = path.split('.').collect();
    match parts.split_first() {
        Some((&root, rest)) => match PathRoot::parse(root) {
            Some(PathRoot::State) => {
                if rest.is_empty() {
                    None
                } else {
                    classify_state_field(rest, program)
                }
            }
            Some(PathRoot::Each) => {
                if exploding {
                    classify_envelope_access(rest)
                } else {
                    Some(Verdict::Contradicted(ContradictReason::EachOutsideExplode))
                }
            }
            None => Some(Verdict::Contradicted(ContradictReason::UnrootedPath)),
        },
        None => Some(Verdict::Contradicted(ContradictReason::UnrootedPath)),
    }
}

/// Classify a `state.<head>…` reference by the head field's shape.
fn classify_state_field(segments: &[&str], program: &Program) -> Option<Verdict> {
    match segments.split_first() {
        Some((head, rest)) => program.fields.get(*head).map_or(
            Some(Verdict::Contradicted(ContradictReason::UnknownField)),
            |field| match FieldShape::of(field) {
                FieldShape::Envelope => classify_envelope_access(rest),
                FieldShape::Map => {
                    (!rest.is_empty()).then_some(Verdict::Lint(LintReason::LiteralMapKey))
                }
                FieldShape::Scalar => (!rest.is_empty())
                    .then_some(Verdict::Contradicted(ContradictReason::ScalarSubfield)),
                FieldShape::Opaque => {
                    (!rest.is_empty()).then_some(Verdict::Opaque(OpaqueReason::OpaqueFieldInterior))
                }
            },
        ),
        None => None,
    }
}

/// The two sub-keys the schema guarantees inside a `{comparator, payload}` entry.
enum EnvelopeKey {
    /// `comparator` — an `i64` scalar.
    Comparator,
    /// `payload` — the opaque stored value.
    Payload,
}

impl EnvelopeKey {
    fn parse(segment: &str) -> Option<Self> {
        match segment {
            "comparator" => Some(Self::Comparator),
            "payload" => Some(Self::Payload),
            _ => None,
        }
    }
}

/// Classify the segments reaching into a `{comparator, payload}` envelope (a `latest_by` field or
/// an exploded `each` element). `payload` deeper than one level crosses the opaque frontier.
fn classify_envelope_access(segments: &[&str]) -> Option<Verdict> {
    match segments.split_first() {
        None => None,
        Some((&head, rest)) => match EnvelopeKey::parse(head) {
            Some(EnvelopeKey::Payload) => {
                (!rest.is_empty()).then_some(Verdict::Opaque(OpaqueReason::PayloadInterior))
            }
            Some(EnvelopeKey::Comparator) => (!rest.is_empty())
                .then_some(Verdict::Contradicted(ContradictReason::ScalarSubfield)),
            None => Some(Verdict::Contradicted(ContradictReason::BadEnvelopeKey)),
        },
    }
}

/// The single `state.<field>` a path names, if it is exactly one field segment.
fn single_state_field(path: &str) -> Option<&str> {
    path.strip_prefix("state.")
        .filter(|field| !field.contains('.'))
}

/// Whether a path is a bare `state.<field>` naming a `keyed_map`.
fn is_keyed_map_path(path: &str, program: &Program) -> bool {
    single_state_field(path)
        .and_then(|field| program.fields.get(field))
        .is_some_and(|field| matches!(field, FieldProg::KeyedMap { .. }))
}

/// Why an `explode` target is invalid (it must be a single `state.<keyed_map>` path).
enum ExplodeError<'a> {
    /// Not a single `state.<field>` path expression.
    NotSingleStatePath,
    /// Names a field the schema doesn't have.
    UnknownField(&'a str),
    /// Names a field that isn't a `keyed_map`.
    NotKeyedMap(&'a str),
}

impl ExplodeError<'_> {
    fn message(&self) -> String {
        match self {
            Self::NotSingleStatePath => "explode must be a single `state.<field>` path".to_string(),
            Self::UnknownField(field) => {
                format!("explode references unknown state field {field:?}")
            }
            Self::NotKeyedMap(field) => format!("explode field {field:?} must be a keyed_map"),
        }
    }
}

/// An `explode` target must be a single `state.<keyed_map>` path; otherwise a boot error.
fn explode_target_error(explode: Option<&Expr>, program: &Program) -> Option<String> {
    explode
        .and_then(|expr| explode_defect(expr, program))
        .map(|defect| defect.message())
}

fn explode_defect<'a>(expr: &'a Expr, program: &Program) -> Option<ExplodeError<'a>> {
    match expr {
        Expr::Path(path) => match single_state_field(path) {
            Some(field) => match program.fields.get(field) {
                Some(FieldProg::KeyedMap { .. }) => None,
                Some(_) => Some(ExplodeError::NotKeyedMap(field)),
                None => Some(ExplodeError::UnknownField(field)),
            },
            None => Some(ExplodeError::NotSingleStatePath),
        },
        _ => Some(ExplodeError::NotSingleStatePath),
    }
}

/// Surface the trusted-by-convention frontier crossings (Phase 2 will formalize this into the
/// `check-transformer` CLI). A down-payment: operators see the exact unverified set at boot.
fn log_opaque_surface(all_findings: &[Finding]) {
    let opaque: Vec<String> = all_findings
        .iter()
        .filter(|finding| matches!(finding.verdict, Verdict::Opaque(_)))
        .map(Finding::message)
        .collect();
    if !opaque.is_empty() {
        tracing::info!(
            count = opaque.len(),
            paths = ?opaque,
            "transformer: unverified opaque surface (trusted by convention)"
        );
    }
}
