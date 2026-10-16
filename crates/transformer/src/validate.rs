//! Structural validation of a compiled stream against the state schema (`state ⊇ transformer`),
//! once at boot; payload/value interiors are opaque foreign JSON, trusted by convention.

use stitcher::state::config::{FieldProg, Program};
use stitcher_dsl::expr::{Builtin, Expr};

use super::engine::CompiledStream;

#[derive(Clone, Copy)]
enum FieldShape {
    Envelope,
    Map,
    Scalar,
    Opaque,
}

impl FieldShape {
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
    UnknownField,
    ScalarSubfield,
    BadEnvelopeKey,
    EachOutsideExplode,
    UnrootedPath,
    RootInTransform,
    ReduceNotMap,
    NotCollection,
    DynamicBuiltinArg,
    UnknownEnrichmentTable,
    GetOnList,
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
            Self::NotCollection => {
                "requires a `state.<keyed_map>` path or a collection-producing call (list/filter/parse_json/get/coalesce/if)"
            }
            Self::DynamicBuiltinArg => {
                "must be a string literal (a dynamic value can't be validated at boot)"
            }
            Self::UnknownEnrichmentTable => "is not a table in the enrichment join-map",
            Self::GetOnList => "is read by key from a list() array, which always resolves to null",
        }
    }
}

/// Provable but almost certainly a mistake — a boot error, reported distinctly from a contradiction.
#[derive(Clone, Copy)]
enum LintReason {
    LiteralMapKey,
    /// `lookup` in a gate: the verdict is recomputed for the OLD state on later transitions,
    /// so a reload flip would strand the emitted row.
    EnrichmentDependentGate,
}

impl LintReason {
    fn describe(self) -> &'static str {
        match self {
            Self::LiteralMapKey => {
                "indexes a keyed_map by a literal key; use latest()/first()/list() or `explode`"
            }
            Self::EnrichmentDependentGate => {
                "reads enrichment (lookup); a gate verdict must be time-stable — it is recomputed for the OLD state on later transitions, so a hot-reloaded map could flip it retroactively and strand the emitted row"
            }
        }
    }
}

/// Valid up to the frontier; the interior is foreign JSON trusted by convention (not a boot error).
#[derive(Clone, Copy)]
enum OpaqueReason {
    PayloadInterior,
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

/// A referenced path's standing; `Proven` yields no verdict.
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

enum FindingContext {
    Root,
    Path(String),
    Table(String),
    ReduceArgument,
    CollectionArgument,
    GetKey,
    FieldArg,
    LookupTableArg,
    Gate,
}

impl FindingContext {
    fn render(&self) -> String {
        match self {
            Self::Root => "`$`".to_string(),
            Self::Path(name) | Self::Table(name) => format!("`{name}`"),
            Self::ReduceArgument => "`latest/first/list`".to_string(),
            Self::CollectionArgument => "`collection argument`".to_string(),
            Self::GetKey => "`get` key".to_string(),
            Self::FieldArg => "`field` argument".to_string(),
            Self::LookupTableArg => "`lookup` table".to_string(),
            Self::Gate => "`gate`".to_string(),
        }
    }
}

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

/// Collects every problem in a single pass, joined; opaque crossings are logged, not failed.
/// `tables`: `None` ⇒ skip `lookup` table checks; `Some` (even empty) ⇒ every `lookup` table
/// must name a member.
pub(super) fn validate(
    stream: &CompiledStream,
    program: &Program,
    tables: Option<&[String]>,
) -> Result<(), String> {
    let exploding = stream.explode.is_some();
    // The explode target is a dependency precondition: `element`/`each` only make sense over a map.
    let explode_error = explode_target_error(stream.explode.as_ref(), program);
    // The gate runs before the explode (no `each` scope); key/common/row run per row.
    // A gate must not read enrichment: its verdict is recomputed for the OLD state on later
    // transitions, and a hot-reloaded `lookup` flip would strand the emitted row (FINDINGS #13).
    let gate_findings = stream.gate.iter().flat_map(|expr| {
        findings(expr, false, program, tables)
            .into_iter()
            .chain(contains_lookup(expr).then(|| {
                Finding::new(
                    FindingContext::Gate,
                    Verdict::Lint(LintReason::EnrichmentDependentGate),
                )
            }))
    });
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

fn contains_lookup(expr: &Expr) -> bool {
    match expr {
        Expr::Call(builtin, args) => {
            matches!(builtin, Builtin::Lookup) || args.iter().any(contains_lookup)
        }
        Expr::Not(inner) => contains_lookup(inner),
        Expr::Bin(_, lhs, rhs) => contains_lookup(lhs) || contains_lookup(rhs),
        _ => false,
    }
}

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

/// Reductions need a keyed_map path; `get`/`lookup` keys need string literals; everything
/// else recurses.
fn call_findings(
    builtin: Builtin,
    args: &[Expr],
    exploding: bool,
    program: &Program,
    tables: Option<&[String]>,
) -> Vec<Finding> {
    match builtin {
        Builtin::List => reduce_findings(args, exploding, program),
        Builtin::Latest | Builtin::First => match args {
            [_single] => reduce_findings(args, exploding, program),
            [coll, field] => {
                let field_error = (!matches!(field, Expr::Str(_))).then(|| {
                    Finding::new(
                        FindingContext::FieldArg,
                        Verdict::Contradicted(ContradictReason::DynamicBuiltinArg),
                    )
                });
                collection_arg_findings(coll, exploding, program, tables)
                    .into_iter()
                    .chain(field_error)
                    .collect()
            }
            _ => Vec::new(),
        },
        Builtin::Get => get_findings(args, exploding, program, tables),
        Builtin::Lookup => lookup_findings(args, exploding, program, tables),
        Builtin::Count => match args {
            [coll] => collection_arg_findings(coll, exploding, program, tables),
            _ => Vec::new(),
        },
        Builtin::Sum | Builtin::Min | Builtin::Max => match args {
            [coll, field] => {
                let field_error = (!matches!(field, Expr::Str(_))).then(|| {
                    Finding::new(
                        FindingContext::FieldArg,
                        Verdict::Contradicted(ContradictReason::DynamicBuiltinArg),
                    )
                });
                collection_arg_findings(coll, exploding, program, tables)
                    .into_iter()
                    .chain(field_error)
                    .collect()
            }
            _ => Vec::new(),
        },
        Builtin::Filter => match args {
            [coll, field, want] => {
                let field_error = (!matches!(field, Expr::Str(_))).then(|| {
                    Finding::new(
                        FindingContext::FieldArg,
                        Verdict::Contradicted(ContradictReason::DynamicBuiltinArg),
                    )
                });
                collection_arg_findings(coll, exploding, program, tables)
                    .into_iter()
                    .chain(field_error)
                    .chain(findings(want, exploding, program, tables))
                    .collect()
            }
            _ => Vec::new(),
        },
        _ => args
            .iter()
            .flat_map(|arg| findings(arg, exploding, program, tables))
            .collect(),
    }
}

/// Validates a collection argument for `count`/`sum`/`filter`/`min`/`max`:
/// a keyed_map path is fine; `filter`/`list`/`parse_json`/`get`/`coalesce`/`if` call results are
/// treated as opaque collections (their args are recursed); everything else is a shape error.
fn collection_arg_findings(
    coll: &Expr,
    exploding: bool,
    program: &Program,
    tables: Option<&[String]>,
) -> Vec<Finding> {
    match coll {
        Expr::Path(path) => {
            let path_verdict = classify_path(path, exploding, program)
                .map(|verdict| Finding::new(FindingContext::Path(path.clone()), verdict));
            let hard_error = path_verdict
                .as_ref()
                .is_some_and(|finding| finding.verdict.is_error());
            let shape_error = (!hard_error && !is_keyed_map_path(path, program)).then(|| {
                Finding::new(
                    FindingContext::Path(path.clone()),
                    Verdict::Contradicted(ContradictReason::NotCollection),
                )
            });
            path_verdict.into_iter().chain(shape_error).collect()
        }
        Expr::Call(
            Builtin::Filter
            | Builtin::List
            | Builtin::ParseJson
            | Builtin::Get
            | Builtin::Coalesce
            | Builtin::If,
            _,
        ) => findings(coll, exploding, program, tables),
        _ => vec![Finding::new(
            FindingContext::CollectionArgument,
            Verdict::Contradicted(ContradictReason::NotCollection),
        )],
    }
}

/// `latest`/`first`/`list` reduce a `state.<keyed_map>`. Otherwise (proven or merely opaque)
/// a non-keyed-map path is a shape error — an opaque payload interior like
/// `latest(state.f.payload.items)` is still not a map.
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
/// is structurally `path.k`, so classify the composed path; otherwise recurse the value.
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
        // get() on a latest/first result reaches into the reduced entry's opaque payload;
        // validate the map arg and record the crossing.
        [value @ Expr::Call(Builtin::Latest | Builtin::First, _), Expr::Str(key)] => {
            let opaque = Finding::new(
                FindingContext::Path(key.clone()),
                Verdict::Opaque(OpaqueReason::PayloadInterior),
            );
            findings(value, exploding, program, tables)
                .into_iter()
                .chain(std::iter::once(opaque))
                .collect()
        }
        // get() by key on a list()/filter() result (an array) always resolves to null — reject it.
        [value @ Expr::Call(Builtin::List | Builtin::Filter, _), Expr::Str(key)] => {
            let reject = Finding::new(
                FindingContext::Path(key.clone()),
                Verdict::Contradicted(ContradictReason::GetOnList),
            );
            findings(value, exploding, program, tables)
                .into_iter()
                .chain(std::iter::once(reject))
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

/// The table must be a string literal and, when an enrichment set is supplied (`Some`, even
/// empty), a member; the key is dynamic, so recurse it.
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

enum PathRoot {
    State,
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

fn single_state_field(path: &str) -> Option<&str> {
    path.strip_prefix("state.")
        .filter(|field| !field.contains('.'))
}

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

/// Surface the trusted-by-convention frontier crossings; shared by boot and `check-transformer`.
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
