//! DSL expression AST; parsed once by [`compile`], walked at runtime by `stitcher::eval`.

use pest::iterators::Pair;

use crate::grammar::{parse_expr, Rule};

/// How a `parse_time` input is interpreted; output is always epoch nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeFormat {
    Rfc3339,
    EpochSeconds,
    EpochMillis,
    EpochMicros,
    EpochNanos,
}

impl std::str::FromStr for TimeFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "rfc3339" => Ok(Self::Rfc3339),
            "epoch_seconds" => Ok(Self::EpochSeconds),
            "epoch_millis" => Ok(Self::EpochMillis),
            "epoch_micros" => Ok(Self::EpochMicros),
            "epoch_nanos" => Ok(Self::EpochNanos),
            other => Err(format!(
                "unknown time format {other:?}; expected one of: \
                 rfc3339, epoch_seconds, epoch_millis, epoch_micros, epoch_nanos"
            )),
        }
    }
}

/// Binary operator (v1 set; single precedence level, left-associative).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl BinOp {
    /// The comparison operator a `cmp_op` token denotes, if any (`&&`/`||` are folded
    /// implicitly, never tokens).
    #[must_use]
    pub fn from_comparison(token: &str) -> Option<Self> {
        Some(match token {
            "==" => Self::Eq,
            "!=" => Self::Ne,
            "<" => Self::Lt,
            "<=" => Self::Le,
            ">" => Self::Gt,
            ">=" => Self::Ge,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arity {
    Exact(usize),
    Range { min: usize, max: usize },
}

impl Arity {
    #[must_use]
    pub const fn allows(self, n: usize) -> bool {
        match self {
            Self::Exact(k) => n == k,
            Self::Range { min, max } => n >= min && n <= max,
        }
    }

    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::Exact(k) => format!("{k}"),
            Self::Range { min, max } => format!("{min}–{max}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Builtin {
    /// `parse_time(x)` or `parse_time(x, 'format')`
    ParseTime,
    Meaningful,
    /// `bucket(x, width)`
    Bucket,
    Round,
    Trim,
    Lower,
    /// `coalesce(a, b)`
    Coalesce,
    /// `latest(map)` or `latest(map, 'field')` — payload of the entry with the greatest stored
    /// comparator (1-arg) or greatest numeric `field` in the payload (2-arg).
    /// Note: the 2-arg `field` is read from inside the payload — it is not the envelope's stored
    /// `comparator`/`payload`; use the 1-arg form to rank by the stored comparator.
    Latest,
    /// `first(map)` or `first(map, 'field')` — payload of the entry with the smallest stored
    /// comparator (1-arg) or least numeric `field` in the payload (2-arg).
    First,
    /// `list(map)` — array of all entry payloads.
    List,
    /// `get(value, path)` — dotted-path lookup into a value.
    Get,
    /// `lookup(table, key)` — enrichment join (only meaningful in transform context).
    Lookup,
    /// `parse_json(x)` — parse a JSON string into a value (misses ⇒ null).
    ParseJson,
    /// `count(coll)` — entry count of a keyed_map or array.
    Count,
    /// `if(cond, then, else)` — one branch chosen by truthiness.
    If,
    /// `sum(coll, 'field')` — numeric sum of a field over the collection.
    Sum,
    /// `filter(coll, 'field', value)` — keep entries whose field equals value.
    Filter,
    /// `min(coll, 'field')` — least numeric field over the collection.
    Min,
    /// `max(coll, 'field')` — greatest numeric field over the collection.
    Max,
}

impl Builtin {
    /// Every builtin — the single source for name/arity lookups and diagnostics.
    const ALL: [Self; 19] = [
        Self::ParseTime,
        Self::Meaningful,
        Self::Bucket,
        Self::Round,
        Self::Trim,
        Self::Lower,
        Self::Coalesce,
        Self::Latest,
        Self::First,
        Self::List,
        Self::Get,
        Self::Lookup,
        Self::ParseJson,
        Self::Count,
        Self::If,
        Self::Sum,
        Self::Filter,
        Self::Min,
        Self::Max,
    ];

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ParseTime => "parse_time",
            Self::Meaningful => "meaningful",
            Self::Bucket => "bucket",
            Self::Round => "round",
            Self::Trim => "trim",
            Self::Lower => "lower",
            Self::Coalesce => "coalesce",
            Self::Latest => "latest",
            Self::First => "first",
            Self::List => "list",
            Self::Get => "get",
            Self::Lookup => "lookup",
            Self::ParseJson => "parse_json",
            Self::Count => "count",
            Self::If => "if",
            Self::Sum => "sum",
            Self::Filter => "filter",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    #[must_use]
    pub const fn arity(self) -> Arity {
        match self {
            Self::ParseTime => Arity::Range { min: 1, max: 2 },
            Self::Meaningful
            | Self::Round
            | Self::Trim
            | Self::Lower
            | Self::List
            | Self::ParseJson
            | Self::Count => Arity::Exact(1),
            Self::Latest | Self::First => Arity::Range { min: 1, max: 2 },
            Self::Bucket
            | Self::Coalesce
            | Self::Get
            | Self::Lookup
            | Self::Sum
            | Self::Min
            | Self::Max => Arity::Exact(2),
            Self::If | Self::Filter => Arity::Exact(3),
        }
    }
}

impl std::str::FromStr for Builtin {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|builtin| builtin.name() == name)
            .ok_or_else(|| {
                let names = Self::ALL
                    .iter()
                    .map(|builtin| builtin.name())
                    .collect::<Vec<_>>()
                    .join("/");
                format!("unknown built-in {name:?}; v1: {names}")
            })
    }
}

/// Compiled expression tree over `serde_json::Value` records.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// The whole record (`$`) — the event as sent.
    Root,
    /// Dotted record path (`log.a.b`); misses evaluate to `null`.
    Path(String),
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
    /// `!x` (truthiness negation).
    Not(Box<Self>),
    Bin(BinOp, Box<Self>, Box<Self>),
    /// Arity is checked at compile time.
    Call(Builtin, Vec<Self>),
}

/// Fails fast on absurd input; the pest call limit is the real stack guard.
const MAX_SOURCE_BYTES: usize = 64 * 1024;

/// Parse DSL source into an [`Expr`] (fails fast on syntax, unknown builtins, arity).
pub fn compile(src: &str) -> Result<Expr, String> {
    within_source_limit(src)?;
    let mut pairs = parse_expr(src).map_err(|e| fmt_parse_error(src, e))?;
    let expr = pairs
        .next()
        .ok_or_else(|| format!("empty expression {}", elide(src)))?;
    build(expr, 0)
}

/// Chars of source shown in error messages before eliding.
const ELIDE_HEAD: usize = 80;

/// Error-message source, elided past [`ELIDE_HEAD`] chars so long input can't bury the reason.
fn elide(src: &str) -> String {
    match src.char_indices().nth(ELIDE_HEAD) {
        // `cut` is a char boundary by construction; the empty default guards refactors.
        Some((cut, _)) => format!(
            "{:?}… ({} bytes)",
            src.get(..cut).unwrap_or_default(),
            src.len()
        ),
        None => format!("{src:?}"),
    }
}

/// pest's `Display` embeds the ENTIRE source line, so past the elide threshold print just the
/// variant message + position; short sources keep the caret rendering.
fn fmt_parse_error(src: &str, e: pest::error::Error<Rule>) -> String {
    if src.len() > ELIDE_HEAD {
        // `line_col` is a field (pest 2.9)
        let (line, col) = match e.line_col {
            pest::error::LineColLocation::Pos(pos) => pos,
            pest::error::LineColLocation::Span(pos, _) => pos,
        };
        format!(
            "DSL parse {} at {line}:{col}: {}",
            elide(src),
            e.variant.message()
        )
    } else {
        format!("DSL parse {}: {e}", elide(src))
    }
}

fn within_source_limit(src: &str) -> Result<(), String> {
    (src.len() <= MAX_SOURCE_BYTES)
        .then_some(())
        .ok_or_else(|| {
            format!(
                "expression too long ({} bytes; max {MAX_SOURCE_BYTES})",
                src.len()
            )
        })
}

/// The AST walk is recursive; this cap keeps it total (pest's call limit only guards parsing).
const MAX_DEPTH: u32 = 256;

fn build(pair: Pair<'_, Rule>, depth: u32) -> Result<Expr, String> {
    if depth > MAX_DEPTH {
        return Err(format!("expression nested too deeply (max {MAX_DEPTH})"));
    }
    match pair.as_rule() {
        // `file = { SOI ~ or ~ EOI }`; descend to the top expression
        Rule::file => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty expression".to_string())?;
            build(inner, depth + 1)
        }
        // `||`/`&&`: left-associative folds
        Rule::or => fold_binary(pair, BinOp::Or, depth),
        Rule::and => fold_binary(pair, BinOp::And, depth),
        // non-associative: `term (cmp_op term)?`, ≤ 1 operator
        Rule::cmp => {
            let mut inner = pair.into_inner();
            let lhs = build(inner.next().ok_or("empty cmp")?, depth + 1)?;
            match inner.next() {
                Some(op) => {
                    let rhs = build(
                        inner.next().ok_or("comparison missing right operand")?,
                        depth + 1,
                    )?;
                    Ok(Expr::Bin(
                        cmp_op(op.as_str())?,
                        Box::new(lhs),
                        Box::new(rhs),
                    ))
                }
                None => Ok(lhs),
            }
        }
        Rule::term | Rule::literal => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty term".to_string())?;
            build(inner, depth + 1)
        }
        Rule::unary => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty unary".to_string())?;
            Ok(Expr::Not(Box::new(build(inner, depth + 1)?)))
        }
        Rule::string => {
            let raw = pair.as_str();
            let content = raw
                .strip_prefix(|c| c == '\'' || c == '"')
                .and_then(|s| s.strip_suffix(|c| c == '\'' || c == '"'))
                .unwrap_or(raw);
            Ok(Expr::Str(content.to_string()))
        }
        Rule::number => {
            let text = pair.as_str();
            if text.contains('.') {
                let lit: f64 = text.parse().map_err(|_| format!("bad number {text:?}"))?;
                Ok(Expr::Float(lit))
            } else {
                let lit: i64 = text.parse().map_err(|_| format!("bad number {text:?}"))?;
                Ok(Expr::Int(lit))
            }
        }
        Rule::bool => Ok(Expr::Bool(pair.as_str() == "true")),
        Rule::null => Ok(Expr::Null),
        Rule::root => Ok(Expr::Root),
        Rule::path => Ok(Expr::Path(pair.as_str().to_string())),
        Rule::call => {
            let mut inner = pair.into_inner();
            let name = inner
                .next()
                .ok_or_else(|| "call without callee".to_string())?
                .as_str();
            let args: Vec<Expr> = inner
                .map(|arg| build(arg, depth + 1))
                .collect::<Result<_, _>>()?;
            let builtin: Builtin = name.parse()?;
            if !builtin.arity().allows(args.len()) {
                return Err(format!(
                    "{} expects {} arg(s), got {}",
                    builtin.name(),
                    builtin.arity().describe(),
                    args.len()
                ));
            }
            // unknown parse_time format literals are rejected at compile time
            if builtin == Builtin::ParseTime {
                if let Some(Expr::Str(fmt)) = args.get(1) {
                    fmt.parse::<TimeFormat>()?;
                }
            }
            Ok(Expr::Call(builtin, args))
        }
        other => Err(format!("unexpected grammar rule: {other:?}")),
    }
}

/// Left-associative fold of a layer's same-operator children; a lone child passes through.
fn fold_binary(pair: Pair<'_, Rule>, op: BinOp, depth: u32) -> Result<Expr, String> {
    let mut inner = pair.into_inner();
    let mut acc = build(inner.next().ok_or("empty binary layer")?, depth + 1)?;
    for child in inner {
        acc = Expr::Bin(op, Box::new(acc), Box::new(build(child, depth + 1)?));
    }
    Ok(acc)
}

fn cmp_op(op: &str) -> Result<BinOp, String> {
    BinOp::from_comparison(op).ok_or_else(|| format!("unsupported comparison operator {op:?}"))
}
