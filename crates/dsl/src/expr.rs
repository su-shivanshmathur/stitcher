//! DSL expression AST: [`compile`] parses source once into a tree evaluated at runtime by
//! the shared `stitcher::eval` walker.

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
    /// `==`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `&&`
    And,
    /// `||`
    Or,
}

impl BinOp {
    /// The comparison operator a `cmp_op` grammar token denotes, if any. `&&`/`||`
    /// are folded implicitly (never tokens), so they are never produced here.
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

/// A builtin's argument-count constraint.
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

/// Built-in call (v1 vocabulary: `parse_time/meaningful/bucket/round/trim/lower/coalesce`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Builtin {
    /// `parse_time(x)` or `parse_time(x, 'format')`
    ParseTime,
    /// `meaningful(x)`
    Meaningful,
    /// `bucket(x, width)`
    Bucket,
    /// `round(x)`
    Round,
    /// `trim(x)`
    Trim,
    /// `lower(x)`
    Lower,
    /// `coalesce(a, b)`
    Coalesce,
    /// `latest(map)` — payload of the entry with the greatest stored comparator.
    Latest,
    /// `first(map)` — payload of the entry with the smallest stored comparator.
    First,
    /// `list(map)` — array of all entry payloads.
    List,
    /// `get(value, path)` — dotted-path lookup into a value.
    Get,
    /// `lookup(table, key)` — enrichment join (only meaningful in transform context).
    Lookup,
}

impl Builtin {
    /// Every builtin — the single source for name/arity lookups and diagnostics.
    const ALL: [Self; 12] = [
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
    ];

    /// Canonical name as written in the DSL.
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
        }
    }

    /// Arity constraint for the builtin.
    #[must_use]
    pub const fn arity(self) -> Arity {
        match self {
            Self::ParseTime => Arity::Range { min: 1, max: 2 },
            Self::Meaningful
            | Self::Round
            | Self::Trim
            | Self::Lower
            | Self::Latest
            | Self::First
            | Self::List => Arity::Exact(1),
            Self::Bucket | Self::Coalesce | Self::Get | Self::Lookup => Arity::Exact(2),
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
    /// The whole record (`$`) — the event as sent, for flat schemas with no envelope.
    Root,
    /// Dotted record path (`log.a.b`); misses evaluate to `null`.
    Path(String),
    /// String literal.
    Str(String),
    /// Integer literal.
    Int(i64),
    /// Float literal.
    Float(f64),
    /// Boolean literal.
    Bool(bool),
    /// `null` literal.
    Null,
    /// `!x` (truthiness negation).
    Not(Box<Self>),
    /// Binary operation (left-associative fold).
    Bin(BinOp, Box<Self>, Box<Self>),
    /// Built-in call; arity is checked at compile time.
    Call(Builtin, Vec<Self>),
}

/// Defensive cap on raw source length. The pest call limit in [`crate::grammar`] is the real
/// stack guard against pathological nesting; this just fails faster and more legibly on absurd
/// input and bounds parse memory. Any real expression is orders of magnitude under this.
const MAX_SOURCE_BYTES: usize = 64 * 1024;

/// Parse DSL source into an [`Expr`] (fails fast on syntax, unknown builtins, arity).
pub fn compile(src: &str) -> Result<Expr, String> {
    within_source_limit(src)?;
    let mut pairs = parse_expr(src).map_err(|e| format!("DSL parse {}: {e}", elide(src)))?;
    let expr = pairs
        .next()
        .ok_or_else(|| format!("empty expression {}", elide(src)))?;
    build(expr, 0)
}

/// Source for an error message, shortened so a long expression can't bury the actual reason
/// (e.g. pest's "call limit reached"). Short sources are shown whole.
fn elide(src: &str) -> String {
    const HEAD: usize = 80;
    match src.char_indices().nth(HEAD) {
        Some((cut, _)) => format!("{:?}… ({} bytes)", src.get(..cut).unwrap_or(src), src.len()),
        None => format!("{src:?}"),
    }
}

/// Reject absurdly long source before parsing (see [`MAX_SOURCE_BYTES`]).
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

/// Belt-and-braces cap on the AST walk's own recursion. The pest call limit already fails deep
/// nesting during parsing, so `build` never sees a tree this deep in practice — but the walk is
/// recursive, so the cap keeps it total regardless of how the grammar evolves. Real expressions
/// sit far below this.
const MAX_DEPTH: u32 = 256;

/// pest-pair → AST.
fn build(pair: Pair<'_, Rule>, depth: u32) -> Result<Expr, String> {
    if depth > MAX_DEPTH {
        return Err(format!("expression nested too deeply (max {MAX_DEPTH})"));
    }
    match pair.as_rule() {
        // entry wrapper (`file = { SOI ~ or ~ EOI }`); descend to the top expression
        Rule::file => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty expression".to_string())?;
            build(inner, depth + 1)
        }
        // `||` / `&&` layers: left-associative folds over same-operator children
        // (the operator literals are implicit — every child is joined the same way).
        Rule::or => fold_binary(pair, BinOp::Or, depth),
        Rule::and => fold_binary(pair, BinOp::And, depth),
        // comparison layer: `term (cmp_op term)?` — non-associative, ≤ 1 operator.
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
            // reject an unknown parse_time format literal at compile time
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

/// Left-associative fold of a precedence layer's same-operator children
/// (`or` → `||`, `and` → `&&`). A lone child passes through un-wrapped so a bare
/// term never gains a spurious `Bin` node.
fn fold_binary(pair: Pair<'_, Rule>, op: BinOp, depth: u32) -> Result<Expr, String> {
    let mut inner = pair.into_inner();
    let mut acc = build(inner.next().ok_or("empty binary layer")?, depth + 1)?;
    for child in inner {
        acc = Expr::Bin(op, Box::new(acc), Box::new(build(child, depth + 1)?));
    }
    Ok(acc)
}

/// Map a `cmp_op` token to its [`BinOp`] (the mapping lives on `BinOp`).
fn cmp_op(op: &str) -> Result<BinOp, String> {
    BinOp::from_comparison(op).ok_or_else(|| format!("unsupported comparison operator {op:?}"))
}
