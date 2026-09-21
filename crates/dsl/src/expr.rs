//! DSL expression AST: [`compile`] parses source once into a tree evaluated at runtime by
//! the shared `stitcher::eval` walker.

use pest::iterators::Pair;

use crate::grammar::{parse_expr, Rule};

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

/// Built-in call (v1 vocabulary: `parse_time/meaningful/bucket/round/trim/lower/coalesce`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Builtin {
    /// `parse_time(x)`
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

    /// Number of arguments the builtin accepts.
    #[must_use]
    pub const fn arity(self) -> usize {
        match self {
            Self::ParseTime
            | Self::Meaningful
            | Self::Round
            | Self::Trim
            | Self::Lower
            | Self::Latest
            | Self::First
            | Self::List => 1,
            Self::Bucket | Self::Coalesce | Self::Get | Self::Lookup => 2,
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

/// Parse DSL source into an [`Expr`] (fails fast on syntax, unknown builtins, arity).
pub fn compile(src: &str) -> Result<Expr, String> {
    let mut pairs = parse_expr(src).map_err(|e| format!("DSL parse {src:?}: {e}"))?;
    let expr = pairs
        .next()
        .ok_or_else(|| format!("empty expression {src:?}"))?;
    build(expr)
}

/// pest-pair → AST.
fn build(pair: Pair<'_, Rule>) -> Result<Expr, String> {
    match pair.as_rule() {
        // entry wrapper (`file = { SOI ~ or ~ EOI }`); descend to the top expression
        Rule::file => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty expression".to_string())?;
            build(inner)
        }
        // `||` / `&&` layers: left-associative folds over same-operator children
        // (the operator literals are implicit — every child is joined the same way).
        Rule::or => fold_binary(pair, BinOp::Or),
        Rule::and => fold_binary(pair, BinOp::And),
        // comparison layer: `term (cmp_op term)?` — non-associative, ≤ 1 operator.
        Rule::cmp => {
            let mut inner = pair.into_inner();
            let lhs = build(inner.next().ok_or("empty cmp")?)?;
            match inner.next() {
                Some(op) => {
                    let rhs = build(inner.next().ok_or("comparison missing right operand")?)?;
                    Ok(Expr::Bin(cmp_op(op.as_str())?, Box::new(lhs), Box::new(rhs)))
                }
                None => Ok(lhs),
            }
        }
        Rule::term | Rule::literal => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty term".to_string())?;
            build(inner)
        }
        Rule::unary => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| "empty unary".to_string())?;
            Ok(Expr::Not(Box::new(build(inner)?)))
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
            let args: Vec<Expr> = inner.map(build).collect::<Result<_, _>>()?;
            let builtin: Builtin = name.parse()?;
            if args.len() != builtin.arity() {
                return Err(format!(
                    "{} expects {} arg(s), got {}",
                    builtin.name(),
                    builtin.arity(),
                    args.len()
                ));
            }
            Ok(Expr::Call(builtin, args))
        }
        other => Err(format!("unexpected grammar rule: {other:?}")),
    }
}

/// Left-associative fold of a precedence layer's same-operator children
/// (`or` → `||`, `and` → `&&`). A lone child passes through un-wrapped so a bare
/// term never gains a spurious `Bin` node.
fn fold_binary(pair: Pair<'_, Rule>, op: BinOp) -> Result<Expr, String> {
    let mut inner = pair.into_inner();
    let mut acc = build(inner.next().ok_or("empty binary layer")?)?;
    for child in inner {
        acc = Expr::Bin(op, Box::new(acc), Box::new(build(child)?));
    }
    Ok(acc)
}

/// Map a `cmp_op` token to its [`BinOp`] (the mapping lives on `BinOp`).
fn cmp_op(op: &str) -> Result<BinOp, String> {
    BinOp::from_comparison(op).ok_or_else(|| format!("unsupported comparison operator {op:?}"))
}

