//! DSL expression AST (v1 vocabulary, PLAN §25): [`compile`] parses source once into a
//! tree that is either evaluated at runtime (`stitcher::state::interp`) or transpiled
//! (`stitcher_macro`). The walk mirrors `stitcher_macro`'s pest traversal rule-for-rule
//! so both backends accept the same language.

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
}

/// Compiled expression tree over `serde_json::Value` records.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
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

/// pest-pair → AST (structural mirror of `stitcher_macro::codegen::emit`).
fn build(pair: Pair<'_, Rule>) -> Result<Expr, String> {
    match pair.as_rule() {
        Rule::expr => {
            let mut inner = pair.into_inner();
            let first = inner.next().ok_or("empty expr")?;
            let mut acc = build(first)?;
            while let Some(op) = inner.next() {
                let rhs = inner.next().ok_or("dangling operator")?;
                let r = build(rhs)?;
                let op = match op.as_str() {
                    "==" => BinOp::Eq,
                    "!=" => BinOp::Ne,
                    "<" => BinOp::Lt,
                    "<=" => BinOp::Le,
                    ">" => BinOp::Gt,
                    ">=" => BinOp::Ge,
                    "&&" => BinOp::And,
                    "||" => BinOp::Or,
                    o => return Err(format!("unsupported operator {o:?}")),
                };
                acc = Expr::Bin(op, Box::new(acc), Box::new(r));
            }
            Ok(acc)
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
        Rule::path => Ok(Expr::Path(pair.as_str().to_string())),
        Rule::call => {
            let mut inner = pair.into_inner();
            let name = inner
                .next()
                .ok_or_else(|| "call without callee".to_string())?
                .as_str();
            let args: Vec<Expr> = inner.map(build).collect::<Result<_, _>>()?;
            let (builtin, arity) = match name {
                "parse_time" => (Builtin::ParseTime, 1),
                "meaningful" => (Builtin::Meaningful, 1),
                "bucket" => (Builtin::Bucket, 2),
                "round" => (Builtin::Round, 1),
                "trim" => (Builtin::Trim, 1),
                "lower" => (Builtin::Lower, 1),
                "coalesce" => (Builtin::Coalesce, 2),
                other => {
                    return Err(format!(
                        "unknown built-in {other:?}; v1: parse_time/meaningful/bucket/round/trim/lower/coalesce"
                    ))
                }
            };
            if args.len() != arity {
                return Err(format!("{name} expects {arity} arg(s), got {}", args.len()));
            }
            Ok(Expr::Call(builtin, args))
        }
        other => Err(format!("unexpected grammar rule: {other:?}")),
    }
}
