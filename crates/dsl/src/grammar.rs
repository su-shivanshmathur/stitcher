//! Grammar entry point: pest parser for the v1 expression DSL.

#![allow(missing_docs)] // pest generates identifiers without docs

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
pub struct DslParser;

/// pest is recursive-descent with no depth guard — this turns deep nesting into a catchable
/// parse error. ~77 calls per flat `||` term ⇒ 50k ≈ 650 flat terms; for nesting the AST cap
/// binds first, the limit's role being pest's own recursion ceiling.
const MAX_PARSE_CALLS: usize = 50_000;

/// The `file` entry rule (`SOI ~ expr ~ EOI`) makes a prefix match a hard error; the
/// returned pairs start at `expr`.
pub fn parse_expr(src: &str) -> Result<pest::iterators::Pairs<'_, Rule>, pest::error::Error<Rule>> {
    use pest::Parser;
    // `set_call_limit` writes a process-global atomic; re-asserting per parse self-heals a
    // foreign write. Fail closed on pathological nesting rather than abort the process.
    pest::set_call_limit(std::num::NonZeroUsize::new(MAX_PARSE_CALLS));
    DslParser::parse(Rule::file, src)
}
