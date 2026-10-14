//! Grammar entry point: pest parser for the v1 expression DSL.

#![allow(missing_docs)] // pest generates identifiers without docs

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
pub struct DslParser;

/// Bound on pest's rule-call count per parse: pest is recursive-descent with no depth guard,
/// so deeply nested source would overflow the stack inside [`DslParser::parse`] before the
/// AST cap in `expr::build` runs — this limit turns that into a catchable parse error.
/// ~77 calls per flat `||` term / ~160 per paren level ⇒ 50k ≈ 650 flat terms; for nesting
/// the AST cap binds first (~62 parens), the limit's role being pest's own recursion ceiling
/// (~310 frames, well within the 8 MiB main-thread stack config-load runs on).
const MAX_PARSE_CALLS: usize = 50_000;

/// Parse via the `file` entry rule (`SOI ~ expr ~ EOI`) so a prefix match is a hard error
/// instead of a silently truncated expression; the returned pairs start at `expr`.
pub fn parse_expr(src: &str) -> Result<pest::iterators::Pairs<'_, Rule>, pest::error::Error<Rule>> {
    use pest::Parser;
    // `set_call_limit` writes a process-global atomic read when each parse state is built;
    // always storing the same constant makes concurrent parses race-free, and re-asserting
    // per parse self-heals a foreign write (this crate is the only pest consumer). Fail
    // closed on pathological nesting rather than abort the process.
    pest::set_call_limit(std::num::NonZeroUsize::new(MAX_PARSE_CALLS));
    DslParser::parse(Rule::file, src)
}
