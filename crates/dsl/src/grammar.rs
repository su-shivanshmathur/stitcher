//! Grammar entry point: pest parser for the v1 expression DSL.

#![allow(missing_docs)] // pest generates identifiers without docs

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
pub struct DslParser;

/// Bound on pest's own rule-call count per parse. pest is recursive-descent with no depth guard
/// by default, so a pathologically nested source (`((((…))))`) overflows the stack *inside*
/// [`DslParser::parse`] — before the AST walk in `expr::build` ever runs, so an AST-depth cap
/// alone can't catch it. This limit makes pest return a catchable parse error instead.
///
/// Sizing: a rule-call is a counter tick, not an AST node — this bounds exponential blowup and
/// pest's own recursion depth, NOT config size per se. Empirically ~77 calls per flat `||` term
/// and ~160 per paren level, so 50k ≈ 650 flat terms (a gate enumerating hundreds of codes
/// passes). For nesting, the AST cap in `expr::build` binds FIRST (~62 paren levels), so this
/// limit never rejects a depth the AST would accept; its nesting role is only capping pest's
/// own recursion (~310 frames — the stack-safety bound, not a supported config depth), well
/// within the 8 MiB main-thread stack config-load runs on (config load is not spawned; a
/// spawned loader would land on a 2 MiB worker stack).
const MAX_PARSE_CALLS: usize = 50_000;

/// Parse an expression source string into its pest pairs.
///
/// Parses the `file` entry rule (`SOI ~ expr ~ EOI`) so a prefix match is a
/// hard error instead of a silently truncated expression; the returned pairs
/// start at the `expr` rule, matching the pre-`EOI` shape callers expect.
pub fn parse_expr(src: &str) -> Result<pest::iterators::Pairs<'_, Rule>, pest::error::Error<Rule>> {
    use pest::Parser;
    // `set_call_limit` writes a process-global atomic (NOT thread-local), read when each parse
    // state is built. Writing it unconditionally here is safe because the value is always the same
    // constant — concurrent parses all install and observe MAX_PARSE_CALLS. The global is shared
    // with any other in-process pest user; this crate is the binary's only one, and re-asserting
    // the limit on every parse self-heals a foreign write. Fail closed on pathological nesting
    // rather than abort the process (see MAX_PARSE_CALLS).
    pest::set_call_limit(std::num::NonZeroUsize::new(MAX_PARSE_CALLS));
    DslParser::parse(Rule::file, src)
}
