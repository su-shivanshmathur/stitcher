//! Grammar entry point: pest parser for the v1 expression DSL.

#![allow(missing_docs)] // pest generates identifiers without docs

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
pub struct DslParser;

/// Parse an expression source string into its pest pairs.
///
/// Parses the `file` entry rule (`SOI ~ expr ~ EOI`) so a prefix match is a
/// hard error instead of a silently truncated expression; the returned pairs
/// start at the `expr` rule, matching the pre-`EOI` shape callers expect.
pub fn parse_expr(src: &str) -> Result<pest::iterators::Pairs<'_, Rule>, pest::error::Error<Rule>> {
    use pest::Parser;
    DslParser::parse(Rule::file, src)
}
