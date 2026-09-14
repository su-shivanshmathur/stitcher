//! Grammar entry point: pest parser for the v1 expression DSL.

#![allow(missing_docs)] // pest generates identifiers without docs

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
pub struct DslParser;

/// Parse an expression source string into its pest pairs.
pub fn parse_expr(src: &str) -> Result<pest::iterators::Pairs<'_, Rule>, pest::error::Error<Rule>> {
    use pest::Parser;
    DslParser::parse(Rule::expr, src)
}
