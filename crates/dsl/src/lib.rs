//! `stitcher-dsl` — the schema + expression language shared by compile-time codegen
//! (`stitcher_macro::schema!`) and the runtime interpreter (`stitcher::state`, lantern
//! #37): pest grammar, YAML schema model, and the expression AST. Parse-only: no
//! evaluation lives here (the evaluator binds `stitcher::builtins`, which sits above
//! this crate to keep the dependency graph acyclic).

pub mod expr;
pub mod grammar;
pub mod model;
