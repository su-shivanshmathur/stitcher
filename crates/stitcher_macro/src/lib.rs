//! `stitcher_macro` — build-time codegen for schema-driven `stitcher` processors (PLAN §24):
//! `schema!("x.yaml")` parses nodes + DSL (`stitcher_dsl`) → validates → transpiles via
//! `quote!` into a struct + `impl Merge` + `impl Processor`.

mod codegen;

use stitcher_dsl::model;

/// Compile a YAML schema into a `Processor` + `Merge`able state type, inline.
#[proc_macro]
pub fn schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let lit = syn::parse_macro_input!(input as syn::LitStr);
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let path = std::path::Path::new(&manifest).join(lit.value());

    let src = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&lit, format!("read {}: {e}", path.display()))
                .to_compile_error()
                .into();
        }
    };
    let schema_model: model::Schema = match serde_yaml_ng::from_str(&src) {
        Ok(m) => m,
        Err(e) => {
            return syn::Error::new_spanned(&lit, format!("yaml {}: {e}", path.display()))
                .to_compile_error()
                .into();
        }
    };
    match codegen::generate(&schema_model, &path.to_string_lossy()) {
        Ok(ts) => {
            // Debug aid: STITCHER_DEBUG_SCHEMA_OUT=/path/out.rs dumps the expansion.
            if let Ok(dump) = std::env::var("STITCHER_DEBUG_SCHEMA_OUT") {
                let _ =
                    std::fs::write(format!("{dump}.{}", schema_model.aggregate), ts.to_string());
            }
            ts.into()
        }
        Err(e) => e.to_compile_error().into(),
    }
}
