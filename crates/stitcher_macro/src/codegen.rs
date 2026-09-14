//! Schema → Rust transpilation (PLAN §Compile-time pipeline).

use pest::iterators::Pair;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};

use stitcher_dsl::{
    grammar::{parse_expr, Rule},
    model::{Field, Schema, Sink},
};

/// Codegen for a whole schema; `abs_path` feeds `include_str!` (recompile-on-change,
/// PLAN §26).
pub fn generate(schema: &Schema, abs_path: &str) -> Result<TokenStream2, syn::Error> {
    let aggregate = upper_camel(&schema.aggregate);
    let state_ident = format_ident!("{}", aggregate);
    let processor_ident = format_ident!("{}Processor", aggregate);
    let id_type = &schema.id_type;
    let version = schema.version;

    validate(schema)?;

    // nested structs first (they are referenced by the aggregate)
    let mut nested_defs: Vec<TokenStream2> = Vec::new();
    for (name, field) in &schema.fields {
        if let Field::Nested { fields } = field {
            let n_ident = format_ident!("{}", upper_camel(name));
            let (defs, s) = gen_struct(&n_ident, fields);
            nested_defs.extend(defs);
            nested_defs.push(s);
        }
    }

    let (defs, aggregate_struct) = gen_struct(&state_ident, &schema.fields);
    nested_defs.extend(defs);

    let merge_impl = gen_merge(&state_ident, &schema.fields);
    let key_body = gen_key_body(&schema.primary_key);
    let filter_body = gen_filter(schema);
    let state_body = gen_state_body(&state_ident, &schema.fields)?;
    let encode_body = gen_encode_body(schema, &schema.fields)?;

    let expanded = quote! {
        #(#nested_defs)*
        #aggregate_struct
        #merge_impl

        /// Generated codec + `Processor` for the `#aggregate` schema (see schema file).
        pub struct #processor_ident {
            tenant_ids: std::collections::HashSet<String>,
        }

        impl #processor_ident {
            /// Construct with the tenant allow-list (empty = accept all).
            #[must_use]
            pub fn new(tenant_ids: &[String]) -> Self {
                Self {
                    tenant_ids: tenant_ids.iter().cloned().collect(),
                }
            }
        }

        impl ::stitcher::processor::Processor for #processor_ident {
            type State = #state_ident;
            fn state_version(&self) -> i64 {
                #version
            }
            fn id_type(&self) -> &str {
                #id_type
            }
            fn decode_stored(&self, blob: &[u8]) -> Option<Self::State> {
                serde_json::from_slice(blob).ok()
            }

            fn primary_key(&self, raw: &[u8]) -> Option<::stitcher::processor::Key> {
                let __record: serde_json::Value = serde_json::from_slice(raw).ok()?;
                #key_body
                Some(__key)
            }

            fn decode(&self, raw: &[u8]) -> Option<Self::State> {
                self.decode_with_key(raw).map(|(_, s)| s)
            }

            fn encode(
                &self,
                state: &Self::State,
                sign: ::stitcher::processor::Sign,
            ) -> Vec<::stitcher::processor::OutMsg> {
                #encode_body
            }

            fn decode_with_key(
                &self,
                raw: &[u8],
            ) -> Option<(::stitcher::processor::Key, Self::State)> {
                let __record: serde_json::Value = serde_json::from_slice(raw).ok()?;
                #filter_body
                #key_body
                #state_body
                Some((__key, __state))
            }
        }

        // PLAN §26: schema edits must trigger a rebuild of the generated code.
        const _: &str = include_str!(#abs_path);
    };
    Ok(expanded)
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

fn validate(schema: &Schema) -> Result<(), syn::Error> {
    let err = |m: String| {
        syn::Error::new(
            Span::call_site(),
            format!("[schema {}] {m}", schema.aggregate),
        )
    };
    if schema.aggregate.is_empty() {
        return Err(err("aggregate name must be non-empty".into()));
    }
    for field in schema.fields.values() {
        validate_field(field)?;
    }
    for sink in &schema.sinks {
        let Some(field) = schema.fields.get(&sink.field) else {
            return Err(err(format!(
                "sink {:?} references unknown field {:?}",
                sink.name, sink.field
            )));
        };
        // fan_out iterates map entries; key_path reads `{field}.payload.{p}` — only a
        // non-fan-out `latest_by` sink has that shape (mirrored in `stitcher::state`).
        if sink.fan_out && !matches!(field, Field::KeyedMap { .. }) {
            return Err(err(format!(
                "sink {:?}: fan_out requires a keyed_map field",
                sink.name
            )));
        }
        if sink.key_path.is_some() && (sink.fan_out || !matches!(field, Field::LatestBy { .. })) {
            return Err(err(format!(
                "sink {:?}: key_path requires a non-fan_out latest_by field",
                sink.name
            )));
        }
        // retention fields are consumed by the pipeline's per-topic config; when stated
        // here we validate coherence so schema and config can't contradict silently.
        match (sink.retention_days, &sink.retention_key) {
            (Some(days), Some(key)) => {
                if days <= 0 || key.is_empty() {
                    return Err(err(format!(
                        "sink {:?}: retention_days must be > 0 and retention_key non-empty",
                        sink.name
                    )));
                }
            }
            (None, None) => {}
            _ => {
                return Err(err(format!(
                    "sink {:?}: retention_days and retention_key must be set together",
                    sink.name
                )))
            }
        }
    }
    Ok(())
}

fn validate_field(field: &Field) -> Result<(), syn::Error> {
    match field {
        Field::LatestBy {
            when,
            comparator,
            payload,
        } => {
            if let Some(w) = when {
                check_expr(w)?;
            }
            check_expr(comparator)?;
            check_expr(payload)?;
        }
        Field::KeyedMap { when, key, value } => {
            if let Some(w) = when {
                check_expr(w)?;
            }
            check_expr(key)?;
            validate_field(value)?;
        }
        Field::Last { when, value } => {
            if let Some(w) = when {
                check_expr(w)?;
            }
            check_expr(value)?;
        }
        Field::Counter { when } => {
            if let Some(w) = when {
                check_expr(w)?;
            }
        }
        Field::Nested { fields } => {
            for f in fields.values() {
                validate_field(f)?;
            }
        }
    }
    Ok(())
}

fn check_expr(src: &str) -> Result<(), syn::Error> {
    parse_expr(src)
        .map(|_| ())
        .map_err(|e| syn::Error::new(Span::call_site(), format!("invalid DSL {src:?}: {e}")))
}

// ---------------------------------------------------------------------------
// struct + merge
// ---------------------------------------------------------------------------

/// Emit `(nested_def_list, struct_def)` for `fields`.
fn gen_struct(
    ident: &proc_macro2::Ident,
    fields: &std::collections::BTreeMap<String, Field>,
) -> (Vec<TokenStream2>, TokenStream2) {
    let mut nested = Vec::new();
    let mut members = Vec::new();
    for (name, field) in fields {
        let f_ident = format_ident!("{name}");
        let ty = field_type(name, field, &mut nested);
        let skip = skip_attr(field);
        members.push(quote! {
            #[serde(default #skip)]
            pub #f_ident: #ty
        });
    }
    let def = quote! {
        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub struct #ident {
            #(#members),*
        }
    };
    (nested, def)
}

fn skip_attr(field: &Field) -> TokenStream2 {
    match field {
        Field::LatestBy { .. } => {
            quote!(, skip_serializing_if = "stitcher::merge::LatestBy::is_default")
        }
        Field::KeyedMap { .. } => {
            quote!(, skip_serializing_if = "stitcher::merge::KeyedMap::is_empty")
        }
        Field::Last { .. } => quote!(, skip_serializing_if = "stitcher::merge::LastWrite::is_none"),
        Field::Counter { .. } => {
            quote!(, skip_serializing_if = "stitcher::merge::Counter::is_zero")
        }
        Field::Nested { .. } => TokenStream2::new(), // nested always serialized
    }
}

fn field_type(name: &str, field: &Field, nested: &mut Vec<TokenStream2>) -> TokenStream2 {
    match field {
        Field::LatestBy { .. } => quote!(stitcher::merge::LatestBy<i64, serde_json::Value>),
        Field::KeyedMap { value, .. } => {
            let inner = field_type(name, value, nested);
            quote!(stitcher::merge::KeyedMap<String, #inner>)
        }
        Field::Last { .. } => quote!(stitcher::merge::LastWrite<serde_json::Value>),
        Field::Counter { .. } => quote!(stitcher::merge::Counter),
        Field::Nested { fields } => {
            let ident = format_ident!("{}", upper_camel(name));
            let (defs, s) = gen_struct(&ident, fields);
            nested.extend(defs);
            nested.push(s);
            quote!(#ident)
        }
    }
}

fn gen_merge(
    ident: &proc_macro2::Ident,
    fields: &std::collections::BTreeMap<String, Field>,
) -> TokenStream2 {
    let names: Vec<_> = fields.keys().map(|n| format_ident!("{n}")).collect();
    quote! {
        impl stitcher::merge::Merge for #ident {
            fn merge(self, newer: Self) -> Self {
                Self {
                    #(#names: self.#names.merge(newer.#names)),*
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// key template
// ---------------------------------------------------------------------------

/// `"{log.a}-{log.b}-c"` → segments of paths/literals joined by "-".
fn gen_key_body(template: &str) -> TokenStream2 {
    let mut parts: Vec<TokenStream2> = Vec::new();
    let mut rest = template;
    loop {
        if let Some(open) = rest.find('{') {
            let (lit, after) = rest.split_at(open);
            if !lit.is_empty() {
                parts.push(quote!(#lit.to_string()));
            }
            if let Some(close) = after.find('}') {
                let path = after.get(1..close).unwrap_or_default().trim().to_string();
                parts.push(quote!(
                    ::stitcher::json_util::get_str(&__record, #path)?.to_string()
                ));
                rest = after.get(close + 1..).unwrap_or_default();
            } else {
                // unbalanced '{' — treat it as a literal and continue after it
                parts.push(quote!("{"));
                rest = after.get(1..).unwrap_or_default();
            }
        } else {
            if !rest.is_empty() {
                parts.push(quote!(#rest.to_string()));
            }
            break;
        }
    }
    quote! {
        let __key: ::stitcher::processor::Key = [#(#parts),*].join("");
    }
}

// ---------------------------------------------------------------------------
// decode filter
// ---------------------------------------------------------------------------

fn gen_filter(schema: &Schema) -> TokenStream2 {
    let filter = &schema.decode_filter;
    let requires: Vec<TokenStream2> = filter
        .require
        .iter()
        .map(|p| {
            let reject = filter.reject_if_contains.clone();
            let reject_check = reject
                .map(|sub| {
                    quote! {
                        if __v.contains(#sub) { return None; }
                    }
                })
                .unwrap_or_default();
            quote!({
                let __v = ::stitcher::json_util::get_str(&__record, #p)?;
                if __v.is_empty() { return None; }
                #reject_check
            })
        })
        .collect();

    let log_types: Vec<String> = filter.log_type_in.clone();
    let log_type_check = if log_types.is_empty() {
        TokenStream2::new()
    } else {
        quote! {
            {
                let allowed: &[&str] = &[#(#log_types),*];
                match ::stitcher::json_util::get_str(&__record, "log.log_type") {
                    Some(lt) if allowed.contains(&lt) => {}
                    _ => return None,
                }
            }
        }
    };

    let tenant_path = filter
        .tenant_path
        .clone()
        .unwrap_or_else(|| "log.tenant_id".to_string());
    let tenant_check = quote! {
        if !self.tenant_ids.is_empty() {
            match ::stitcher::json_util::get_str(&__record, #tenant_path) {
                Some(t) if self.tenant_ids.contains(t) => {}
                _ => return None,
            }
        }
    };

    quote! {
        #(#requires)*
        #log_type_check
        #tenant_check
    }
}

// ---------------------------------------------------------------------------
// state construction
// ---------------------------------------------------------------------------

fn gen_state_body(
    aggregate: &proc_macro2::Ident,
    fields: &std::collections::BTreeMap<String, Field>,
) -> Result<TokenStream2, syn::Error> {
    let mut lets = Vec::new();
    let mut names = Vec::new();
    for (name, field) in fields {
        let f_ident = format_ident!("{name}");
        let ty = field_type_no_nested(name, field);
        let build = field_build(field)?;
        lets.push(quote! {
            let #f_ident: #ty = { #build };
        });
        names.push(f_ident);
    }
    Ok(quote! {
        #(#lets)*
        let __state = #aggregate { #(#names),* };
    })
}

/// Type for a top-level field; nested structs are disallowed at codegen level 1 (use a
/// `keyed_map`/`latest_by` — keeps the DSL bounded, PLAN §v1 vocabulary).
fn field_type_no_nested(name: &str, field: &Field) -> TokenStream2 {
    let mut sink: Vec<TokenStream2> = Vec::new();
    field_type(name, field, &mut sink)
}

fn field_build(field: &Field) -> Result<TokenStream2, syn::Error> {
    match field {
        Field::LatestBy {
            when,
            comparator,
            payload,
        } => {
            let cmp = gen_value(comparator)?;
            let pay = gen_value(payload)?;
            wrap_when(
                when.as_ref(),
                quote! {
                    let __cmp = #cmp;
                    let comparator = ::stitcher::builtins::parse_time(&__cmp)
                        .or_else(|| ::stitcher::json_util::as_i64(&__cmp))
                        .unwrap_or(0);
                    ::stitcher::merge::LatestBy { comparator, payload: #pay }
                },
            )
        }
        Field::KeyedMap { when, key, value } => {
            let key_e = gen_value(key)?;
            let val_build = field_build_inner(value)?;
            let ty_v = field_type_no_nested("value", value);
            wrap_when(
                when.as_ref(),
                quote! {
                    let __ky = #key_e;
                    match ::stitcher::builtins::key_string(&__ky) {
                        Some(__k) => {
                            let mut m = std::collections::HashMap::new();
                            let v: #ty_v = { #val_build };
                            m.insert(__k, v);
                            ::stitcher::merge::KeyedMap(m)
                        }
                        None => ::std::default::Default::default(),
                    }
                },
            )
        }
        Field::Last { when, value } => {
            let v = gen_value(value)?;
            wrap_when(
                when.as_ref(),
                quote! {
                    let __lv = #v;
                    ::stitcher::merge::LastWrite(::stitcher::builtins::meaningful(&__lv).cloned())
                },
            )
        }
        Field::Counter { when } => wrap_when(when.as_ref(), quote!(::stitcher::merge::Counter(1))),
        Field::Nested { .. } => Err(syn::Error::new(
            Span::call_site(),
            "nested state fields are not part of DSL v1; use keyed_map / latest_by",
        )),
    }
}

/// Inner-node construction (no surrounding `when` — the outer `keyed_map` gates).
fn field_build_inner(field: &Field) -> Result<TokenStream2, syn::Error> {
    match field {
        Field::LatestBy {
            when,
            comparator,
            payload,
        } => {
            if when.is_some() {
                return Err(syn::Error::new(
                    Span::call_site(),
                    "inner latest_by must not re-declare `when` (gated by the keyed_map)",
                ));
            }
            let cmp = gen_value(comparator)?;
            let pay = gen_value(payload)?;
            Ok(quote! {
                {
                    let __cmp = #cmp;
                    let comparator = ::stitcher::builtins::parse_time(&__cmp)
                        .or_else(|| ::stitcher::json_util::as_i64(&__cmp))
                        .unwrap_or(0);
                    ::stitcher::merge::LatestBy { comparator, payload: #pay }
                }
            })
        }
        other => Err(syn::Error::new(
            Span::call_site(),
            format!("unsupported keyed_map value node: {other:?}"),
        )),
    }
}

/// `when = None` → always-true; has to return the value in both branches so types unify.
/// A malformed `when` expression surfaces as a spanned proc-macro error (not a swallowed
/// `compile_error!` in the generated code).
fn wrap_when(when: Option<&String>, build: TokenStream2) -> Result<TokenStream2, syn::Error> {
    match when {
        Some(src) => {
            let pred = gen_value(src).map_err(|e| {
                syn::Error::new(Span::call_site(), format!("invalid `when` {src:?}: {e}"))
            })?;
            Ok(quote! {
                let __p = #pred;
                if ::stitcher::builtins::truthy(&__p) {
                    #build
                } else {
                    ::std::default::Default::default()
                }
            })
        }
        None => Ok(build),
    }
}

// ---------------------------------------------------------------------------
// encode (canonical stitcher shape — see module docs)
// ---------------------------------------------------------------------------

fn gen_encode_body(
    schema: &Schema,
    fields: &std::collections::BTreeMap<String, Field>,
) -> Result<TokenStream2, syn::Error> {
    let mut arms = Vec::new();
    for sink in &schema.sinks {
        let field = fields
            .get(&sink.field)
            .ok_or_else(|| syn::Error::new(Span::call_site(), "sink.field must exist"))?;
        arms.push(gen_sink(sink, field));
    }
    Ok(quote! {
        let __sign: i64 = sign.value();
        let mut __out: Vec<::stitcher::processor::OutMsg> = Vec::new();
        #(#arms)*
        __out
    })
}

fn gen_sink(sink: &Sink, field: &Field) -> TokenStream2 {
    let topic = &sink.topic;
    let field_name = &sink.field;
    let f_ident = format_ident!("{field_name}");
    let gate = gate_expr(&f_ident, field);
    let key_single = sink_key_expr(sink);

    if sink.fan_out {
        quote! {
            if #gate {
                for (__k, __v) in &state.#f_ident.0 {
                    let __payload = serde_json::json!({
                        "sign_flag": __sign,
                        "key": __k,
                        #field_name: __v,
                    });
                    __out.push(::stitcher::processor::OutMsg {
                        topic: #topic.to_string(),
                        key: __k.clone(),
                        payload: serde_json::to_vec(&__payload).unwrap_or_else(|_| b"{}".to_vec()),
                    });
                }
            }
        }
    } else {
        quote! {
            if #gate {
                let __payload = serde_json::json!({
                    "sign_flag": __sign,
                    #field_name: state.#f_ident,
                });
                __out.push(::stitcher::processor::OutMsg {
                    topic: #topic.to_string(),
                    key: #key_single,
                    payload: serde_json::to_vec(&__payload).unwrap_or_else(|_| b"{}".to_vec()),
                });
            }
        }
    }
}

/// Skip a sink's emission when its field carries no state (mirrors `isStateProper`).
fn gate_expr(f_ident: &proc_macro2::Ident, field: &Field) -> TokenStream2 {
    match field {
        Field::LatestBy { .. } => quote!(!state.#f_ident.is_default()),
        Field::KeyedMap { .. } => quote!(!state.#f_ident.is_empty()),
        Field::Last { .. } => quote!(!state.#f_ident.is_none()),
        Field::Counter { .. } => quote!(!state.#f_ident.is_zero()),
        Field::Nested { .. } => quote!(true),
    }
}

fn sink_key_expr(sink: &Sink) -> TokenStream2 {
    let field_name = &sink.field;
    if let Some(p) = &sink.key_path {
        let full = format!("{field_name}.payload.{p}");
        quote!(
            ::stitcher::json_util::get_str(&__payload, #full)
                .unwrap_or(#field_name)
                .to_string()
        )
    } else {
        quote!(#field_name.to_string())
    }
}

// ---------------------------------------------------------------------------
// DSL transpiler
// ---------------------------------------------------------------------------

fn gen_value(src: &str) -> Result<TokenStream2, syn::Error> {
    let mut pairs = parse_expr(src)
        .map_err(|e| syn::Error::new(Span::call_site(), format!("DSL parse {src:?}: {e}")))?;
    let expr = pairs
        .next()
        .ok_or_else(|| syn::Error::new(Span::call_site(), "empty expression"))?;
    emit(expr)
}

fn emit(pair: Pair<'_, Rule>) -> Result<TokenStream2, syn::Error> {
    match pair.as_rule() {
        Rule::expr => {
            let mut inner = pair.into_inner();
            let first = inner
                .next()
                .ok_or_else(|| syn::Error::new(Span::call_site(), "empty expr"))?;
            let mut acc = emit(first)?;
            while let Some(op) = inner.next() {
                let rhs = inner
                    .next()
                    .ok_or_else(|| syn::Error::new(Span::call_site(), "dangling operator"))?;
                let r = emit(rhs)?;
                acc = match op.as_str() {
                    "==" => quote!(serde_json::Value::from(
                        ::stitcher::builtins::json_eq(&(#acc), &(#r))
                    )),
                    "!=" => quote!(serde_json::Value::from(
                        ::stitcher::builtins::json_ne(&(#acc), &(#r))
                    )),
                    "<" => cmp(&acc, &r, &quote!(<)),
                    "<=" => cmp(&acc, &r, &quote!(<=)),
                    ">" => cmp(&acc, &r, &quote!(>)),
                    ">=" => cmp(&acc, &r, &quote!(>=)),
                    "&&" => quote!(serde_json::Value::from(
                        ::stitcher::builtins::truthy(&(#acc)) && ::stitcher::builtins::truthy(&(#r))
                    )),
                    "||" => quote!(serde_json::Value::from(
                        ::stitcher::builtins::truthy(&(#acc)) || ::stitcher::builtins::truthy(&(#r))
                    )),
                    o => {
                        return Err(syn::Error::new(
                            Span::call_site(),
                            format!("unsupported operator {o:?}"),
                        ))
                    }
                };
            }
            Ok(acc)
        }
        Rule::term | Rule::literal => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| syn::Error::new(Span::call_site(), "empty term"))?;
            emit(inner)
        }
        Rule::unary => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| syn::Error::new(Span::call_site(), "empty unary"))?;
            let v = emit(inner)?;
            Ok(quote!(serde_json::Value::from(!::stitcher::builtins::truthy(&(#v)))))
        }
        Rule::string => {
            let raw = pair.as_str();
            let content = raw
                .strip_prefix(|c| c == '\'' || c == '"')
                .and_then(|s| s.strip_suffix(|c| c == '\'' || c == '"'))
                .unwrap_or(raw);
            Ok(quote!(serde_json::Value::from(#content)))
        }
        Rule::number => {
            let text = pair.as_str();
            if text.contains('.') {
                let lit: f64 = text.parse().map_err(|_| {
                    syn::Error::new(Span::call_site(), format!("bad number {text:?}"))
                })?;
                Ok(quote!(serde_json::Value::from(#lit)))
            } else {
                let lit: i64 = text.parse().map_err(|_| {
                    syn::Error::new(Span::call_site(), format!("bad number {text:?}"))
                })?;
                Ok(quote!(serde_json::Value::from(#lit)))
            }
        }
        Rule::bool => {
            let b = pair.as_str() == "true";
            Ok(quote!(serde_json::Value::from(#b)))
        }
        Rule::null => Ok(quote!(serde_json::Value::Null)),
        Rule::path => {
            let dotted = pair.as_str().to_string();
            Ok(quote!(
                ::stitcher::json_util::get_path(&__record, #dotted)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            ))
        }
        Rule::call => {
            let mut inner = pair.into_inner();
            let name = inner
                .next()
                .ok_or_else(|| syn::Error::new(Span::call_site(), "call without callee"))?
                .as_str()
                .to_string();
            let args: Vec<TokenStream2> = inner.map(emit).collect::<Result<Vec<_>, _>>()?;
            gen_call(&name, &args)
        }
        other => Err(syn::Error::new(
            Span::call_site(),
            format!("unexpected grammar rule: {other:?}"),
        )),
    }
}

fn cmp(a: &TokenStream2, b: &TokenStream2, ord: &TokenStream2) -> TokenStream2 {
    quote!(serde_json::Value::from(match (
        ::stitcher::builtins::value_as_f64(&(#a)),
        ::stitcher::builtins::value_as_f64(&(#b))
    ) {
        (Some(x), Some(y)) => x #ord y,
        _ => false,
    }))
}

/// Built-in call set (v1 vocabulary, PLAN §25). `args` is re-checked against the target
/// arity (the pest grammar admits any count), so every extraction below is total.
fn gen_call(name: &str, args: &[TokenStream2]) -> Result<TokenStream2, syn::Error> {
    let bad = |msg: String| syn::Error::new(Span::call_site(), msg);
    match name {
        "parse_time" => {
            let [a] = args else {
                return Err(bad(format!("parse_time expects 1 arg, got {}", args.len())));
            };
            Ok(quote!(match ::stitcher::builtins::parse_time(&(#a)) {
                Some(v) => serde_json::Value::from(v),
                None => serde_json::Value::Null,
            }))
        }
        "meaningful" => {
            let [a] = args else {
                return Err(bad(format!("meaningful expects 1 arg, got {}", args.len())));
            };
            Ok(
                quote!(if ::stitcher::builtins::meaningful(&(#a)).is_some() {
                    (#a).clone()
                } else {
                    serde_json::Value::Null
                }),
            )
        }
        "bucket" => {
            let [a, b] = args else {
                return Err(bad(format!("bucket expects 2 args, got {}", args.len())));
            };
            Ok(quote!(serde_json::Value::from(::stitcher::builtins::bucket(
                ::stitcher::json_util::as_i64(&(#a)).unwrap_or(0),
                ::stitcher::json_util::as_i64(&(#b)).unwrap_or(0)
            ))))
        }
        "round" => {
            let [a] = args else {
                return Err(bad(format!("round expects 1 arg, got {}", args.len())));
            };
            Ok(quote!(serde_json::Value::from(
                ::stitcher::builtins::round_half_even(&(#a)).unwrap_or(0)
            )))
        }
        "trim" => {
            let [a] = args else {
                return Err(bad(format!("trim expects 1 arg, got {}", args.len())));
            };
            Ok(quote!(serde_json::Value::from(
                ::stitcher::builtins::trim(&(#a)).unwrap_or_default()
            )))
        }
        "lower" => {
            let [a] = args else {
                return Err(bad(format!("lower expects 1 arg, got {}", args.len())));
            };
            Ok(quote!(serde_json::Value::from(
                ::stitcher::builtins::lower(&(#a)).unwrap_or_default()
            )))
        }
        "coalesce" => {
            let [a, b] = args else {
                return Err(bad(format!("coalesce expects 2 args, got {}", args.len())));
            };
            Ok(quote!(match ::stitcher::builtins::coalesce(&(#a), &(#b)) {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }))
        }
        other => Err(bad(format!(
            "unknown built-in {other:?}; v1: parse_time/meaningful/bucket/round/trim/lower/coalesce"
        ))),
    }
}

// ---------------------------------------------------------------------------
// ident util
// ---------------------------------------------------------------------------

fn upper_camel(s: &str) -> String {
    s.split('_')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().chain(chars).collect(),
                None => String::new(),
            }
        })
        .collect()
}
