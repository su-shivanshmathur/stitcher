//! Ops HTTP server: `GET /metrics`, `GET /health`, `GET /state`.
//!
//! Runs on a dedicated OS thread with its own `actix_web::rt::System` (`HttpServer` is
//! `!Send`). The pipeline's already-built [`Store`] is shared in via [`AppState`] — the
//! `/state` handler reuses that one connection pool rather than opening a new session
//! (mirrors an `AppState`-of-shared-handles design). The store's scylla session lives on
//! the pipeline's runtime, which stays alive for the process lifetime.
//!
//! `/state` returns raw session data (PII); keep the ops port cluster-internal.

use std::sync::Arc;

use prometheus::Encoder;

use crate::errors::{StitcherError, StitcherResult};
use crate::store::Store;

/// Shared handles injected into every request.
#[derive(Clone)]
struct AppState {
    store: Arc<dyn Store>,
    /// The single id_type this process serves. The store's local cache is keyed to it
    /// (`RocksStore` is single-id_type per process), so `/state` must reject any other
    /// id_type — otherwise a cache hit could return the wrong type's state.
    id_type: Arc<str>,
    /// Admin API key required in the `api-key` header to read `/state` (raw PII). `None`
    /// ⇒ every `/state` request is rejected (401), so the endpoint is closed by default.
    admin_api_key: Option<Arc<str>>,
}

/// Spawn the actix-web ops server on a dedicated OS thread, reusing `store` for `/state`.
/// `id_type` is the type this process serves (used to reject cross-type `/state` queries);
/// `admin_api_key`, when set, is required in the `api-key` header for `/state`.
/// The pre-bound `TcpListener` surfaces bind errors before the thread is spawned.
pub fn spawn(
    host: &str,
    port: u16,
    store: Arc<dyn Store>,
    id_type: &str,
    admin_api_key: Option<&str>,
) -> StitcherResult<()> {
    let addr = format!("{host}:{port}");
    let listener = std::net::TcpListener::bind(&addr)
        .map_err(|e| error_stack::report!(StitcherError::Telemetry(format!("bind {addr}: {e}"))))?;
    if admin_api_key.is_none() {
        tracing::warn!("server.admin_api_key unset; GET /state will reject every request (401)");
    }
    tracing::info!(%addr, "ops endpoint up (/metrics, /health, /state)");

    let state = actix_web::web::Data::new(AppState {
        store,
        id_type: Arc::from(id_type),
        admin_api_key: admin_api_key.map(Arc::from),
    });
    std::thread::Builder::new()
        .name("ops-http".to_string())
        .spawn(move || {
            let rt = actix_web::rt::System::new();
            rt.block_on(async move {
                let built = actix_web::HttpServer::new(move || {
                    actix_web::App::new()
                        .app_data(state.clone())
                        .route("/metrics", actix_web::web::get().to(scrape))
                        .route("/health", actix_web::web::get().to(health))
                        .route("/state", actix_web::web::get().to(state_handler))
                })
                .workers(1)
                .listen(listener);
                match built {
                    Ok(server) => {
                        if let Err(e) = server.run().await {
                            tracing::error!(error = %e, "ops server exited");
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "ops server listen failed"),
                }
            });
        })
        .map_err(|e| {
            error_stack::report!(StitcherError::Telemetry(format!("spawn ops-http: {e}")))
        })?;
    Ok(())
}

/// Compare the request's key against the configured key without leaking, via timing, how
/// many bytes matched or how long the *request* key was. Differences are XOR-accumulated
/// (no short-circuit), and the loop always runs the configured key's length — a constant
/// per process — so the running time does not depend on the request. A length mismatch is
/// folded in up front, so a shorter or longer request still fails.
fn secret_eq(provided: &[u8], configured: &[u8]) -> bool {
    let mut diff = u8::from(provided.len() != configured.len());
    for (i, c) in configured.iter().enumerate() {
        diff |= provided.get(i).copied().unwrap_or(0) ^ *c;
    }
    diff == 0
}

async fn health() -> &'static str {
    "ok"
}

async fn scrape() -> actix_web::HttpResponse {
    match gather_metrics() {
        Ok(body) => actix_web::HttpResponse::Ok()
            .content_type("text/plain; version=0.0.4")
            .body(body),
        Err(e) => actix_web::HttpResponse::InternalServerError().body(e),
    }
}

fn gather_metrics() -> Result<String, String> {
    let encoder = prometheus::TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&crate::metrics::registry().gather(), &mut buf)
        .map_err(|e| e.to_string())?;
    String::from_utf8(buf).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// GET /state?id_type=<>&id=<>
// ---------------------------------------------------------------------------

/// Query parameters for `GET /state`.
#[derive(serde::Deserialize)]
struct StateParams {
    id_type: String,
    id: String,
}

/// Response body for a found session state.
#[derive(serde::Serialize)]
struct StateFound {
    id_type: String,
    id: String,
    version: i64,
    state: serde_json::Value,
}

/// Response body for error cases.
#[derive(serde::Serialize)]
struct ErrorBody {
    error: String,
}

/// True when the request carries an `api-key` header matching the configured admin key.
/// A missing configured key means "closed" — nothing authorizes. The header is compared
/// as raw bytes, so a non-ASCII key still matches (`to_str` would reject it).
fn authorized(req: &actix_web::HttpRequest, state: &AppState) -> bool {
    state.admin_api_key.as_ref().is_some_and(|key| {
        req.headers()
            .get("api-key")
            .is_some_and(|got| secret_eq(got.as_bytes(), key.as_bytes()))
    })
}

/// `GET /state`. Authentication runs before the query is inspected, so an unauthenticated
/// malformed request still gets 401 — the `Query` extractor is taken as a `Result` so a
/// parse error is handed to the handler rather than rejected before it runs (a bare
/// `web::Query<_>` argument would 400 ahead of the auth check).
async fn state_handler(
    req: actix_web::HttpRequest,
    payload: Result<actix_web::web::Query<StateParams>, actix_web::Error>,
    state: actix_web::web::Data<AppState>,
) -> actix_web::HttpResponse {
    let mut response = if !authorized(&req, &state) {
        // Uniform 401 whether the key is unset or wrong, so the response never reveals
        // which — the startup log flags an unset key for the operator instead.
        actix_web::HttpResponse::Unauthorized().json(ErrorBody {
            error: "unauthorized: a valid api-key header is required".to_string(),
        })
    } else {
        match payload {
            Err(e) => actix_web::HttpResponse::BadRequest().json(ErrorBody {
                error: e.to_string(),
            }),
            Ok(payload) => query_state(payload.into_inner(), &state).await,
        }
    };
    // `/state` carries PII and authenticates with a custom header, so shared caches do not
    // apply the safeguards tied to `Authorization`. Force `no-store` on every response so
    // no intermediary or client cache can retain or replay one past the key check.
    response.headers_mut().insert(
        actix_web::http::header::CACHE_CONTROL,
        actix_web::http::header::HeaderValue::from_static("no-store"),
    );
    response
}

/// Validate a parsed request and read its state from the shared store.
async fn query_state(params: StateParams, state: &AppState) -> actix_web::HttpResponse {
    if params.id_type.is_empty() || params.id.is_empty() {
        actix_web::HttpResponse::BadRequest().json(ErrorBody {
            error: "id_type and id must not be empty".to_string(),
        })
    } else if *params.id_type != *state.id_type {
        // This process serves one id_type; its local cache is keyed to it and ignores the
        // argument, so another type could yield a wrong-type cache hit — reject it.
        actix_web::HttpResponse::BadRequest().json(ErrorBody {
            error: format!(
                "this instance serves id_type={}; query for id_type={} not allowed",
                state.id_type, params.id_type
            ),
        })
    } else {
        fetch_state(&params, state).await
    }
}

/// Read one `(id_type, id)` from the shared store and render it. Local cache first,
/// remote on a miss (`ComposedStore`); validation is the caller's job.
async fn fetch_state(params: &StateParams, state: &AppState) -> actix_web::HttpResponse {
    // `Key` is `type Key = String`.
    let key: crate::processor::Key = params.id.clone();
    match state
        .store
        .get_many(&params.id_type, std::slice::from_ref(&key))
        .await
    {
        Err(e) => {
            tracing::error!(error = ?e, id_type = %params.id_type, id = %params.id, "state query failed");
            actix_web::HttpResponse::InternalServerError().json(ErrorBody {
                error: "store error".to_string(),
            })
        }
        Ok(mut found) => match found.remove(&key) {
            None => actix_web::HttpResponse::NotFound().json(ErrorBody {
                error: format!("no state for id_type={} id={}", params.id_type, params.id),
            }),
            Some((version, blob)) => match serde_json::from_slice::<serde_json::Value>(&blob) {
                Err(e) => {
                    tracing::error!(error = %e, id_type = %params.id_type, id = %params.id, "stored state is not valid JSON");
                    actix_web::HttpResponse::InternalServerError().json(ErrorBody {
                        error: "stored state corrupt".to_string(),
                    })
                }
                Ok(state) => actix_web::HttpResponse::Ok().json(StateFound {
                    id_type: params.id_type.clone(),
                    id: params.id.clone(),
                    version,
                    state,
                }),
            },
        },
    }
}
