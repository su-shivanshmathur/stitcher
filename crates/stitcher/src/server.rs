//! Ops HTTP server: `GET /metrics`, `GET /health`, `GET /state`.
//!
//! Runs on a dedicated OS thread with its own `actix_web::rt::System` (`HttpServer` is
//! `!Send`); `/state` reuses the pipeline's shared [`Store`] connection. `/state` returns
//! raw session data (PII); keep the ops port cluster-internal.

use std::sync::Arc;

use prometheus::Encoder;

use crate::errors::{StitcherError, StitcherResult};
use crate::store::Store;

/// Shared handles injected into every request.
#[derive(Clone)]
struct AppState {
    store: Arc<dyn Store>,
    /// The single id_type this process serves. The store's local cache is keyed to it, so
    /// `/state` must reject any other id_type — a cache hit could return wrong-type state.
    id_type: Arc<str>,
    /// Admin API key required in the `api-key` header to read `/state` (raw PII). `None`
    /// ⇒ every `/state` request is rejected (401) — the endpoint is closed by default.
    admin_api_key: Option<Arc<str>>,
}

/// Spawn the actix-web ops server on a dedicated OS thread, reusing `store` for `/state`.
/// `id_type` rejects cross-type `/state` queries; `admin_api_key` (when set) is required in
/// the `api-key` header. The pre-bound `TcpListener` surfaces bind errors before the spawn.
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
                // The pipeline is the sole shutdown authority (it drains the in-flight
                // batch on SIGTERM). Without this, actix installs its own signal handler
                // and /health + /metrics go dark mid-drain, tripping k8s probes.
                .disable_signals()
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

/// Constant-time byte comparison (XOR-accumulated, no short-circuit; the loop always runs
/// the configured key's length and a length mismatch is folded in up front), so the
/// running time reveals nothing about the request key.
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
/// `None` configured ⇒ closed. Compared as raw bytes, so a non-ASCII key still matches.
fn authorized(req: &actix_web::HttpRequest, state: &AppState) -> bool {
    state.admin_api_key.as_ref().is_some_and(|key| {
        req.headers()
            .get("api-key")
            .is_some_and(|got| secret_eq(got.as_bytes(), key.as_bytes()))
    })
}

/// `GET /state`. Auth runs before the query is inspected, so an unauthenticated malformed
/// request still gets 401 — hence the `Result<Query<_>>` (a bare extractor would 400
/// ahead of the auth check).
async fn state_handler(
    req: actix_web::HttpRequest,
    payload: Result<actix_web::web::Query<StateParams>, actix_web::Error>,
    state: actix_web::web::Data<AppState>,
) -> actix_web::HttpResponse {
    let mut response = if !authorized(&req, &state) {
        // Uniform 401 whether the key is unset or wrong; the startup log flags unset.
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
    // `/state` carries PII behind a custom header (shared caches don't apply their
    // `Authorization` safeguards), so force `no-store` on every response.
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
        // The local cache is keyed to this process's id_type and ignores the argument,
        // so another type could yield a wrong-type cache hit — reject it.
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

/// Read one `(id_type, id)` from the shared store (local cache first, remote on a miss);
/// validation is the caller's job.
async fn fetch_state(params: &StateParams, state: &AppState) -> actix_web::HttpResponse {
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
