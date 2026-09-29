//! Ops HTTP server: `GET /metrics`, `GET /health`, `GET /state`.
//!
//! Runs on a dedicated OS thread with its own `actix_web::rt::System` (`HttpServer` is
//! `!Send`). The pipeline's already-built [`Store`] is shared in via [`AppState`] — the
//! `/state` handler reuses that one connection pool rather than opening a new session
//! (mirrors an `AppState`-of-shared-handles design). The store's scylla session lives on
//! the pipeline's runtime, which stays alive for the process lifetime.

use std::sync::Arc;

use crate::errors::{StitcherError, StitcherResult};
use crate::store::Store;

/// Shared handles injected into every request (currently just the state store).
#[derive(Clone)]
struct AppState {
    store: Arc<dyn Store>,
}

/// Spawn the actix-web ops server on a dedicated OS thread, reusing `store` for `/state`.
/// The pre-bound `TcpListener` surfaces bind errors before the thread is spawned.
pub fn spawn(host: &str, port: u16, store: Arc<dyn Store>) -> StitcherResult<()> {
    let addr = format!("{host}:{port}");
    let listener = std::net::TcpListener::bind(&addr)
        .map_err(|e| error_stack::report!(StitcherError::Telemetry(format!("bind {addr}: {e}"))))?;
    tracing::info!(%addr, "ops endpoint up (/metrics, /health, /state)");

    std::thread::Builder::new()
        .name("ops-http".to_string())
        .spawn(move || {
            let state = actix_web::web::Data::new(AppState { store });
            let rt = actix_web::rt::System::new();
            rt.block_on(async move {
                let server = match actix_web::HttpServer::new(move || {
                    actix_web::App::new()
                        .app_data(state.clone())
                        .route("/metrics", actix_web::web::get().to(scrape))
                        .route("/health", actix_web::web::get().to(health))
                        .route("/state", actix_web::web::get().to(state_handler))
                })
                .workers(1)
                .listen(listener)
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, "ops server listen failed");
                        return;
                    }
                };
                if let Err(e) = server.run().await {
                    tracing::error!(error = %e, "ops server exited");
                }
            });
        })
        .map_err(|e| {
            error_stack::report!(StitcherError::Telemetry(format!("spawn ops-http: {e}")))
        })?;
    Ok(())
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
    use prometheus::Encoder;
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

/// Fetch one `(id_type, id)` state from the shared store. Reads authoritative data
/// (the local cache falls through to the remote store on a miss).
async fn state_handler(
    params: actix_web::web::Query<StateParams>,
    state: actix_web::web::Data<AppState>,
) -> actix_web::HttpResponse {
    // The extractor already 400s on missing params; guard empty strings too.
    if params.id_type.is_empty() || params.id.is_empty() {
        return actix_web::HttpResponse::BadRequest().json(ErrorBody {
            error: "id_type and id must not be empty".to_string(),
        });
    }

    // `Key` is `type Key = String`.
    let key: crate::processor::Key = params.id.clone();
    let result = state
        .store
        .get_many(&params.id_type, std::slice::from_ref(&key))
        .await;

    match result {
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
