//! Logging init: `tracing-subscriber`, JSON (default) or human from `[log] format`. Writes
//! through a non-blocking appender; the returned [`TelemetryGuard`] must be held for the
//! process lifetime so buffered lines flush on shutdown. `RUST_LOG` wins over config `level`.

use error_stack::ResultExt;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::{
    config::LogCfg,
    errors::{StitcherError, StitcherResult},
};

/// Holds the appender's `WorkerGuard`; drop flushes the background writer. Keep it in `main`.
#[must_use]
pub struct TelemetryGuard(pub WorkerGuard);

/// Install the global subscriber (once). Hold the returned guard until shutdown.
pub fn init(cfg: &LogCfg, inspect_events: bool) -> StitcherResult<TelemetryGuard> {
    let filter = EnvFilter::try_new(filtering_directive(cfg, inspect_events))
        .map_err(|e| error_stack::report!(StitcherError::Telemetry(e.to_string())))?;
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    let init = match cfg.format {
        crate::config::LogFormat::Json => tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json().flatten_event(true).with_writer(writer))
            .try_init(),
        crate::config::LogFormat::HumanReadable => tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_writer(writer))
            .try_init(),
    };
    init.map_err(|e| error_stack::report!(StitcherError::Telemetry(e.to_string())))
        .attach_printable("global subscriber already set")?;
    Ok(TelemetryGuard(guard))
}

/// `RUST_LOG` (verbatim) → config `level` (+ the `stitcher::inspect` raise when inspecting).
fn filtering_directive(cfg: &LogCfg, inspect_events: bool) -> String {
    if let Ok(env) = std::env::var("RUST_LOG") {
        if !env.is_empty() {
            return env;
        }
    }
    if inspect_events {
        format!("{},{}=debug", cfg.level, crate::inspect::TARGET)
    } else {
        cfg.level.clone()
    }
}
