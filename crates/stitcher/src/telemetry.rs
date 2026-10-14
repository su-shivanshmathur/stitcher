//! `tracing-subscriber` init (JSON default); hold the returned guard or the non-blocking
//! writer's buffered lines are lost. `RUST_LOG` wins over config `level`.

use error_stack::ResultExt;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::{
    config::LogCfg,
    errors::{StitcherError, StitcherResult},
};

/// Drop flushes the background writer — keep it in `main`.
#[must_use]
pub struct TelemetryGuard(pub WorkerGuard);

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
