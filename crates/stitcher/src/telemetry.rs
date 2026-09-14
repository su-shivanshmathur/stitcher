//! Logging init: `tracing-subscriber` (PLAN #22). JSON (default) or human from `[log]
//! format`; `RUST_LOG` wins verbatim (else config level + inspect raise); `log` bridges in.

use error_stack::ResultExt;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::{
    config::LogCfg,
    errors::{StitcherError, StitcherResult},
};

/// Install the global console subscriber (once, before any event). `RUST_LOG` wins over
/// both the config level and the inspect-raise.
pub fn init(cfg: &LogCfg, inspect_events: bool) -> StitcherResult<()> {
    let filter = EnvFilter::try_new(filtering_directive(cfg, inspect_events))
        .map_err(|e| error_stack::report!(StitcherError::Telemetry(e.to_string())))?;
    let init = match cfg.format {
        crate::config::LogFormat::Json => tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json().flatten_event(true))
            .try_init(),
        crate::config::LogFormat::HumanReadable => tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().pretty())
            .try_init(),
    };
    init.map_err(|e| error_stack::report!(StitcherError::Telemetry(e.to_string())))
        .attach_printable("global subscriber already set")?;
    Ok(())
}

/// The effective filtering directive: `RUST_LOG` → config level (+ inspect raise).
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
