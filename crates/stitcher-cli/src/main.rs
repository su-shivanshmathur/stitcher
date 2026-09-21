//! `stitcher` CLI: wires the config-driven state processor to an output projection —
//! the transformer (feature `transform`, when configured) else the state logger.

#[tokio::main]
async fn main() -> stitcher::StitcherResult<()> {
    let cfg = stitcher::config::load()?;
    // hold the guard for process life so the non-blocking log writer flushes on shutdown
    let _telemetry = stitcher::telemetry::init(&cfg.log, cfg.debug.any_print())?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "stitcher starting");
    tracing::debug!(startup_config = ?cfg, "effective config"); // secrets redacted by Debug

    let state_cfg = cfg.state.as_ref().ok_or_else(|| {
        error_stack::report!(stitcher::StitcherError::Config(
            "the stitcher binary requires [state] config_file (a state.yaml)".into()
        ))
    })?;
    let program = stitcher::state::config::load(&state_cfg.config_file)?;
    tracing::info!(schema = %program.aggregate, version = program.version, "loaded state config");

    let projection = build_projection(&cfg, &program)?;
    stitcher::run(
        stitcher::state::ConfigProcessor::new(program, &cfg.tenant_ids),
        projection,
        cfg,
    )
    .await
}

/// The transformer when built with `--features transform` and configured; else the logger.
#[cfg(feature = "transform")]
fn build_projection(
    cfg: &stitcher::config::Settings,
    program: &stitcher::state::config::Program,
) -> stitcher::StitcherResult<Box<dyn stitcher::Projection>> {
    match &cfg.transform.config_file {
        Some(path) => Ok(Box::new(transformer::load(path, program)?)),
        None => Ok(Box::new(stitcher::StateLogger)),
    }
}

#[cfg(not(feature = "transform"))]
fn build_projection(
    _cfg: &stitcher::config::Settings,
    _program: &stitcher::state::config::Program,
) -> stitcher::StitcherResult<Box<dyn stitcher::Projection>> {
    Ok(Box::new(stitcher::StateLogger))
}
