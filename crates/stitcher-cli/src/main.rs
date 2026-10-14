//! `check-transformer` (CI validation) or the pipeline wired to a transformer/logger projection.

use std::path::Path;

use stitcher::config::Command;

#[tokio::main]
async fn main() -> stitcher::StitcherResult<()> {
    let cli = stitcher::config::parse_cli();
    match cli.command {
        Some(Command::CheckTransformer {
            state,
            transformer,
            enrichment,
        }) => check_transformer(&state, &transformer, enrichment.as_deref()),
        None => run_pipeline(cli.config_path.as_deref(), cli.inspect).await,
    }
}

async fn run_pipeline(config_path: Option<&Path>, inspect: bool) -> stitcher::StitcherResult<()> {
    let cfg = stitcher::config::load_with(config_path, inspect)?;
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

    // One enrichment read at boot, shared by validation and runtime.
    let enrichment = stitcher::enrichment::Enrichment::spawn_reloader(&cfg.enrichment)?;
    let projection = build_projection(&cfg, &program, &enrichment)?;
    stitcher::run(
        stitcher::state::ConfigProcessor::new(program, &cfg.tenant_ids),
        projection,
        cfg,
        enrichment,
    )
    .await
}

/// The `state ⊇ transformer` check without Kafka/store; non-zero exit on failure.
#[cfg(feature = "transform")]
fn check_transformer(
    state: &Path,
    transformer: &Path,
    enrichment: Option<&Path>,
) -> stitcher::StitcherResult<()> {
    let log = stitcher::config::LogCfg {
        format: stitcher::config::LogFormat::HumanReadable,
        ..stitcher::config::LogCfg::default()
    };
    let _telemetry = stitcher::telemetry::init(&log, false)?;
    let program = stitcher::state::config::load(state)?;
    tracing::info!(schema = %program.aggregate, version = program.version, "loaded state schema");
    let tables = match enrichment {
        Some(path) => {
            stitcher::enrichment::Enrichment::table_names(&stitcher::config::Enrichment {
                config_file: path.to_string_lossy().into_owned(),
                ..Default::default()
            })?
        }
        None => None,
    };
    transformer::load(transformer, &program, tables.as_deref())?;
    tracing::info!("transformer config is valid");
    Ok(())
}

#[cfg(not(feature = "transform"))]
fn check_transformer(
    _state: &Path,
    _transformer: &Path,
    _enrichment: Option<&Path>,
) -> stitcher::StitcherResult<()> {
    Err(error_stack::report!(stitcher::StitcherError::Config(
        "check-transformer requires building with --features transform".into()
    )))
}

#[cfg(feature = "transform")]
fn build_projection(
    cfg: &stitcher::config::Settings,
    program: &stitcher::state::config::Program,
    enrichment: &stitcher::enrichment::Enrichment,
) -> stitcher::StitcherResult<Box<dyn stitcher::Projection>> {
    // validation and runtime share the boot enrichment snapshot — validated lookups
    // can't silently go null
    let configured: Box<dyn stitcher::Projection> = match &cfg.transform.config_file {
        Some(path) => Box::new(transformer::load(
            path,
            program,
            Some(enrichment.boot_tables()),
        )?),
        None => Box::new(stitcher::StateLogger),
    };
    // hand-written projections join the composite here
    Ok(Box::new(stitcher::CompositeProjection::new(vec![
        configured,
    ])))
}

#[cfg(not(feature = "transform"))]
fn build_projection(
    _cfg: &stitcher::config::Settings,
    _program: &stitcher::state::config::Program,
    _enrichment: &stitcher::enrichment::Enrichment,
) -> stitcher::StitcherResult<Box<dyn stitcher::Projection>> {
    Ok(Box::new(stitcher::StateLogger))
}
