//! `stitcher` — the generic, fully config-driven binary (lantern #36/#37): wires
//! `config/<RUN_ENV>.toml` (+ `[state] config_file`) to a config-driven
//! [`stitcher::state::ConfigProcessor`] and runs the pipeline. No domain code.

#[tokio::main]
async fn main() -> stitcher::StitcherResult<()> {
    let cfg = stitcher::config::load()?;
    stitcher::telemetry::init(&cfg.log, cfg.debug.any_print())?;

    let state_cfg = cfg.state.as_ref().ok_or_else(|| {
        error_stack::report!(stitcher::StitcherError::Config(
            "the stitcher binary requires [state] config_file (a state.yaml)".into()
        ))
    })?;
    let program = stitcher::state::config::load(&state_cfg.config_file)?;
    tracing::info!(
        schema = %program.aggregate,
        version = program.version,
        "loaded state config"
    );

    stitcher::run(
        stitcher::state::ConfigProcessor::new(program, &cfg.tenant_ids),
        cfg,
    )
    .await
}
