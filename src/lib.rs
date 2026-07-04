pub mod bot;
pub mod config;
pub mod exception;
pub mod utils;
pub mod wasm;

use anyhow::Result;

pub async fn run() -> Result<()> {
    let _cfg = config::Config::from_env();
    utils::helpers::init_logging()?;

    let engine = wasm::plugin::create_engine()?;
    let plugin_manager = wasm::loader::PluginManager::new(&engine)?;
    plugin_manager.load_all().await?;

    let watch_handle = tokio::spawn(wasm::hotreload::watch(plugin_manager.clone()));

    let (shutdown_tx, mut tasks) = bot::handler::connect(plugin_manager.clone()).await?;

    let mut shutdown = false;
    tokio::select! {
        res = &mut tasks => {
            res??;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutdown signal received, closing gateway connection...");
            drop(shutdown_tx);
            shutdown = true;
        }
    }

    if shutdown {
        if let Err(e) = tasks.await {
            tracing::error!(?e, "Bot loop terminated unexpectedly");
        }
    }

    watch_handle.abort();
    let _ = watch_handle.await;

    plugin_manager.unload_all().await;
    tracing::info!("Shutdown complete");

    Ok(())
}
