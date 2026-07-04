pub mod bot;
pub mod config;
pub mod exception;
pub mod utils;
pub mod wasm;

use std::sync::Arc;

use anyhow::Result;

pub async fn run() -> Result<()> {
    let _cfg = config::Config::from_env();
    utils::helpers::init_logging()?;

    let engine = wasm::plugin::create_engine()?;
    let http_client = Arc::new(bot::handler::create_http_client()?);
    let plugin_manager = wasm::loader::PluginManager::new(&engine, Arc::clone(&http_client))?;
    plugin_manager.load_all().await?;

    tokio::spawn(wasm::hotreload::watch(plugin_manager.clone()));

    let (_shard, tasks) = bot::handler::connect(http_client, plugin_manager).await?;

    tasks.await??;
    Ok(())
}
