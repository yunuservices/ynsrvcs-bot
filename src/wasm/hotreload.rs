use std::path::Path;
use tracing::info;

use super::loader::{PluginManager, plugin_dir};
use notify::{RecursiveMode, Watcher};

pub async fn watch(manager: PluginManager) {
    let dir = plugin_dir();
    if !dir.exists() {
        info!("Plugin directory does not exist, hot-reload disabled: {}", dir.display());
        return;
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let mut watcher =
        match notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
            if let Ok(event) = res
                && let Some(p) = event.paths.first()
                && p.extension().is_some_and(|e| e == "wasm")
            {
                let _ = tx.send(p.clone());
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(?e, "Failed to create file watcher");
                return;
            }
        };

    if let Err(e) = watcher.watch(&dir, RecursiveMode::NonRecursive) {
        tracing::error!(?e, "Failed to watch plugin directory");
        return;
    }

    info!("Watching plugin directory: {}", dir.display());

    while let Some(p) = rx.recv().await {
        handle_file_event(&manager, &p).await;
    }
}

async fn handle_file_event(manager: &PluginManager, path: &Path) {
    let name = PluginManager::plugin_name(path);

    if path.exists() {
        if manager.is_loaded(&name).await {
            manager.unload(&name).await;
        }
        match manager.load(path).await {
            Ok(n) => info!("Hot-loaded plugin: {n}"),
            Err(e) => tracing::error!("Failed to hot-load {name}: {e}"),
        }
    } else if manager.is_loaded(&name).await {
        manager.unload(&name).await;
    }
}
