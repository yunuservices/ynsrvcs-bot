use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::info;

use super::loader::{PluginManager, plugin_dir};
use notify::{RecursiveMode, Watcher};

const RELOAD_DEBOUNCE: Duration = Duration::from_millis(300);

pub async fn watch(manager: PluginManager) {
    let dir = plugin_dir();
    if !dir.exists() {
        info!(
            "Plugin directory does not exist, hot-reload disabled: {}",
            dir.display()
        );
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
        let mut paths = HashSet::new();
        paths.insert(p);
        let mut deadline = Instant::now() + RELOAD_DEBOUNCE;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(next)) => {
                    paths.insert(next);
                    deadline = Instant::now() + RELOAD_DEBOUNCE;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        for path in paths {
            handle_file_event(&manager, &path).await;
        }
    }
}

async fn handle_file_event(manager: &PluginManager, path: &Path) {
    if path.exists() {
        match manager.reload_plugin(path).await {
            Ok(name) => info!("Hot-reloaded plugin: {name}"),
            Err(e) => tracing::error!(
                "Failed to hot-reload {}: {e}",
                PluginManager::plugin_name(path)
            ),
        }
    } else {
        let name = PluginManager::plugin_name(path);
        if manager.is_loaded(&name).await {
            manager.unload(&name).await;
            info!("Hot-unloaded plugin: {name}");
        }
    }
}
