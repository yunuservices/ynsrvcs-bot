use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;
use wasmtime::component::Component;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView, WasiCtxView};

use super::plugin;

const PLUGIN_CALL_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HostContext {
    wasi: WasiCtx,
    table: ResourceTable,
    client: reqwest::Client,
    gateway_ping_ms: Arc<AtomicU64>,
    kv: Arc<Mutex<HashMap<(String, String), Vec<u8>>>>,
}

impl HostContext {
    pub fn new(
        gateway_ping_ms: Arc<AtomicU64>,
        kv: Arc<Mutex<HashMap<(String, String), Vec<u8>>>>,
    ) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().build(),
            table: ResourceTable::default(),
            client: reqwest::Client::new(),
            gateway_ping_ms,
            kv,
        }
    }
}

impl wasmtime::component::HasData for HostContext {
    type Data<'a> = &'a mut Self;
}

impl WasiView for HostContext {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl plugin::ynsrvcs::plugins::host::Host for HostContext {
    async fn http_request(
        &mut self,
        method: String,
        url: String,
        body: Vec<u8>,
    ) -> Result<plugin::ynsrvcs::plugins::host::Response, String> {
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| e.to_string())?;

        let req = self
            .client
            .request(method, &url)
            .body(body)
            .build()
            .map_err(|e| e.to_string())?;

        let resp = tokio::time::timeout(HTTP_TIMEOUT, self.client.execute(req))
            .await
            .map_err(|_| "http request timed out".to_string())?
            .map_err(|e| e.to_string())?;

        let status = resp.status().as_u16();
        let body = resp
            .bytes()
            .await
            .map_err(|e| e.to_string())?
            .to_vec();

        Ok(plugin::ynsrvcs::plugins::host::Response { status, body })
    }

    async fn get_env(&mut self, name: String) -> Option<String> {
        std::env::var(&name).ok().filter(|v| !v.is_empty())
    }

    async fn gateway_ping(&mut self) -> u64 {
        self.gateway_ping_ms.load(Ordering::Relaxed)
    }

    async fn now_ms(&mut self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    async fn log(&mut self, level: String, message: String) {
        match level.to_lowercase().as_str() {
            "error" => tracing::error!("{message}"),
            "warn" => tracing::warn!("{message}"),
            "info" => tracing::info!("{message}"),
            "debug" => tracing::debug!("{message}"),
            "trace" => tracing::trace!("{message}"),
            _ => tracing::info!("{message}"),
        }
    }

    async fn kv_get(&mut self, scope: String, key: String) -> Option<Vec<u8>> {
        self.kv.lock().ok()?.get(&(scope, key)).cloned()
    }

    async fn kv_set(&mut self, scope: String, key: String, value: Vec<u8>) {
        if let Ok(mut kv) = self.kv.lock() {
            kv.insert((scope, key), value);
        }
    }

    async fn fs_read(&mut self, path: String) -> Result<Vec<u8>, String> {
        tokio::fs::read(&path).await.map_err(|e| e.to_string())
    }

    async fn fs_write(&mut self, path: String, content: Vec<u8>) -> Result<(), String> {
        tokio::fs::write(&path, &content)
            .await
            .map_err(|e| e.to_string())
    }
}

pub(crate) struct LoadedPlugin {
    world: plugin::PluginWorld,
    store: Store<HostContext>,
}

#[derive(Clone)]
pub struct PluginManager {
    plugins: Arc<AsyncMutex<HashMap<String, LoadedPlugin>>>,
    engine: Arc<Engine>,
    gateway_ping_ms: Arc<AtomicU64>,
    kv: Arc<Mutex<HashMap<(String, String), Vec<u8>>>>,
}

pub fn plugin_dir() -> String {
    std::env::var("PLUGIN_DIR").unwrap_or_else(|_| "./plugins".into())
}

impl PluginManager {
    pub fn new(engine: &Engine) -> Result<Self> {
        Ok(Self {
            plugins: Arc::new(AsyncMutex::new(HashMap::new())),
            engine: Arc::new(engine.clone()),
            gateway_ping_ms: Arc::new(AtomicU64::new(0)),
            kv: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn set_gateway_ping_ms(&self, ms: u64) {
        self.gateway_ping_ms.store(ms, Ordering::Relaxed);
    }

    pub async fn load_all(&self) -> Result<()> {
        let dir = plugin_dir();
        let path = Path::new(&dir);
        if !path.exists() {
            tokio::fs::create_dir_all(path).await?;
            info!("Created plugin directory: {dir}");
        }

        let mut entries = Vec::new();
        let mut read = tokio::fs::read_dir(path).await?;
        while let Some(entry) = read.next_entry().await? {
            let p = entry.path();
            if p.extension().is_some_and(|e| e == "wasm") {
                entries.push(p);
            }
        }

        let mut loaded_plugins = Vec::new();
        for wasm_path in &entries {
            match Self::load_one(
                &self.engine,
                Arc::clone(&self.gateway_ping_ms),
                Arc::clone(&self.kv),
                wasm_path,
            )
            .await
            {
                Ok((name, loaded)) => {
                    loaded_plugins.push((name, loaded));
                }
                Err(e) => {
                    tracing::error!("Failed to load {}: {e}", wasm_path.display());
                }
            }
        }

        let mut plugins = self.plugins.lock().await;
        for (name, loaded) in loaded_plugins {
            plugins.insert(name, loaded);
        }

        info!(count = plugins.len(), "Plugins loaded");
        Ok(())
    }

    pub fn plugin_name(path: &Path) -> String {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    pub(crate) async fn load_one(
        engine: &Engine,
        gateway_ping_ms: Arc<AtomicU64>,
        kv: Arc<Mutex<HashMap<(String, String), Vec<u8>>>>,
        wasm_path: &Path,
    ) -> Result<(String, LoadedPlugin)> {
        let bytes = tokio::fs::read(wasm_path).await?;
        let name = Self::plugin_name(wasm_path);

        let component = Component::new(engine, &bytes)?;
        let mut linker = wasmtime::component::Linker::new(engine);

        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

        plugin::PluginWorld::add_to_linker::<HostContext, HostContext>(
            &mut linker,
            |s: &mut HostContext| s,
        )?;

        let mut store = Store::new(engine, HostContext::new(gateway_ping_ms, kv));
        let world = plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await?;

        Ok((
            name,
            LoadedPlugin {
                world,
                store,
            },
        ))
    }

    pub async fn load(&self, wasm_path: &Path) -> Result<String> {
        let (name, loaded) = Self::load_one(
            &self.engine,
            Arc::clone(&self.gateway_ping_ms),
            Arc::clone(&self.kv),
            wasm_path,
        )
        .await?;
        self.plugins.lock().await.insert(name.clone(), loaded);
        Ok(name)
    }

    pub async fn unload(&self, name: &str) {
        self.plugins.lock().await.remove(name);
        info!("Plugin unloaded: {name}");
    }

    pub async fn unload_by_path(&self, wasm_path: &Path) {
        let name = Self::plugin_name(wasm_path);
        self.unload(&name).await;
    }

    pub async fn is_loaded(&self, name: &str) -> bool {
        self.plugins.lock().await.contains_key(name)
    }

    pub async fn loaded_names(&self) -> Vec<String> {
        self.plugins.lock().await.keys().cloned().collect()
    }

    pub async fn dispatch_event(
        &self,
        event_type: &str,
        payload: Vec<u8>,
        guild_id: u64,
        channel_id: u64,
    ) {
        let mut plugins = self.plugins.lock().await;
        for (_name, loaded) in plugins.iter_mut() {
            let guest = loaded.world.ynsrvcs_plugins_plugin();
            let fut = guest.call_handle_event(
                &mut loaded.store,
                event_type,
                &payload,
                guild_id,
                channel_id,
            );

            match tokio::time::timeout(PLUGIN_CALL_TIMEOUT, fut).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!("Plugin {_name} error handling {event_type}: {e}"),
                Err(_) => tracing::error!("Plugin {_name} timed out handling {event_type}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_ping_wasm() -> Result<std::path::PathBuf> {
        let root = std::env::var("CARGO_MANIFEST_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_default();
        let wasm_path = root.join("plugins").join("ping.wasm");
        if wasm_path.exists() {
            return Ok(wasm_path);
        }

        let plugin_dir = root.join("example-plugin");
        let output = std::process::Command::new("cargo")
            .args([
                "build",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
                plugin_dir.join("Cargo.toml").to_str().unwrap(),
            ])
            .output()
            .expect("failed to build example-plugin");

        if !output.status.success() {
            panic!(
                "example-plugin build failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let artifact = plugin_dir
            .join("target")
            .join("wasm32-wasip2")
            .join("debug")
            .join("ping_plugin.wasm");
        if !artifact.exists() {
            panic!("expected wasm artifact at {}", artifact.display());
        }

        std::fs::create_dir_all(wasm_path.parent().unwrap())?;
        std::fs::copy(&artifact, &wasm_path)?;
        Ok(wasm_path)
    }

    #[tokio::test]
    async fn test_load_ping_plugin() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let wasm_path = ensure_ping_wasm()?;
        let engine = crate::wasm::plugin::create_engine()?;
        let (name, mut loaded) = PluginManager::load_one(
            &engine,
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
            &wasm_path,
        )
        .await?;

        assert_eq!(name, "ping");

        loaded.world.ynsrvcs_plugins_plugin().call_handle_event(
            &mut loaded.store,
            "MESSAGE_CREATE",
            b"hello",
            0,
            0,
        ).await?;

        Ok(())
    }
}
