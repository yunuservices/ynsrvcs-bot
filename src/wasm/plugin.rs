use std::sync::Arc;
use wasmtime::Engine;

pub type SharedEngine = Arc<Engine>;

pub fn create_engine() -> anyhow::Result<Engine> {
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    config.wasm_multi_memory(true);
    Ok(Engine::new(&config)?)
}

wasmtime::component::bindgen!({
    path: "src/wasm/wit",
    world: "plugin-world",
    imports: { default: async },
    exports: { default: async },
});
