#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();

    ynsrvcs::run().await
}

fn load_env() {
    let exe_env = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(".env")));

    let loaded_from_exe = exe_env
        .as_deref()
        .map_or(false, |path| dotenvy::from_path(path).is_ok());

    if !loaded_from_exe {
        let _ = dotenvy::dotenv();
    }
}
