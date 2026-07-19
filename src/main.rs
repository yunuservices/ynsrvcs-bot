#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
        .is_some_and(|path| dotenvy::from_path(path).is_ok());

    if !loaded_from_exe {
        let _ = dotenvy::dotenv();
    }
}
