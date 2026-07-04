use ynsrvcs::config::Config;

#[test]
fn config_from_env_uses_defaults() {
    unsafe {
        std::env::set_var("DISCORD_TOKEN", "test-token");
    }

    let cfg = Config::from_env();
    assert_eq!(cfg.discord_token, "test-token");
    assert_eq!(cfg.log_level, "ynsrvcs=info");
}
