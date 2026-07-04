use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub discord_token: String,
    pub plugin_dir: PathBuf,
    pub log_level: String,
    pub gateway_intents: Vec<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            discord_token: env("DISCORD_TOKEN"),
            plugin_dir: env_opt("PLUGIN_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("plugins")),
            log_level: env_opt("LOG_LEVEL")
                .unwrap_or_else(|| "ynsrvcs=info".into()),
            gateway_intents: env_opt("GATEWAY_INTENTS")
                .map(|v| v.split(',').map(String::from).collect())
                .unwrap_or_else(|| vec!["GUILD_MESSAGES".into(), "MESSAGE_CONTENT".into()]),
        }
    }
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set"))
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
