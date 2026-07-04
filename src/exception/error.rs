use std::fmt;

#[derive(Debug)]
pub enum BotError {
    Config(String),
    Wasm(String),
    Discord(String),
    Other(String),
}

impl fmt::Display for BotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "Config error: {msg}"),
            Self::Wasm(msg) => write!(f, "Wasm error: {msg}"),
            Self::Discord(msg) => write!(f, "Discord error: {msg}"),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for BotError {}

impl From<anyhow::Error> for BotError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e.to_string())
    }
}

impl From<String> for BotError {
    fn from(s: String) -> Self {
        Self::Other(s)
    }
}
