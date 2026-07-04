use std::sync::Arc;

use anyhow::Result;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, StreamExt};
use twilight_http::Client as HttpClient;
use twilight_model::gateway::ShardId;

use super::events;

pub type SharedHttp = Arc<HttpClient>;

pub fn create_http_client() -> Result<HttpClient> {
    let token = std::env::var("DISCORD_TOKEN")?;
    Ok(HttpClient::new(token))
}

pub async fn connect(
    http: SharedHttp,
    manager: crate::wasm::loader::PluginManager,
) -> Result<(Shard, tokio::task::JoinHandle<Result<()>>)> {
    let token = std::env::var("DISCORD_TOKEN")?;
    let intents = Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;

    let shard = Shard::new(ShardId::ONE, token, intents);
    let handle = tokio::spawn(async move {
        bot_loop(shard, http, manager).await
    });

    let placeholder = Shard::new(ShardId::ONE, String::new(), Intents::empty());
    Ok((placeholder, handle))
}

async fn bot_loop(
    mut shard: Shard,
    http: SharedHttp,
    manager: crate::wasm::loader::PluginManager,
) -> Result<()> {
    tracing::info!("Connecting to Discord gateway...");

    while let Some(item) = shard.next_event(EventTypeFlags::all()).await {
        let event = match item {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(?e, "Gateway receive error");
                continue;
            }
        };

        if let Err(e) = handle_event(event, &http, &manager).await {
            tracing::error!(?e, "Event handler error");
        }
    }

    Ok(())
}

async fn handle_event(
    event: Event,
    _http: &SharedHttp,
    manager: &crate::wasm::loader::PluginManager,
) -> Result<()> {
    match event {
        Event::Ready(ready) => {
            tracing::info!(
                user = ?ready.user.name,
                "Bot is ready"
            );

            manager.dispatch_event("ready", Vec::new(), 0, 0).await;
            events::ready::handle(manager).await?;
        }
        Event::MessageCreate(msg) => {
            events::message::handle(&msg, manager).await?;
        }
        _ => {}
    }

    Ok(())
}
