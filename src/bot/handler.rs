use anyhow::Result;
use tokio_stream::StreamExt;
use twilight_gateway::{CloseFrame, Intents, Message, Shard};
use twilight_model::gateway::ShardId;

pub async fn connect(
    manager: crate::wasm::loader::PluginManager,
) -> Result<(
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let token = std::env::var("DISCORD_TOKEN")?;
    let intents = Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;

    let shard = Shard::new(ShardId::ONE, token, intents);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        bot_loop(shard, manager, shutdown_rx).await
    });

    Ok((shutdown_tx, handle))
}

async fn bot_loop(
    mut shard: Shard,
    manager: crate::wasm::loader::PluginManager,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    tracing::info!("Connecting to Discord gateway...");

    loop {
        let item = tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                let _ = shard.close(CloseFrame::NORMAL);
                return Ok(());
            }
            item = shard.next() => item,
        };
        let msg = match item {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                tracing::warn!(?e, "Gateway receive error");
                continue;
            }
            None => break,
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
        };

        let payload: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(?e, "Failed to parse gateway message");
                continue;
            }
        };

        if payload.get("op").and_then(|v| v.as_u64()) != Some(0) {
            continue;
        }

        let event_name = payload
            .get("t")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN")
            .to_string();
        let data = payload.get("d").cloned().unwrap_or_default();
        let data_bytes = serde_json::to_vec(&data)?;

        if event_name == "READY" {
            let user = data.get("user").and_then(|u| u.get("username")).and_then(|v| v.as_str());
            tracing::info!(user = ?user, "Bot is ready");
        }

        let guild_id = data
            .get("guild_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let channel_id = data
            .get("channel_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .or_else(|| {
                data.get("channel")
                    .and_then(|c| c.get("id"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0);

        manager.set_gateway_ping_ms(
            shard
                .latency()
                .average()
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        );

        manager
            .dispatch_event(&event_name, data_bytes, guild_id, channel_id)
            .await;
    }

    Ok(())
}
