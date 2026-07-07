use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use anyhow::Result;
use songbird::{Songbird, shards::TwilightMap};
use tokio::sync::Notify;
use tokio_stream::StreamExt;
use twilight_gateway::{CloseFrame, Intents, Message, MessageSender, Shard};
use twilight_model::gateway::ShardId;
use twilight_model::gateway::event::{Event as GatewayEvent, GatewayEventDeserializer};
use twilight_model::gateway::payload::incoming::{VoiceServerUpdate, VoiceStateUpdate};
use twilight_model::id::Id;
use twilight_model::id::marker::UserMarker;

const GATEWAY_BOT_URL: &str = "https://discord.com/api/v10/gateway/bot";
const HTTP_TIMEOUT_SECONDS: u64 = 15;

pub async fn connect(
    manager: crate::wasm::loader::PluginManager,
) -> Result<(
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let token = std::env::var("DISCORD_TOKEN")?;
    let intents = Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT | Intents::GUILD_VOICE_STATES;

    let shard_count = fetch_recommended_shard_count(&token).await?;
    tracing::info!(shard_count, "Shard configuration created");

    let mut shards = Vec::with_capacity(shard_count);
    for shard_id in 0..shard_count {
        shards.push(Shard::new(
            ShardId::new(shard_id as u32, shard_count as u32),
            token.clone(),
            intents,
        ));
    }

    let senders: Vec<MessageSender> = shards.iter().map(|s| s.sender()).collect();
    manager.set_shard_senders(senders).await;
    manager.set_shard_count(shard_count as u64);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let notify = Arc::new(Notify::new());

    // Bridge the one-shot shutdown signal into a notify broadcast so every shard
    // can react without needing its own channel.
    let notify_clone = Arc::clone(&notify);
    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        notify_clone.notify_waiters();
    });

    let mut tasks = Vec::with_capacity(shards.len());
    for shard in shards {
        let manager = manager.clone();
        let notify = Arc::clone(&notify);
        tasks.push(tokio::spawn(bot_loop(shard, manager, notify)));
    }

    let handle = tokio::spawn(async move {
        for task in tasks {
            task.await??;
        }
        Ok(())
    });

    Ok((shutdown_tx, handle))
}

async fn fetch_recommended_shard_count(token: &str) -> Result<usize> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .build()?;

    let response = client
        .get(GATEWAY_BOT_URL)
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await?
        .error_for_status()?;

    let body: serde_json::Value = response.json().await?;
    let shards = body
        .get("shards")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .max(1) as usize;

    Ok(shards)
}

async fn bot_loop(
    mut shard: Shard,
    manager: crate::wasm::loader::PluginManager,
    notify: Arc<Notify>,
) -> Result<()> {
    let shard_id = shard.id();
    tracing::info!(?shard_id, "Connecting shard to Discord gateway...");

    loop {
        let item = tokio::select! {
            biased;
            _ = notify.notified() => {
                shard.close(CloseFrame::NORMAL);
                return Ok(());
            }
            item = shard.next() => item,
        };
        let msg = match item {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                tracing::warn!(?e, ?shard_id, "Gateway receive error");
                continue;
            }
            None => break,
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
        };

        let Some(gateway_deserializer) = GatewayEventDeserializer::from_json(&text) else {
            tracing::debug!(?shard_id, "Failed to parse gateway frame header");
            continue;
        };

        // Only dispatch op 0 events to plugins; all other opcodes are internal.
        if gateway_deserializer.op() != 0 {
            continue;
        }

        let event_name = gateway_deserializer
            .event_type()
            .unwrap_or("UNKNOWN")
            .to_string();

        let payload: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(?e, ?shard_id, event = %event_name, "Failed to parse gateway payload");
                continue;
            }
        };

        let data = payload.get("d").cloned().unwrap_or_default();
        let data_bytes = serde_json::to_vec(&data)?;

        if event_name == "READY" {
            let user = data
                .get("user")
                .and_then(|u| u.get("username"))
                .and_then(|v| v.as_str());
            if let Some(user_id) = data
                .get("user")
                .and_then(|u| u.get("id"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                && let Some(nz) = NonZeroU64::new(user_id)
            {
                init_songbird(&manager, Id::<UserMarker>::new(nz.get())).await;
            }
            if let Some(app_id) = data
                .get("application")
                .and_then(|a| a.get("id"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
            {
                manager.set_application_id(app_id);
            }
            tracing::info!(user = ?user, ?shard_id, "Bot is ready");
        }

        match event_name.as_str() {
            "VOICE_STATE_UPDATE" => {
                if let Ok(event) = serde_json::from_value::<Box<VoiceStateUpdate>>(data.clone()) {
                    manager
                        .process_voice_event(GatewayEvent::VoiceStateUpdate(event))
                        .await;
                }
            }
            "VOICE_SERVER_UPDATE" => {
                if let Ok(event) = serde_json::from_value::<VoiceServerUpdate>(data.clone()) {
                    manager
                        .process_voice_event(GatewayEvent::VoiceServerUpdate(event))
                        .await;
                }
            }
            _ => {}
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

async fn init_songbird(manager: &crate::wasm::loader::PluginManager, user_id: Id<UserMarker>) {
    let senders = manager.clone_shard_senders().await;
    let mut shard_map = HashMap::with_capacity(senders.len());
    for (idx, sender) in senders.into_iter().enumerate() {
        shard_map.insert(idx as u32, sender);
    }

    let twilight_map = Arc::new(TwilightMap::new(shard_map));
    let songbird = Songbird::twilight(twilight_map, user_id);
    manager.set_songbird(songbird).await;
    tracing::info!(%user_id, "Songbird voice driver ready");
}
