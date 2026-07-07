use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Deserialize;
use songbird::Songbird;
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;
use twilight_gateway::MessageSender;
use twilight_model::gateway::event::Event as GatewayEvent;
use twilight_model::gateway::payload::outgoing::{
    RequestGuildMembers, UpdatePresence, UpdateVoiceState,
};
use twilight_model::gateway::presence::{Activity, ActivityType, Status};
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker};
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use super::kv::KvStore;
use super::plugin;

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct PluginPermissions {
    #[serde(default)]
    pub http: bool,
    #[serde(default)]
    pub fs_read: bool,
    #[serde(default)]
    pub fs_write: bool,
    #[serde(default)]
    pub env: bool,
    #[serde(default)]
    pub kv: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct PluginLimits {
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: usize,
    #[serde(default = "default_max_instances")]
    pub max_instances: usize,
    #[serde(default = "default_max_tables")]
    pub max_tables: usize,
    #[serde(default = "default_max_memories")]
    pub max_memories: usize,
}

impl Default for PluginLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: default_max_memory_bytes(),
            max_instances: default_max_instances(),
            max_tables: default_max_tables(),
            max_memories: default_max_memories(),
        }
    }
}

fn default_max_memory_bytes() -> usize {
    64 * 1024 * 1024 // 64 MiB
}

fn default_max_instances() -> usize {
    10
}

fn default_max_tables() -> usize {
    10
}

fn default_max_memories() -> usize {
    1
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct PluginConfig {
    #[serde(flatten)]
    pub permissions: PluginPermissions,
    #[serde(default)]
    pub limits: PluginLimits,
}

pub struct PluginResourceLimiter {
    limits: PluginLimits,
}

impl PluginResourceLimiter {
    fn new(limits: PluginLimits) -> Self {
        Self { limits }
    }
}

impl wasmtime::ResourceLimiter for PluginResourceLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(desired <= self.limits.max_memory_bytes)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(true)
    }

    fn instances(&self) -> usize {
        self.limits.max_instances
    }

    fn memories(&self) -> usize {
        self.limits.max_memories
    }

    fn tables(&self) -> usize {
        self.limits.max_tables
    }
}

const PLUGIN_CALL_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const PLUGIN_FUEL: u64 = 50_000_000;
const FUEL_ASYNC_YIELD_INTERVAL: u64 = 10_000;
const PLUGIN_HOSTCALL_FUEL: usize = 1_000_000;

fn is_discord_api_url(url: &str) -> bool {
    url.starts_with("https://discord.com/api/")
        || url.starts_with("https://canary.discord.com/api/")
}

pub struct HostContext {
    wasi: WasiCtx,
    table: wasmtime::component::ResourceTable,
    client: reqwest::Client,
    gateway_ping_ms: Arc<AtomicU64>,
    application_id: Arc<AtomicU64>,
    shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
    shard_count: Arc<AtomicU64>,
    songbird: Arc<AsyncMutex<Option<Songbird>>>,
    kv: KvStore,
    workspace: PathBuf,
    config: PluginConfig,
    limiter: PluginResourceLimiter,
}

impl HostContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gateway_ping_ms: Arc<AtomicU64>,
        application_id: Arc<AtomicU64>,
        shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
        shard_count: Arc<AtomicU64>,
        songbird: Arc<AsyncMutex<Option<Songbird>>>,
        kv: KvStore,
        workspace: PathBuf,
        config: PluginConfig,
    ) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().build(),
            table: wasmtime::component::ResourceTable::default(),
            client: reqwest::Client::new(),
            gateway_ping_ms,
            application_id,
            shard_senders,
            shard_count,
            songbird,
            kv,
            workspace,
            limiter: PluginResourceLimiter::new(config.limits),
            config,
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
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;

        let mut req_builder = self.client.request(method, &url);
        if is_discord_api_url(&url) {
            if let Ok(token) = std::env::var("DISCORD_TOKEN") {
                req_builder = req_builder.header("Authorization", format!("Bot {token}"));
            }
            if !body.is_empty() {
                req_builder = req_builder.header("Content-Type", "application/json");
            }
        }
        let req = req_builder.body(body).build().map_err(|e| e.to_string())?;

        let resp = tokio::time::timeout(HTTP_TIMEOUT, self.client.execute(req))
            .await
            .map_err(|_| "http request timed out".to_string())?
            .map_err(|e| e.to_string())?;

        let status = resp.status().as_u16();
        let body = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
        if status >= 400 {
            let text = String::from_utf8_lossy(&body);
            tracing::warn!("http_request returned {status} for {url}: {text}");
        }

        Ok(plugin::ynsrvcs::plugins::host::Response { status, body })
    }

    async fn send_channel_message_with_attachments(
        &mut self,
        channel_id: u64,
        content: String,
        attachments: Vec<plugin::ynsrvcs::plugins::host::Attachment>,
    ) -> Result<plugin::ynsrvcs::plugins::host::Response, String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");

        let attachment_meta: Vec<serde_json::Value> = attachments
            .iter()
            .enumerate()
            .map(|(idx, a)| {
                serde_json::json!({
                    "id": idx.to_string(),
                    "filename": a.filename,
                    "description": "",
                })
            })
            .collect();
        let payload = serde_json::json!({
            "content": content,
            "attachments": attachment_meta,
        })
        .to_string();

        let mut form = reqwest::multipart::Form::new().text("payload_json", payload);
        for (idx, attachment) in attachments.into_iter().enumerate() {
            let part = reqwest::multipart::Part::bytes(attachment.data)
                .file_name(attachment.filename)
                .mime_str(&attachment.content_type)
                .map_err(|e| e.to_string())?;
            form = form.part(format!("files[{idx}]"), part);
        }

        let req = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .multipart(form)
            .build()
            .map_err(|e| e.to_string())?;

        let resp = tokio::time::timeout(HTTP_TIMEOUT, self.client.execute(req))
            .await
            .map_err(|_| "http request timed out".to_string())?
            .map_err(|e| e.to_string())?;

        let status = resp.status().as_u16();
        let body = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
        if status >= 400 {
            let text = String::from_utf8_lossy(&body);
            tracing::warn!(
                "send_channel_message_with_attachments returned {status} for {url}: {text}"
            );
        }

        Ok(plugin::ynsrvcs::plugins::host::Response { status, body })
    }

    async fn send_channel_message_with_components(
        &mut self,
        channel_id: u64,
        content: String,
        components: String,
    ) -> Result<plugin::ynsrvcs::plugins::host::Response, String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let components_json: serde_json::Value = if components.trim().is_empty() {
            serde_json::Value::Array(Vec::new())
        } else {
            serde_json::from_str(&components)
                .map_err(|e| format!("invalid components json: {e}"))?
        };

        let body = serde_json::json!({
            "content": content,
            "components": components_json,
        });

        let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = resp.status().as_u16();
        let body_bytes = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
        if status >= 400 {
            tracing::warn!(
                status,
                url,
                text = %String::from_utf8_lossy(&body_bytes),
                "send_channel_message_with_components failed"
            );
        }

        Ok(plugin::ynsrvcs::plugins::host::Response {
            status,
            body: body_bytes,
        })
    }

    async fn reply_to_interaction(
        &mut self,
        interaction_id: u64,
        interaction_token: String,
        content: String,
        ephemeral: bool,
    ) -> Result<(), String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let mut data = serde_json::json!({
            "content": content,
        });
        if ephemeral {
            data["flags"] = 64.into();
        }

        let body = serde_json::json!({
            "type": 4,
            "data": data,
        });

        let url = format!(
            "https://discord.com/api/v10/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            Err(format!("interaction reply failed ({status}): {text}"))
        }
    }

    async fn edit_interaction_message(
        &mut self,
        interaction_token: String,
        content: String,
        components: String,
    ) -> Result<(), String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let app_id = self.application_id.load(Ordering::Relaxed).to_string();
        if app_id == "0" {
            return Err("application id not available".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let components_json: serde_json::Value = if components.trim().is_empty() {
            serde_json::Value::Array(Vec::new())
        } else {
            serde_json::from_str(&components)
                .map_err(|e| format!("invalid components json: {e}"))?
        };

        let body = serde_json::json!({
            "content": content,
            "components": components_json,
        });

        let url = format!(
            "https://discord.com/api/v10/webhooks/{app_id}/{interaction_token}/messages/@original"
        );
        let resp = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            Err(format!(
                "edit interaction message failed ({status}): {text}"
            ))
        }
    }

    async fn show_modal(
        &mut self,
        interaction_id: u64,
        interaction_token: String,
        title: String,
        custom_id: String,
        components: String,
    ) -> Result<(), String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let components_json: serde_json::Value = serde_json::from_str(&components)
            .map_err(|e| format!("invalid components json: {e}"))?;

        let body = serde_json::json!({
            "type": 9,
            "data": {
                "custom_id": custom_id,
                "title": title,
                "components": components_json,
            }
        });

        let url = format!(
            "https://discord.com/api/v10/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            Err(format!("show modal failed ({status}): {text}"))
        }
    }

    async fn get_env(&mut self, name: String) -> Option<String> {
        if !self.config.permissions.env {
            return None;
        }

        std::env::var(&name).ok().filter(|v| !v.is_empty())
    }

    async fn gateway_ping(&mut self) -> u64 {
        self.gateway_ping_ms.load(Ordering::Relaxed)
    }

    async fn application_id(&mut self) -> Option<String> {
        let id = self.application_id.load(Ordering::Relaxed);
        if id == 0 { None } else { Some(id.to_string()) }
    }

    async fn update_presence(
        &mut self,
        status: String,
        activity_type: u8,
        activity_name: String,
    ) -> Result<(), String> {
        let kind = match activity_type {
            1 => ActivityType::Streaming,
            2 => ActivityType::Listening,
            3 => ActivityType::Watching,
            4 => ActivityType::Custom,
            5 => ActivityType::Competing,
            _ => ActivityType::Playing,
        };
        let activity = Activity {
            application_id: None,
            assets: None,
            buttons: Vec::new(),
            created_at: None,
            details: None,
            emoji: None,
            flags: None,
            id: None,
            instance: None,
            kind,
            name: activity_name,
            party: None,
            secrets: None,
            state: None,
            timestamps: None,
            url: None,
        };
        let status = match status.to_lowercase().as_str() {
            "dnd" | "donotdisturb" => Status::DoNotDisturb,
            "idle" => Status::Idle,
            "invisible" => Status::Invisible,
            "offline" => Status::Offline,
            _ => Status::Online,
        };
        let command = UpdatePresence::new(vec![activity], false, None::<u64>, status)
            .map_err(|e| e.to_string())?;

        let senders = self.shard_senders.lock().await;
        if senders.is_empty() {
            return Err("no shard senders available".to_string());
        }
        for sender in senders.iter() {
            sender.command(&command).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn update_voice_state(
        &mut self,
        guild_id: u64,
        channel_id: Option<u64>,
        self_mute: bool,
        self_deaf: bool,
    ) -> Result<(), String> {
        let shard_count = self.shard_count.load(Ordering::Relaxed);
        if shard_count == 0 {
            return Err("shards not ready".to_string());
        }
        let shard_id = ((guild_id >> 22) % shard_count) as usize;
        let senders = self.shard_senders.lock().await;
        let sender = senders
            .get(shard_id)
            .ok_or_else(|| "shard sender not found".to_string())?;

        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;
        let channel_id = channel_id
            .and_then(NonZeroU64::new)
            .map(|nz| Id::<ChannelMarker>::new(nz.get()));
        let command = UpdateVoiceState::new(guild_id, channel_id, self_deaf, self_mute);
        sender.command(&command).map_err(|e| e.to_string())
    }

    async fn request_guild_members(&mut self, guild_id: u64) -> Result<(), String> {
        let shard_count = self.shard_count.load(Ordering::Relaxed);
        if shard_count == 0 {
            return Err("shards not ready".to_string());
        }
        let shard_id = ((guild_id >> 22) % shard_count) as usize;
        let senders = self.shard_senders.lock().await;
        let sender = senders
            .get(shard_id)
            .ok_or_else(|| "shard sender not found".to_string())?;

        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;
        let command = RequestGuildMembers::builder(guild_id).query("", None);
        sender.command(&command).map_err(|e| e.to_string())
    }

    async fn join_voice_channel(
        &mut self,
        guild_id: u64,
        channel_id: u64,
        self_mute: bool,
        self_deaf: bool,
    ) -> Result<(), String> {
        self.update_voice_state(guild_id, Some(channel_id), self_mute, self_deaf)
            .await
    }

    async fn leave_voice_channel(&mut self, guild_id: u64) -> Result<(), String> {
        self.update_voice_state(guild_id, None, false, false).await
    }

    async fn play_audio_url(&mut self, guild_id: u64, url: String) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let mut call = call.lock().await;

        let input: songbird::input::Input =
            songbird::input::HttpRequest::new(self.client.clone(), url).into();
        call.play_input(input);
        Ok(())
    }

    async fn stop_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let mut call = call.lock().await;

        call.stop();
        Ok(())
    }

    async fn pause_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        call.queue().pause().map_err(|e| e.to_string())
    }

    async fn resume_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        call.queue().resume().map_err(|e| e.to_string())
    }

    async fn skip_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        call.queue().skip().map_err(|e| e.to_string())
    }

    async fn set_volume(&mut self, guild_id: u64, volume: f32) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        let handle = call
            .queue()
            .current()
            .ok_or_else(|| "no active track".to_string())?;

        handle.set_volume(volume).map_err(|e| e.to_string())
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
        if !self.config.permissions.kv {
            return None;
        }

        self.kv.get(&scope, &key)
    }

    async fn kv_set(&mut self, scope: String, key: String, value: Vec<u8>) {
        if !self.config.permissions.kv {
            return;
        }

        self.kv.set(scope, key, value);
    }

    async fn fs_read(&mut self, path: String) -> Result<Vec<u8>, String> {
        if !self.config.permissions.fs_read {
            return Err("fs read is not permitted".to_string());
        }

        tokio::fs::read(self.workspace.join(path))
            .await
            .map_err(|e| e.to_string())
    }

    async fn fs_write(&mut self, path: String, content: Vec<u8>) -> Result<(), String> {
        if !self.config.permissions.fs_write {
            return Err("fs write is not permitted".to_string());
        }

        let path = self.workspace.join(path);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| e.to_string())?;
        }
        tokio::fs::write(&path, &content)
            .await
            .map_err(|e| e.to_string())
    }
}

pub(crate) struct LoadedPlugin {
    component: Arc<Component>,
    config: PluginConfig,
}

const PLUGIN_FAILURE_THRESHOLD: u32 = 5;

#[derive(Clone)]
pub struct PluginManager {
    plugins: Arc<AsyncMutex<HashMap<String, LoadedPlugin>>>,
    engine: Arc<Engine>,
    gateway_ping_ms: Arc<AtomicU64>,
    application_id: Arc<AtomicU64>,
    shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
    shard_count: Arc<AtomicU64>,
    songbird: Arc<AsyncMutex<Option<Songbird>>>,
    plugin_failures: Arc<AsyncMutex<HashMap<String, u32>>>,
    kv: KvStore,
}

pub fn plugin_dir() -> PathBuf {
    std::env::var("PLUGIN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./plugins"))
}

fn workspace_path(name: &str) -> PathBuf {
    plugin_dir().join(name).join("workspace")
}

async fn load_plugin_config(wasm_path: &Path) -> PluginConfig {
    let config_path = wasm_path.with_extension("json");

    if !config_path.exists() {
        return PluginConfig::default();
    }

    match tokio::fs::read_to_string(&config_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(err) => {
            tracing::warn!(
                "Failed to read plugin config at {}: {err}",
                config_path.display()
            );
            PluginConfig::default()
        }
    }
}

fn configure_store(store: &mut Store<HostContext>) -> Result<()> {
    store.set_fuel(PLUGIN_FUEL)?;
    store.fuel_async_yield_interval(Some(FUEL_ASYNC_YIELD_INTERVAL))?;
    store.set_hostcall_fuel(PLUGIN_HOSTCALL_FUEL);
    store.limiter(|state| &mut state.limiter);
    Ok(())
}

fn create_linker(engine: &Engine) -> Result<Linker<HostContext>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    plugin::PluginWorld::add_to_linker::<HostContext, HostContext>(&mut linker, |s| s)?;
    Ok(linker)
}

impl PluginManager {
    pub fn new(engine: &Engine) -> Result<Self> {
        Ok(Self {
            plugins: Arc::new(AsyncMutex::new(HashMap::new())),
            engine: Arc::new(engine.clone()),
            gateway_ping_ms: Arc::new(AtomicU64::new(0)),
            application_id: Arc::new(AtomicU64::new(0)),
            shard_senders: Arc::new(AsyncMutex::new(Vec::new())),
            shard_count: Arc::new(AtomicU64::new(0)),
            songbird: Arc::new(AsyncMutex::new(None)),
            plugin_failures: Arc::new(AsyncMutex::new(HashMap::new())),
            kv: KvStore::load_or_default(super::kv::kv_path())?,
        })
    }

    pub fn set_gateway_ping_ms(&self, ms: u64) {
        self.gateway_ping_ms.store(ms, Ordering::Relaxed);
    }

    pub fn set_application_id(&self, id: u64) {
        self.application_id.store(id, Ordering::Relaxed);
    }

    pub async fn set_shard_senders(&self, senders: Vec<MessageSender>) {
        *self.shard_senders.lock().await = senders;
    }

    pub async fn clone_shard_senders(&self) -> Vec<MessageSender> {
        self.shard_senders.lock().await.clone()
    }

    pub fn set_shard_count(&self, count: u64) {
        self.shard_count.store(count, Ordering::Relaxed);
    }

    pub async fn set_songbird(&self, songbird: Songbird) {
        *self.songbird.lock().await = Some(songbird);
    }

    pub async fn process_voice_event(&self, event: GatewayEvent) {
        if let Some(songbird) = self.songbird.lock().await.as_ref() {
            songbird.process(&event).await;
        }
    }

    pub async fn load_all(&self) -> Result<()> {
        let path = plugin_dir();
        if !path.exists() {
            tokio::fs::create_dir_all(&path).await?;
            info!("Created plugin directory: {}", path.display());
        }

        let mut entries = Vec::new();
        let mut read = tokio::fs::read_dir(&path).await?;
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
                Arc::clone(&self.application_id),
                Arc::clone(&self.shard_senders),
                Arc::clone(&self.shard_count),
                Arc::clone(&self.songbird),
                self.kv.clone(),
                wasm_path,
            )
            .await
            {
                Ok((name, loaded)) => loaded_plugins.push((name, loaded)),
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_one(
        engine: &Engine,
        gateway_ping_ms: Arc<AtomicU64>,
        application_id: Arc<AtomicU64>,
        shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
        shard_count: Arc<AtomicU64>,
        songbird: Arc<AsyncMutex<Option<Songbird>>>,
        kv: KvStore,
        wasm_path: &Path,
    ) -> Result<(String, LoadedPlugin)> {
        let bytes = tokio::fs::read(wasm_path).await?;
        let name = Self::plugin_name(wasm_path);

        let component = Component::new(engine, &bytes)?;
        let workspace = workspace_path(&name);
        let config = load_plugin_config(wasm_path).await;
        tokio::fs::create_dir_all(&workspace).await?;

        let mut store = Store::new(
            engine,
            HostContext::new(
                gateway_ping_ms,
                application_id,
                shard_senders,
                shard_count,
                songbird,
                kv.clone(),
                workspace.clone(),
                config,
            ),
        );
        configure_store(&mut store)?;

        let linker = create_linker(engine)?;
        let instance =
            plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await?;

        match instance
            .ynsrvcs_plugins_plugin()
            .call_initialize(&mut store, None)
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => anyhow::bail!("plugin initialization failed: {err}"),
            Err(err) => anyhow::bail!("plugin initialization trapped: {err}"),
        }

        Ok((
            name,
            LoadedPlugin {
                component: Arc::new(component),
                config,
            },
        ))
    }

    pub async fn load(&self, wasm_path: &Path) -> Result<String> {
        let (name, loaded) = Self::load_one(
            &self.engine,
            Arc::clone(&self.gateway_ping_ms),
            Arc::clone(&self.application_id),
            Arc::clone(&self.shard_senders),
            Arc::clone(&self.shard_count),
            Arc::clone(&self.songbird),
            self.kv.clone(),
            wasm_path,
        )
        .await?;
        self.plugins.lock().await.insert(name.clone(), loaded);
        Ok(name)
    }

    /// Safely reload a plugin: load the new version first, then shutdown the
    /// old version. If the new version fails to load, the old plugin stays in
    /// service.
    pub async fn reload_plugin(&self, wasm_path: &Path) -> Result<String> {
        let name = Self::plugin_name(wasm_path);
        let (loaded_name, loaded) = Self::load_one(
            &self.engine,
            Arc::clone(&self.gateway_ping_ms),
            Arc::clone(&self.application_id),
            Arc::clone(&self.shard_senders),
            Arc::clone(&self.shard_count),
            Arc::clone(&self.songbird),
            self.kv.clone(),
            wasm_path,
        )
        .await?;

        if loaded_name != name {
            tracing::warn!(
                "Reload path {} produced plugin name {}, expected {}",
                wasm_path.display(),
                loaded_name,
                name
            );
        }

        if self.is_loaded(&name).await {
            self.unload(&name).await;
        }

        self.plugins.lock().await.insert(name.clone(), loaded);
        self.plugin_failures.lock().await.remove(&name);
        Ok(name)
    }

    pub async fn unload(&self, name: &str) {
        let maybe_loaded = {
            let plugins = self.plugins.lock().await;
            plugins
                .get(name)
                .map(|loaded| (Arc::clone(&loaded.component), loaded.config))
        };

        if let Some((component, config)) = maybe_loaded {
            let mut store = Store::new(
                &self.engine,
                HostContext::new(
                    Arc::clone(&self.gateway_ping_ms),
                    Arc::clone(&self.application_id),
                    Arc::clone(&self.shard_senders),
                    Arc::clone(&self.shard_count),
                    Arc::clone(&self.songbird),
                    self.kv.clone(),
                    workspace_path(name),
                    config,
                ),
            );
            if let Err(err) = configure_store(&mut store) {
                tracing::error!("Failed to configure store for {name} shutdown: {err}");
            }
            let linker = match create_linker(&self.engine) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to create linker for {name} shutdown: {e}");
                    self.plugins.lock().await.remove(name);
                    return;
                }
            };

            match plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await {
                Ok(instance) => {
                    if let Err(e) = instance
                        .ynsrvcs_plugins_plugin()
                        .call_shutdown(&mut store)
                        .await
                    {
                        tracing::warn!("Shutdown trap for {name}: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to instantiate {name} for shutdown: {e}");
                }
            }
        }

        self.plugins.lock().await.remove(name);
        info!("Plugin unloaded: {name}");
    }

    pub async fn unload_all(&self) {
        let names: Vec<String> = {
            let plugins = self.plugins.lock().await;
            plugins.keys().cloned().collect()
        };
        for name in names {
            self.unload(&name).await;
        }
    }

    pub fn save_kv(&self) -> Result<()> {
        self.kv.save()
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
        let plugins = {
            let guard = self.plugins.lock().await;
            guard
                .iter()
                .map(|(name, loaded)| (name.clone(), Arc::clone(&loaded.component), loaded.config))
                .collect::<Vec<_>>()
        };

        let failures = Arc::clone(&self.plugin_failures);
        let manager = self.clone();

        for (name, component, config) in plugins {
            let engine = Arc::clone(&self.engine);
            let gateway_ping_ms = Arc::clone(&self.gateway_ping_ms);
            let application_id = Arc::clone(&self.application_id);
            let shard_senders = Arc::clone(&self.shard_senders);
            let shard_count = Arc::clone(&self.shard_count);
            let songbird = Arc::clone(&self.songbird);
            let kv = self.kv.clone();
            let kv_for_save = kv.clone();
            let workspace = workspace_path(&name);
            let event_type = event_type.to_string();
            let payload = payload.clone();
            let failures = Arc::clone(&failures);
            let manager = manager.clone();

            let handle = async move {
                let mut store = Store::new(
                    &engine,
                    HostContext::new(
                        gateway_ping_ms,
                        application_id,
                        shard_senders,
                        shard_count,
                        songbird,
                        kv,
                        workspace,
                        config,
                    ),
                );
                if let Err(err) = configure_store(&mut store) {
                    tracing::error!("Failed to configure store for {name}: {err}");
                    return;
                }
                let linker = match create_linker(&engine) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("Failed to create linker for {name}: {e}");
                        return;
                    }
                };

                let instance =
                    match plugin::PluginWorld::instantiate_async(&mut store, &component, &linker)
                        .await
                    {
                        Ok(i) => i,
                        Err(e) => {
                            tracing::error!("Failed to instantiate {name} for {event_type}: {e}");
                            return;
                        }
                    };

                let guest = instance.ynsrvcs_plugins_plugin();
                let fut = guest.call_handle_event(
                    &mut store,
                    &event_type,
                    &payload,
                    guild_id,
                    channel_id,
                );

                let failed = match tokio::time::timeout(PLUGIN_CALL_TIMEOUT, fut).await {
                    Ok(Ok(Ok(()))) => {
                        failures.lock().await.remove(&name);
                        false
                    }
                    Ok(Ok(Err(err))) => {
                        tracing::error!("Plugin {name} error handling {event_type}: {err}");
                        true
                    }
                    Ok(Err(err)) => {
                        tracing::error!("Plugin {name} trapped handling {event_type}: {err}");
                        true
                    }
                    Err(_) => {
                        tracing::error!("Plugin {name} timed out handling {event_type}");
                        true
                    }
                };

                if failed {
                    let mut map = failures.lock().await;
                    let count = map.entry(name.clone()).or_insert(0);
                    *count += 1;
                    if *count >= PLUGIN_FAILURE_THRESHOLD {
                        tracing::error!("Plugin {name} exceeded failure threshold; unloading");
                        drop(map);
                        manager.unload(&name).await;
                    }
                }

                if let Err(err) = kv_for_save.save() {
                    tracing::error!("Failed to persist KV after {event_type} for {name}: {err}");
                }
            };

            handle.await;
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
        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

        let wasm_path = ensure_ping_wasm()?;
        let engine = crate::wasm::plugin::create_engine()?;
        let (name, _) = PluginManager::load_one(
            &engine,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AsyncMutex::new(Vec::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AsyncMutex::new(None)),
            KvStore::with_path(std::env::temp_dir().join("ynsrvcs-test-kv.json")),
            &wasm_path,
        )
        .await?;
        assert_eq!(name, "ping");

        Ok(())
    }
}
