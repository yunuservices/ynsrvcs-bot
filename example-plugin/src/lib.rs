wit_bindgen::generate!({
    world: "plugin-world",
    path: "wit",
});

use std::cell::RefCell;

use crate::exports::ynsrvcs::plugins::plugin::Guest;
use crate::ynsrvcs::plugins::host::{application_id, http_request};

thread_local! {
    static APP_ID: RefCell<Option<String>> = RefCell::new(None);
}

struct PingPlugin;

fn register_ping_command(app_id: &str) {
    let url = format!(
        "https://discord.com/api/v10/applications/{app_id}/commands"
    );
    let body = serde_json::json!([
        {
            "name": "ping",
            "description": "Ping!",
            "type": 1
        }
    ])
    .to_string();
    let _ = http_request("PUT", &url, &body.into_bytes());
}

fn ensure_command_registered() {
    let id = application_id().or_else(|| APP_ID.with(|a| a.borrow().clone()));
    if let Some(id) = id {
        register_ping_command(&id);
        APP_ID.with(|a| *a.borrow_mut() = Some(id));
    }
}

fn handle_interaction(event: &serde_json::Value) {
    let Some(name) = event
        .get("data")
        .and_then(|d| d.get("name"))
        .and_then(|v| v.as_str())
    else {
        return;
    };
    if name != "ping" {
        return;
    }
    let Some(id) = event.get("id").and_then(|v| v.as_str()) else {
        return;
    };
    let Some(token) = event.get("token").and_then(|v| v.as_str()) else {
        return;
    };

    let body = serde_json::json!({
        "type": 4,
        "data": { "content": "Pong!" }
    })
    .to_string();
    let url = format!(
        "https://discord.com/api/v10/interactions/{id}/{token}/callback"
    );
    let _ = http_request("POST", &url, &body.into_bytes());
}

impl Guest for PingPlugin {
    fn initialize(_settings: Option<String>) -> Result<(), String> {
        ensure_command_registered();
        Ok(())
    }

    fn handle_bus_event(_topic: String, _payload: Vec<u8>) -> Result<(), String> {
        Ok(())
    }

    fn handle_event(
        event_type: String,
        payload: Vec<u8>,
        _guild_id: u64,
        _channel_id: u64,
    ) -> Result<(), String> {
        let Ok(event) = serde_json::from_slice::<serde_json::Value>(&payload) else {
            return Ok(());
        };

        match event_type.as_str() {
            "READY" => {
                if let Some(id) = event
                    .get("application")
                    .and_then(|a| a.get("id"))
                    .and_then(|v| v.as_str())
                {
                    APP_ID.with(|a| *a.borrow_mut() = Some(id.to_string()));
                    register_ping_command(id);
                }
            }
            "INTERACTION_CREATE" => handle_interaction(&event),
            _ => {}
        }

        Ok(())
    }

    fn shutdown() -> Result<(), String> {
        Ok(())
    }
}

export!(PingPlugin);
