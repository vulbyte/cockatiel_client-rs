use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::{
    connect_async, connect_async_tls_with_config, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};
use uuid::Uuid;

// The protocol types are compiled from the shared cockatiel_proto crate — the
// single source of truth for the wire format. Consumers keep using
// `cockatiel_client::proto::*` and `cockatiel_client::PromptKind`.
pub use cockatiel_proto::proto;
pub use cockatiel_proto::PromptKind;

use proto::container::Payload;
use proto::*;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ── Config ───────────────────────────────────────────────────────────

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CockatielConfig {
    #[serde(default = "default_ip")]
    pub ip: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "is_zero_pin")]
    pub pin: i32,
    pub module_name: String,
    #[serde(default = "default_position")]
    pub position: i32,
    #[serde(default = "default_priority")]
    pub priority: u32,
}

fn default_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    9734
}
fn default_position() -> i32 {
    1
}
fn default_priority() -> u32 {
    100
}

fn is_zero_pin(pin: &i32) -> bool {
    *pin == 0
}

impl CockatielConfig {
    pub fn load_or_create<P: AsRef<Path>>(path: P) -> Self {
        if let Ok(file_content) = fs::read_to_string(&path) {
            if let Ok(config) = serde_json::from_str(&file_content) {
                return config;
            }
            // The file exists but isn't a bare connection config — it may be a
            // combined file (connection settings + `module_specific`). Don't
            // clobber it; fill in the default connection fields only.
            if let Ok(mut root) = serde_json::from_str::<serde_json::Value>(&file_content) {
                if let Some(obj) = root.as_object_mut() {
                    obj.entry("ip".to_string())
                        .or_insert_with(|| serde_json::json!(default_ip()));
                    obj.entry("port".to_string())
                        .or_insert_with(|| serde_json::json!(default_port()));
                    obj.entry("module_name".to_string())
                        .or_insert_with(|| serde_json::json!("unnamed_module"));
                    obj.entry("position".to_string())
                        .or_insert_with(|| serde_json::json!(default_position()));
                    obj.entry("priority".to_string())
                        .or_insert_with(|| serde_json::json!(default_priority()));
                }
                let _ = fs::write(&path, serde_json::to_string_pretty(&root).unwrap());
                if let Ok(config) = serde_json::from_str::<Self>(&serde_json::to_string(&root).unwrap()) {
                    return config;
                }
            }
        }

        let default_config = Self {
            ip: default_ip(),
            port: default_port(),
            pin: 0,
            module_name: "unnamed_module".to_string(),
            position: default_position(),
            priority: default_priority(),
        };

        let _ = fs::write(path, serde_json::to_string_pretty(&default_config).unwrap());
        default_config
    }
}

// ── Client ───────────────────────────────────────────────────────────

/// Load a KEY=VALUE `.env` file into the process environment so the module can
/// read its secrets via `std::env::var`. Real environment variables win — this
/// only fills in values that weren't already set. Values that parse as JSON
/// arrays are joined with "\n" (so list credentials load as one multi-line env
/// var, matching how the engine presents them).
pub fn load_env_file(path: impl AsRef<std::path::Path>) {
    let Ok(content) = std::fs::read_to_string(path.as_ref()) else {
        return;
    };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_string();
            let mut value = value.trim().trim_matches('"').to_string();
            if key.is_empty() {
                continue;
            }
            if let Ok(items) = serde_json::from_str::<Vec<String>>(&value) {
                value = items.join("\n");
            }
            if std::env::var(&key).is_err() {
                // Startup-only; values come from a file the module owns.
                unsafe {
                    std::env::set_var(key, value);
                }
            }
        }
    }
}

/// Merge key=value pairs into a `.env` file (creating it if missing), with
/// owner-only permissions since the file holds secrets.
pub fn write_env_file(path: impl AsRef<std::path::Path>, pairs: &[(&str, &str)]) {
    let path = path.as_ref();
    let mut lines: Vec<String> = std::fs::read_to_string(path)
        .map(|c| c.lines().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    for (key, value) in pairs {
        let entry = format!("{}={}", key, value);
        let prefix = format!("{}=", key);
        if let Some(idx) = lines.iter().position(|l| l.trim().starts_with(&prefix)) {
            lines[idx] = entry;
        } else {
            lines.push(entry);
        }
    }
    let mut content = lines.join("\n");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    if std::fs::write(path, content).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

pub struct CockatielClient {
    pub stream: WsStream,
    pub config: CockatielConfig,
    pub auth_token: String,
    pub instance_uuid7: String,
}

impl CockatielClient {
    /// Full two-phase handshake:
    /// 1. Connect WebSocket
    /// 2. Send ConnectionRequest with PIN
    /// 3. Wait for ConnectionRequestReturn (auth_token)
    /// 4. Sever connection
    /// 5. Reconnect with auth_token
    pub async fn connect(config_path: impl Into<String>) -> Result<Self, String> {
        let config_path = config_path.into();
        let mut config = CockatielConfig::load_or_create(&config_path);
        let mut changed = false;

        // PIN delivery: an injected env var (set by the supervisor) wins over
        // the config file, so the PIN never has to travel on the command line
        // (visible in `ps`). `--pin` on argv remains a manual-run override.
        // The env PIN is never persisted back into config.json — the PIN does
        // not live in config files (they're gitignored, but a stale pin in one
        // must not shadow the injected value either).
        if let Ok(pin_env) = std::env::var("COCKATIEL_PIN") {
            if let Ok(pin) = pin_env.parse() {
                if config.pin != pin {
                    config.pin = pin;
                }
            }
        }

        // Apply CLI overrides: --ip, --port, --pin, --name
        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            if (args[i] == "--ip" || args[i] == "-i") && i + 1 < args.len() {
                config.ip = args[i + 1].clone();
                changed = true;
                i += 2;
            } else if (args[i] == "--port" || args[i] == "-p") && i + 1 < args.len() {
                if let Ok(port) = args[i + 1].parse() {
                    config.port = port;
                    changed = true;
                }
                i += 2;
            } else if (args[i] == "--pin") && i + 1 < args.len() {
                if let Ok(pin) = args[i + 1].parse() {
                    config.pin = pin;
                    changed = true;
                }
                i += 2;
            } else if (args[i] == "--name" || args[i] == "-n") && i + 1 < args.len() {
                let name = args[i + 1].clone();
                if !name.trim().is_empty() {
                    config.module_name = name.trim().to_string();
                    changed = true;
                }
                i += 2;
            } else {
                i += 1;
            }
        }

        // Persist CLI overrides back to the config file. Merge into any
        // existing JSON so a single config.json can hold BOTH the connection
        // settings (ip/port/pin/module_name) and the module's own settings
        // (e.g. `module_specific`) without clobbering them.
        if changed {
            let mut root: serde_json::Value = std::fs::read_to_string(&config_path)
                .ok()
                .and_then(|data| serde_json::from_str(&data).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            if root.is_object() {
                // Never drop module-specific settings even if the read above
                // failed (e.g. transient lock): re-read and preserve them.
                let preserved_ms = if root.get("module_specific").is_none() {
                    std::fs::read_to_string(&config_path)
                        .ok()
                        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
                        .and_then(|full| full.get("module_specific").cloned())
                } else {
                    None
                };
                let obj = root.as_object_mut().unwrap();
                obj.insert("ip".to_string(), serde_json::json!(config.ip));
                obj.insert("port".to_string(), serde_json::json!(config.port));
                obj.insert("module_name".to_string(), serde_json::json!(config.module_name));
                obj.insert("position".to_string(), serde_json::json!(config.position));
                obj.insert("priority".to_string(), serde_json::json!(config.priority));
                if let Some(ms) = preserved_ms {
                    obj.insert("module_specific".to_string(), ms);
                }
                if let Ok(pretty) = serde_json::to_string_pretty(&root) {
                    let _ = fs::write(&config_path, pretty);
                }
            }
        }

        let ws_url = format!("ws://{}:{}", config.ip, config.port);

        // ── Phase 1: Connect + send ConnectionRequest ──────────────────
        let mut ws = Self::connect_ws(&ws_url).await?;

        let connection_request = Container {
            version: 1,
            auth_token: String::new(),
            module_name: config.module_name.clone(),
            module_instance_uuid7: Uuid::now_v7().to_string(),
            payload: Some(Payload::ConnectionRequest(ConnectionRequest {
                pin: config.pin,
                process_position: config.position,
                priority: config.priority,
                module_instance_uuid7: String::new(), // engine will assign
            })),
        };

        Self::send_raw(&mut ws, &connection_request).await?;

        // Wait for ConnectionRequestReturn
        let response = Self::receive_raw(&mut ws, 15000).await?;

        let (auth_token, assigned_uuid) = match response.payload {
            Some(Payload::ConnectionRequestReturn(ret)) => {
                if ret.new_port == 0 && !response.auth_token.is_empty() {
                    (response.auth_token, ret.module_instance_uuid7)
                } else if ret.new_port == 0 && response.auth_token.is_empty() {
                    return Err("Connection rejected: invalid PIN or module not whitelisted. Pass --pin <PIN> or set pin in your config.json".into());
                } else {
                    return Err(format!(
                        "Connection rejected: new_port={}, auth_token_empty={}",
                        ret.new_port,
                        response.auth_token.is_empty()
                    ));
                }
            }
            other => {
                return Err(format!(
                    "Expected ConnectionRequestReturn, got: {:?}",
                    other
                ));
            }
        };

        println!(
            "[{}] Authenticated as {} [{}]",
            config.module_name, config.module_name, assigned_uuid
        );

        Ok(Self {
            stream: ws,
            config,
            auth_token,
            instance_uuid7: assigned_uuid,
        })
    }

    /// Send a payload to the engine. Automatically includes auth_token and instance_uuid7.
    pub async fn send(&mut self, payload: Payload) -> Result<(), String> {
        let container = Container {
            version: 1,
            auth_token: self.auth_token.clone(),
            module_name: self.config.module_name.clone(),
            module_instance_uuid7: self.instance_uuid7.clone(),
            payload: Some(payload),
        };

        Self::send_raw(&mut self.stream, &container).await
    }

    /// Receive the next Container from the engine.
    pub async fn receive(&mut self) -> Option<Container> {
        match Self::receive_raw(&mut self.stream, u64::MAX).await {
            Ok(container) => self.maybe_answer_probe(container).await,
            Err(_) => None,
        }
    }

    /// Receive with a timeout in milliseconds.
    pub async fn receive_timeout(&mut self, timeout_ms: u64) -> Result<Container, String> {
        let container = Self::receive_raw(&mut self.stream, timeout_ms).await?;
        match self.maybe_answer_probe(container).await {
            Some(c) => Ok(c),
            None => Err("connection closed while answering a liveness probe".to_string()),
        }
    }

    /// Transparently answer the engine's liveness probes: an incoming
    /// `AuthVerify` is a request to prove we're alive, so reply with our
    /// current auth token (which the engine verifies) and keep reading.
    async fn maybe_answer_probe(&mut self, container: Container) -> Option<Container> {
        if matches!(container.payload, Some(Payload::AuthVerify(_))) {
            let reply = Container {
                version: 1,
                auth_token: self.auth_token.clone(),
                module_name: self.config.module_name.clone(),
                module_instance_uuid7: self.instance_uuid7.clone(),
                payload: Some(Payload::AuthVerify(AuthVerify {
                    cur_auth: self.auth_token.clone(),
                })),
            };
            let _ = Self::send_raw(&mut self.stream, &reply).await;
            // Keep reading — the probe was an interleaved control message.
            Self::receive_raw(&mut self.stream, u64::MAX).await.ok()
        } else {
            Some(container)
        }
    }

    /// Reconnect to the engine using stored auth credentials.
    pub async fn reconnect(&mut self) -> Result<(), String> {
        let ws_url = format!("ws://{}:{}", self.config.ip, self.config.port);

        let _ = self.stream.close(None).await;
        let _ = &mut self.stream;

        sleep(Duration::from_millis(100)).await;

        let mut ws = Self::connect_ws(&ws_url).await?;

        let reauth = Container {
            version: 1,
            auth_token: self.auth_token.clone(),
            module_name: self.config.module_name.clone(),
            module_instance_uuid7: self.instance_uuid7.clone(),
            // The engine gates the first frame on ANY fresh socket to a
            // ConnectionRequest, so the reauth carries one; the JWT in
            // auth_token is what re-authenticates (pin is ignored).
            payload: Some(Payload::ConnectionRequest(ConnectionRequest {
                pin: 0,
                process_position: self.config.position,
                priority: self.config.priority,
                module_instance_uuid7: self.instance_uuid7.clone(),
            })),
        };

        Self::send_raw(&mut ws, &reauth).await?;

        self.stream = ws;
        Ok(())
    }

    // ── Internal helpers ───────────────────────────────────────────────

    async fn connect_ws(url: &str) -> Result<WsStream, String> {
    // WSS is chosen when the supervisor points us at the engine's self-signed
    // certificate via COCKATIEL_TLS_CERT. That cert is PINNED as the trust
    // root — a self-signed engine cert would never pass webpki-roots, and we
    // deliberately don't accept arbitrary certs.
    let (scheme, connector): (&str, Option<tokio_tungstenite::Connector>) =
        match std::env::var("COCKATIEL_TLS_CERT") {
            Ok(path) if !path.trim().is_empty() => {
                let cfg = Self::pinned_tls_config(&path)?;
                ("wss", Some(tokio_tungstenite::Connector::Rustls(Arc::new(cfg))))
            }
            _ => ("ws", None),
        };
    let hostport = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
        .unwrap_or(url);
    let url = format!("{}://{}", scheme, hostport);

    let max_attempts = 60;
    let mut attempt = 0;

    loop {
        attempt += 1;
        let result = match &connector {
            Some(c) => {
                connect_async_tls_with_config(&url, None, false, Some(c.clone())).await
            }
            None => connect_async(&url).await,
        };
        match result {
            Ok((ws_stream, _)) => return Ok(ws_stream),
            Err(e) => {
                if attempt >= max_attempts {
                    return Err(format!("Failed to connect after {} attempts: {}", max_attempts, e));
                }
                eprintln!(
                    "[cockatiel] Connection failed (attempt {}/{}): {}. Retrying in 5s...",
                    attempt, max_attempts, e
                );
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Build a rustls client config that trusts exactly the engine's self-signed
/// certificate (cert pinning). Any other chain is rejected.
fn pinned_tls_config(cert_pem_path: &str) -> Result<rustls::ClientConfig, String> {
    let cert_bytes = std::fs::read(cert_pem_path).map_err(|e| format!("read TLS cert {}: {}", cert_pem_path, e))?;
    let mut reader = std::io::BufReader::new(cert_bytes.as_slice());
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parse TLS cert: {}", e))?;
    if certs.is_empty() {
        return Err(format!("no certificate found in {}", cert_pem_path));
    }
    let mut roots = rustls::RootCertStore::empty();
    for c in certs {
        roots
            .add(c)
            .map_err(|e| format!("pinning TLS cert failed: {}", e))?;
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

    async fn send_raw(ws: &mut WsStream, container: &Container) -> Result<(), String> {
        let mut buf = Vec::new();
        container
            .encode(&mut buf)
            .map_err(|e| format!("Encode error: {}", e))?;
        ws.send(Message::Binary(buf.into()))
            .await
            .map_err(|e| format!("Send error: {}", e))
    }

    async fn receive_raw(ws: &mut WsStream, timeout_ms: u64) -> Result<Container, String> {
        let result = tokio::time::timeout(Duration::from_millis(timeout_ms), ws.next()).await;
        match result {
            Ok(Some(Ok(Message::Binary(data)))) => {
                Container::decode(data.as_ref()).map_err(|e| format!("Decode error: {}", e))
            }
            Ok(Some(Ok(Message::Close(_)))) => Err("Connection closed by server".into()),
            Ok(Some(Ok(_))) => Err("Received non-binary message".into()),
            Ok(Some(Err(e))) => Err(format!("WebSocket error: {}", e)),
            Ok(None) => Err("Stream ended".into()),
            Err(_) => Err("Timeout".into()),
        }
    }
}
