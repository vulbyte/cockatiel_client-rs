# cockatiel_client-rs

Rust client SDK for connecting a module to the Cockatiel engine.

This crate builds **standalone** — it does not depend on the engine's source
tree. The protocol types come from the shared
[`cockatiel_proto`](https://github.com/vulbyte/cockatiel_proto) crate, pinned
by git commit, so the wire format a module is built against never shifts.

## Using it in a module

Pin it by commit (preferred — deterministic) or by branch:

```toml
[dependencies]
cockatiel-client = { git = "https://github.com/vulbyte/cockatiel_client-rs", rev = "<commit-sha>" }
prost = "0.14"
tokio = { version = "1.0", features = ["full"] }
tokio-tungstenite = { version = "0.24", features = ["rustls-tls-webpki-roots"] }
```

No `build.rs`, no vendored `.proto`, no relative paths into the engine folder.

## Example module

```rust
use cockatiel_client::proto::{container::Payload, *};
use cockatiel_client::CockatielClient;

#[tokio::main]
async fn main() {
    let mut client = CockatielClient::connect("config.json")
        .await
        .expect("Fatal: Could not connect to Engine");

    while let Some(container) = client.receive().await {
        match container.payload {
            Some(Payload::MessagePreProcess(msg)) => {
                println!("Received: {:?}", msg);
            }
            _ => {}
        }
    }
}
```

The client handles the engine's auth handshake (PIN → JWT), transparently
answers liveness probes (`AuthVerify`), and provides config/`.env` helpers:

- `CockatielClient::connect("config.json")` — connects using `--ip/--port/--pin`
  CLI overrides, persists them, and returns a JWT-authenticated client.
- `client.receive()` / `client.receive_timeout(ms)` / `client.send(payload)`.
- `client.reconnect()` — re-auths using the stored token.
- `load_env_file(".env")` / `write_env_file(".env", &pairs)` — secret handling.
- `PromptKind` / `Prompt::kind()` — how to interpret an incoming prompt
  (re-exported from `cockatiel_proto`).

## Releasing a change

Bump the git commit that consumers pin. Because every consumer pins a `rev`,
nothing drifts silently: a module only picks up a change when it deliberately
moves to a newer commit.

## License

GPL-2.0