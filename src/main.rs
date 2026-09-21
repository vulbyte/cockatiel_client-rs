use cockatiel_client::proto::container::Payload;
use cockatiel_client::CockatielClient;

#[tokio::main]
async fn main() {
    let mut client = CockatielClient::connect("cockatiel-config.json")
        .await
        .expect("Fatal: Could not connect to Engine");

    println!(
        "Module active: [{}] on position {}",
        client.config.module_name, client.config.position
    );

    while let Some(container) = client.receive().await {
        match container.payload {
            Some(Payload::MessagePreProcess(msg)) => {
                println!("Received message payload: {:?}", msg);
            }
            _ => {}
        }
    }
}
