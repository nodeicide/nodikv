use futures_lite::StreamExt;
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets, protocol::Router};
use iroh_gossip::{api::Event, net::Gossip, proto::TopicId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;

fn key_path() -> PathBuf {
    dirs::data_dir().unwrap().join("nodikv").join("key.hex")
}

fn id_path() -> PathBuf {
    dirs::data_dir().unwrap().join("nodikv").join("id.txt")
}

fn load_or_create_secret_key(path: &PathBuf) -> anyhow::Result<SecretKey> {
    if path.exists() {
        
        let hex = std::fs::read_to_string(path)?;
        Ok(SecretKey::from_str(hex.trim())?)
    } else {
        let key = SecretKey::generate();
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(path, hex::encode(key.to_bytes()))?;
        Ok(key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    from: EndpointId,
    text: String,
}

impl Message {
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = key_path();
    let secret_key = load_or_create_secret_key(&path)?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await?;

    
    
    endpoint.online().await;

    let id = endpoint.id();
    println!("> our endpoint id: {id}");

    
    
    let id_file = id_path();
    std::fs::write(&id_file, id.to_string())?;
    println!("> id written to: {}", id_file.display());

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    let topic_id = TopicId::from_bytes([0u8; 32]);

    
    
    
    println!("> waiting for a peer to connect...");
    let (_sender, mut receiver) = gossip.subscribe_and_join(topic_id, vec![]).await?.split();
    println!("> connected!");

    tokio::spawn(async move {
        while let Some(event) = receiver.try_next().await.unwrap() {
            match event {
                Event::Received(msg) => match Message::from_bytes(&msg.content) {
                    Ok(decoded) => println!(
                        "> {} (relayed via {}): {}",
                        decoded.from.fmt_short(),
                        msg.delivered_from.fmt_short(),
                        decoded.text,
                    ),
                    Err(_) => println!(
                        "> {}: <could not decode message>",
                        msg.delivered_from.fmt_short()
                    ),
                },
                Event::NeighborUp(id) => println!("> {} joined our neighborhood", id.fmt_short()),
                Event::NeighborDown(id) => println!("> {} left our neighborhood", id.fmt_short()),
                Event::Lagged => println!("> we lagged and may have missed messages"),
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    println!("> shutting down");

    Ok(())
}
