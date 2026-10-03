use anyhow::Result;
use futures_lite::StreamExt;
use iroh::{Endpoint, EndpointId, PublicKey, SecretKey, endpoint::presets, protocol::Router};
use iroh_gossip::{
    api::{Event, GossipSender},
    net::Gossip,
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    thread,
};
use tokio::runtime::Handle;

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    from: EndpointId,
    text: String,
}

impl Message {
    fn to_vec(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

async fn bootstrap_id() -> Option<EndpointId> {
    let path = dirs::data_dir()?.join("nodikv").join("key.hex");
    let hex = tokio::fs::read_to_string(&path).await.ok()?;
    let secret_key: SecretKey = hex.trim().parse().ok()?;
    Some(secret_key.public())
}

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    println!("> our endpoint id: {}", endpoint.id());

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    let topic_id = TopicId::from_bytes([0u8; 32]);

    let mut peer_ids: Vec<EndpointId> = std::env::args()
        .skip(1)
        .map(|s| s.parse().expect("not a valid endpoint id"))
        .collect();

    if peer_ids.is_empty() {
        println!("> no peer given, checking for a local bootstrap node...");
        match bootstrap_id().await {
            Some(id) => {
                println!("> using bootstrap id: {}", id.fmt_short());
                peer_ids.push(id);
            }
            None => {
                println!("run bootstrap");
            }
        }
    }

    if peer_ids.is_empty() {
        println!("> waiting for a peer to connect...");
    } else {
        println!("> connecting to {} known peer(s)...", peer_ids.len());
    }

    let (sender, mut receiver) = gossip.subscribe_and_join(topic_id, peer_ids).await?.split();
    println!("> connected!");

    let my_id: PublicKey = endpoint.id();

    let handle = Handle::current();
    thread::spawn(move || handle.block_on(message_loop(my_id, sender)));

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

    Ok(())
}

async fn send_message(sender: &GossipSender, id: PublicKey, text: &str) -> Result<()> {
    let message = Message {
        from: id,
        text: text.to_string(),
    };
    sender.broadcast(message.to_vec().into()).await?;
    Ok(())
}

async fn message_loop(id: PublicKey, sender: GossipSender) -> Result<()> {
    loop {
        print!("Message -> ");
        io::stdout().flush()?;
        let line = match io::stdin().lines().next() {
            Some(line) => line?,
            None => break,
        };
        send_message(&sender, id, &line).await?;
    }
    Ok(())
}
