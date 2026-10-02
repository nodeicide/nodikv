use anyhow::Result;
use futures_lite::StreamExt;
use iroh::{Endpoint, EndpointId, PublicKey, endpoint::presets, protocol::Router};
use iroh_gossip::{
    api::{Event, GossipSender},
    net::Gossip,
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    thread,
    time::Duration,
};
use tokio::{fs, io::AsyncWriteExt, runtime::Handle};

const FILE: &str = "node_ids.txt";

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

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    println!("> our endpoint id: {}", endpoint.id());

    let mut file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(FILE)
        .await?;

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
        println!("> no peer given, checking known ids in {FILE}...");
        if let Some(found) = fetch(&endpoint).await {
            println!("> found a live peer: {}", found.fmt_short());
            peer_ids.push(found);
        } else {
            println!("> no reachable peers in {FILE}");
        }
    }

    let id_log = format!("{}\n", endpoint.id());
    file.write_all(id_log.as_bytes()).await?;

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

async fn fetch(endpoint: &Endpoint) -> Option<EndpointId> {
    let abs_path = std::env::current_dir()
        .map(|d| d.join(FILE).display().to_string())
        .unwrap_or_else(|_| FILE.to_string());

    let content = match fs::read_to_string(FILE).await {
        Ok(c) => c,
        Err(e) => {
            println!("> couldn't read {abs_path}: {e}");
            return None;
        }
    };
    println!("> reading known ids from {abs_path}");

    let my_id = endpoint.id();
    let mut tried = 0;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let peer_id: EndpointId = match line.parse() {
            Ok(id) => id,
            Err(e) => {
                println!("> skipping bad id line {line:?}: {e}");
                continue;
            }
        };

        if peer_id == my_id {
            continue;
        }

        tried += 1;
        print!("> trying {}... ", peer_id.fmt_short());
        io::stdout().flush().ok();

        let alpn = iroh_gossip::ALPN;
        match tokio::time::timeout(Duration::from_secs(10), endpoint.connect(peer_id, alpn)).await {
            Ok(Ok(_conn)) => {
                println!("reachable!");
                return Some(peer_id);
            }
            Ok(Err(e)) => println!("failed: {e}"),
            Err(_) => println!("timed out"),
        }
    }

    println!("> tried {tried} known id(s), none reachable");
    None
}
