use crate::State::{Candidate, Follower, Leader};
use anyhow::Result;
use futures_lite::StreamExt;
use iroh::{endpoint::presets, protocol::Router, Endpoint, EndpointId, PublicKey};
use iroh_gossip::{
    api::{Event, GossipSender},
    net::Gossip,
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::{Arc, Mutex},
};
use tokio::{
    runtime::Handle,
    sync::mpsc,
    time::{sleep, Duration},
};

type Db = Arc<Mutex<HashMap<String, String>>>;
const BOOSTSTRAP_ID: &str = "2931d32cea5a69f2c0065e7436fc0ff10c219ce98c2b43d8b74f411714270717";

//to apply what other nodes say to do to yourself
fn apply_command(line: &str, db: &Db) -> String {
    let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
    match parts.as_slice() {
        ["PUSH", key, value] => {
            db.lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            "OK".to_string()
        }
        ["PULL", key] => match db.lock().unwrap().get(*key) {
            Some(v) => v.clone(),
            None => "NOT FOUND".to_string(),
        },
        _ => "ERR unknown command".to_string(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    from: EndpointId,
    text: String,
}

#[derive(PartialEq)]
enum State {
    Follower,
    Candidate,
    Leader,
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
    let mut current_state: State = Follower;
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

    //the bootstrap node joining to connect to other peers
    peer_ids.push(BOOSTSTRAP_ID.parse().expect("not a valid endpoint id"));

    if peer_ids.is_empty() {
        println!("> waiting for a peer to connect...");
    } else {
        println!("> connecting to {} known peer(s)...", peer_ids.len());
    }

    let (sender, mut receiver) = gossip.subscribe_and_join(topic_id, peer_ids).await?.split();
    println!("> connected!");

    let my_id: PublicKey = endpoint.id();

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    let handle = Handle::current();
    let command_db = db.clone();
    let value = sender.clone();
    std::thread::spawn(move || handle.block_on(command_loop(my_id, value, command_db)));

    let receive_db = db.clone();
    tokio::spawn(async move {
        while let Some(event) = receiver.try_next().await.unwrap() {
            match event {
                Event::Received(msg) => match Message::from_bytes(&msg.content) {
                    Ok(decoded) => {
                        if decoded.text == "VOTE_REQUEST" {
                            println!("> Received vote request from {}", decoded.from.fmt_short());

                            let _ = send_message(&sender, my_id, "VOTE_GRANT").await;
                        } else if decoded.text == "VOTE_GRANT" {
                            println!("> Received vote grant from {}", decoded.from.fmt_short());
                        }
                        let response = apply_command(&decoded.text, &receive_db);
                        println!(
                            "> {} (relayed via {}): {} -> {}",
                            decoded.from.fmt_short(),
                            msg.delivered_from.fmt_short(),
                            decoded.text,
                            response,
                        );
                    }
                    Err(_) => println!(
                        "> {}: <could not decode message>",
                        msg.delivered_from.fmt_short()
                    ),
                },

                //to know how many members of your group you have
                Event::NeighborUp(id) => println!("> {} joined our neighborhood", id.fmt_short()),
                Event::NeighborDown(id) => println!("> {} left our neighborhood", id.fmt_short()),
                Event::Lagged => println!("> we lagged and may have missed messages"),
            }
        }
    });

    tokio::spawn(async move {
        let (beat_tx, beat_rx) = mpsc::channel(10);

        tokio::spawn(async move {
            if let Err(e) = pulse(beat_rx, &mut current_state).await {
                eprintln!("pulse error: {e}");
            }
        });
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

async fn command_loop(id: PublicKey, sender: GossipSender, db: Db) -> Result<()> {
    loop {
        print!("Command -> ");
        io::stdout().flush()?;
        let line = match io::stdin().lock().lines().next() {
            Some(line) => line?,
            None => break,
        };

        if !line.contains("PUSH") && !line.contains("PULL") {
            println!("invalid command, must be PUSH or PULL");
            continue;
        }

        //ensure you execute it youself first
        let response = apply_command(&line, &db);
        println!("{response}");

        //send what you do elsewhere
        if line.trim().starts_with("PUSH ") {
            send_message(&sender, id, &line).await?;
        }
    }
    Ok(())
}

async fn heart(id: PublicKey, sender: GossipSender, current: &mut State) -> Result<()> {
    loop {
        if *current == Leader {
            let _ = send_message(&sender, id, "BEAT").await?;
            sleep(Duration::from_millis(500)).await;
        }
    }
}

async fn pulse(mut beat_rx: mpsc::Receiver<()>, current: &mut State) -> Result<()> {
    loop {
        if *current == Follower {
            tokio::select! {
                    Some(_) = beat_rx.recv() => {
                        println!("beat recieved");
                    }
                    _ = sleep(Duration::from_secs(1)) => {
                        println!("no beat");
                        *current = Candidate;
                    }
            }
        }
    }
}

async fn voter(
    id: PublicKey,
    sender: GossipSender,
    current: &mut State,
    total_peers: usize,
    mut vote_rx: mpsc::Receiver<()>,
) -> Result<()> {
    let total_nodes = total_peers + 1;
    let majority = (total_nodes / 2) + 1;

    println!("> Starting election. Need {majority} votes out of {total_nodes} nodes.");

    let mut votes_received = 1;
    let _ = send_message(&sender, id, "VOTE_REQUEST").await?;

    let election_timeout = Duration::from_millis(750);

    loop {
        tokio::select! {
            Some(_) = vote_rx.recv() => {
                votes_received += 1;
                println!("> Vote received! Total: {votes_received}/{majority}");

                if votes_received >= majority {
                    println!("> Won election! Transitioning to Leader.");
                    *current = Leader;
                    break;
                }
            }
            _ = sleep(election_timeout) => {
                println!("> Election timed out. Reverting to Follower.");
                *current = Follower;
                break;
            }
        }
    }

    Ok(())
}
