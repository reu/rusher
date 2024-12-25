use std::env;

use futures::{SinkExt, StreamExt};
use http::Uri;
use rusher_core::{ChannelName, ClientEvent};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() {
    let subscribers_count = env::var("SUB")
        .ok()
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(1);

    let publishers_count = env::var("PUB")
        .ok()
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(1);

    let url = env::var("URL")
        .ok()
        .and_then(|url| url.parse::<Uri>().ok())
        .unwrap_or(Uri::from_static("ws://localhost:4444/app/test"));

    let channel = "benchmark".parse::<ChannelName>().unwrap();

    let (recv_tx, mut revc_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut count = 0;
        let start = std::time::Instant::now();
        while let Some(()) = revc_rx.recv().await {
            count += 1;

            if count % 10_000 == 0 {
                let msg_per_sec = if start.elapsed().as_secs() > 0 {
                    count / start.elapsed().as_secs()
                } else {
                    0
                };
                print!("\rReceived: {count} total msgs - {msg_per_sec} msgs/sec         ");
            }
        }
    });

    let subs = tokio::spawn({
        let url = url.clone();
        let channel = channel.clone();
        async move {
            let mut handles = JoinSet::new();
            for _ in 0..subscribers_count {
                let recv_tx = recv_tx.clone();
                let channel = channel.clone();
                let url = url.clone();

                handles.spawn(async move {
                    let (ws, _) = connect_async(&url).await.expect("Failed to connect");
                    let (mut writer, mut reader) = ws.split();

                    let subscribe = ClientEvent::Subscribe {
                        channel: channel.clone(),
                        auth: None,
                        channel_data: None,
                    };
                    let subscribe = serde_json::to_string(&subscribe).unwrap();

                    writer.send(Message::text(subscribe)).await.unwrap();

                    while let Some(Ok(_msg)) = reader.next().await {
                        recv_tx.send(()).ok();
                    }
                });
            }
            while let Some(Ok(_)) = handles.join_next().await {}
        }
    });

    let pubs = tokio::spawn({
        let url = url.clone();
        let channel = channel.clone();
        async move {
            let mut handles = JoinSet::new();
            for _ in 0..publishers_count {
                let channel = channel.clone();
                let url = url.clone();

                handles.spawn(async move {
                    let (ws, _) = connect_async(&url).await.expect("Failed to connect");
                    let (mut writer, _) = ws.split();

                    let event = ClientEvent::ChannelEvent {
                        channel,
                        event: "lol".into(),
                        data: "wut".into(),
                    };
                    let event = serde_json::to_string(&event).unwrap();
                    loop {
                        writer.send(Message::text(&event)).await.unwrap();
                    }
                });
            }
            while let Some(Ok(_)) = handles.join_next().await {}
        }
    });

    tokio::select! {
        _ = subs => eprintln!("Reader finished"),
        _ = pubs => eprintln!("Writer finished"),
    }
}
