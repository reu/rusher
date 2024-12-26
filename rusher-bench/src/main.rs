use std::{
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
    usize,
};

use futures::{SinkExt, StreamExt};
use http::Uri;
use rusher_core::{ChannelName, ClientEvent};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone)]
enum Event {
    Connect,
    Disconnect,
    Message,
}

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

    let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
    let (recv_tx, mut revc_rx) = mpsc::unbounded_channel();

    let current_pubs = Arc::new(AtomicUsize::new(0));
    let current_subs = Arc::new(AtomicUsize::new(0));
    let sent_count = Arc::new(AtomicUsize::new(0));
    let recv_count = Arc::new(AtomicUsize::new(0));

    tokio::spawn({
        let current_subs = current_subs.clone();
        let recv_count = recv_count.clone();
        async move {
            while let Some((event, _timestamp)) = revc_rx.recv().await {
                match event {
                    Event::Connect => current_subs.fetch_add(1, Ordering::Relaxed),
                    Event::Disconnect => current_subs.fetch_sub(1, Ordering::Relaxed),
                    Event::Message => recv_count.fetch_add(1, Ordering::Relaxed),
                };
            }
        }
    });

    tokio::spawn({
        let current_pubs = current_pubs.clone();
        let sent_count = sent_count.clone();
        async move {
            while let Some((event, _timestamp)) = sent_rx.recv().await {
                match event {
                    Event::Connect => current_pubs.fetch_add(1, Ordering::Relaxed),
                    Event::Disconnect => current_pubs.fetch_sub(1, Ordering::Relaxed),
                    Event::Message => sent_count.fetch_add(1, Ordering::Relaxed),
                };
            }
        }
    });

    tokio::spawn(async move {
        let timer = Instant::now();
        loop {
            let subs = current_subs.load(Ordering::Relaxed);
            let pubs = current_pubs.load(Ordering::Relaxed);
            let sent = sent_count.load(Ordering::Relaxed);
            let recv = recv_count.load(Ordering::Relaxed);
            let lag = if subs > 0 {
                sent - recv / subs
            } else {
                sent - recv
            };
            let elapsed = timer.elapsed().as_secs() as usize;
            let sent_per_sec = if elapsed > 0 { sent / elapsed } else { 0 };
            let recv_per_sec = if elapsed > 0 { recv / elapsed } else { 0 };
            println!("Subs: {subs} Pubs: {pubs} Sent: {sent} Received: {recv} Lag: {lag} Sent/sec: {sent_per_sec} Received/sec: {recv_per_sec}");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let subs = tokio::spawn({
        let url = url.clone();
        let channel = channel.clone();
        let recv_tx = recv_tx.clone();
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
                    recv_tx.send((Event::Connect, Instant::now())).ok();

                    while let Some(Ok(_msg)) = reader.next().await {
                        recv_tx.send((Event::Message, Instant::now())).ok();
                    }
                });
            }

            loop {
                match handles.join_next().await {
                    Some(_) => {
                        recv_tx.send((Event::Disconnect, Instant::now())).ok();
                    }
                    _ => break,
                }
            }
        }
    });

    let pubs = tokio::spawn({
        let url = url.clone();
        let channel = channel.clone();
        let sent_tx = sent_tx.clone();
        async move {
            let mut handles = JoinSet::new();
            for _ in 0..publishers_count {
                let channel = channel.clone();
                let url = url.clone();
                let sent_tx = sent_tx.clone();

                handles.spawn(async move {
                    let (ws, _) = connect_async(&url).await.expect("Failed to connect");
                    let (mut writer, _) = ws.split();
                    sent_tx.send((Event::Connect, Instant::now())).ok();

                    let event = ClientEvent::ChannelEvent {
                        channel,
                        event: "lol".into(),
                        data: "wut".into(),
                    };
                    let event = serde_json::to_string(&event).unwrap();
                    loop {
                        writer.send(Message::text(&event)).await.unwrap();
                        sent_tx.send((Event::Message, Instant::now())).ok();
                    }
                });
            }
            loop {
                match handles.join_next().await {
                    Some(_) => {
                        sent_tx.send((Event::Disconnect, Instant::now())).ok();
                    }
                    _ => break,
                }
            }
        }
    });

    tokio::select! {
        _ = subs => eprintln!("Reader finished"),
        _ = pubs => eprintln!("Writer finished"),
    }
}
