use std::env;

use anyhow::bail;
use axum::{
    extract::{ws::Message, State, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::any,
    Router,
};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use rusher_core::{ClientEvent, ConnectionInfo, CustomEvent, ServerEvent};
use rusher_pubsub::Broker;
use tokio::{net::TcpListener, sync::mpsc};

#[tokio::main]
async fn main() {
    let port = env::var("PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(4444);

    let broker = Broker::default();

    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();

    let app = Router::new()
        .route("/app", any(handle_ws))
        .with_state(broker);

    axum::serve(listener, app).await.unwrap();
}

async fn handle_ws(State(broker): State<Broker>, ws: WebSocketUpgrade) -> impl IntoResponse {
    match broker.new_connection().await {
        Ok(mut connection) => Ok(ws.on_upgrade(move |ws| async move {
            let socket_id = rand::thread_rng().gen();

            let (mut write_ws, mut read_ws) = ws.split();
            let (tx, mut rx) = mpsc::channel(64);

            let write_messages = async move {
                while let Some(msg) = rx.recv().await {
                    if let Ok(msg) = serde_json::to_string(&msg) {
                        if let Err(err) = write_ws.send(Message::Text(msg)).await {
                            bail!(err)
                        }
                    }
                }
                anyhow::Ok(())
            };

            #[allow(unused)]
            let read_messages = async move {
                tx.send(ServerEvent::ConnectionEstablished {
                    data: ConnectionInfo {
                        socket_id,
                        activity_timeout: 120,
                    },
                })
                .await?;

                loop {
                    tokio::select! {
                        Ok(msg) = connection.recv() => {
                            tx.send(msg).await;
                        },

                        Some(Ok(msg)) = read_ws.next() => {
                            match msg {
                                Message::Text(text) => {
                                    match serde_json::from_str(&text) {
                                        Ok(ClientEvent::Ping) => tx.send(ServerEvent::Pong).await?,
                                        Ok(ClientEvent::Subscribe { channel, .. }) => {
                                            connection.subscribe(channel.as_ref()).await.ok();
                                            tx.send(ServerEvent::SubscriptionSucceeded {
                                                channel,
                                                data: None,
                                            })
                                            .await?;
                                        }
                                        Ok(ClientEvent::Unsubscribe { channel }) => {
                                            connection.unsubscribe(channel.as_ref()).await.ok();
                                        }
                                        Ok(ClientEvent::ChannelEvent {
                                            event,
                                            channel,
                                            data,
                                        }) => {
                                            connection
                                                .publish(
                                                    channel.as_ref(),
                                                    ServerEvent::ChannelEvent(CustomEvent {
                                                        event,
                                                        channel: channel.clone(),
                                                        data,
                                                        user_id: None,
                                                    }),
                                                )
                                                .await
                                                .ok();
                                        }
                                        Err(_) => continue,
                                    };
                                }
                                _ => continue,
                            }
                        }
                    }
                }
                anyhow::Ok(())
            };

            tokio::select! {
                _ = write_messages => eprintln!("Writer finished"),
                _ = read_messages => eprintln!("Reader finished"),
            };

            eprintln!("Client disconnected");
        })),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
