use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
};

use anyhow::bail;
use axum::{
    extract::{ws::Message, Path, Request, State, WebSocketUpgrade},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Extension, Json, Router,
};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use rusher_core::{ChannelName, ClientEvent, ConnectionInfo, CustomEvent, ServerEvent, SocketId};
use rusher_pubsub::Broker;
use serde::Deserialize;
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::{mpsc, Mutex},
};

#[derive(Debug, Clone)]
struct AppId(String);

#[tokio::main]
async fn main() {
    let port = env::var("PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(4444);

    let brokers = Arc::new(Mutex::new(HashMap::default()));

    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();

    let app = Router::new()
        .route("/app/:app/channels", get(list_channels))
        .route("/app/:app", post(publish))
        .route("/app/:app", any(handle_ws))
        .route_layer(middleware::from_fn_with_state(
            brokers.clone(),
            broker_middleware,
        ));

    axum::serve(listener, app).await.unwrap();
}

async fn broker_middleware(
    State(brokers): State<Arc<Mutex<HashMap<String, Broker>>>>,
    Path(app): Path<String>,
    mut request: Request,
    next: Next,
) -> Response {
    let mut brokers = brokers.lock().await;
    let broker = brokers.entry(app.clone()).or_default();
    request.extensions_mut().insert(AppId(app));
    request.extensions_mut().insert(broker.clone());
    drop(brokers);
    next.run(request).await
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventPayload {
    pub name: String,
    pub data: String,
    pub channels: Option<HashSet<ChannelName>>,
    pub channel: Option<ChannelName>,
    pub socket_id: Option<SocketId>,
}

async fn publish(
    Extension(broker): Extension<Broker>,
    Json(payload): Json<EventPayload>,
) -> impl IntoResponse {
    let channel = payload.channel.unwrap();
    let event = ServerEvent::ChannelEvent(CustomEvent {
        event: payload.name,
        data: payload.data.into(),
        channel: channel.to_owned(),
        user_id: None,
    });
    match broker.publish(channel.as_ref(), event).await {
        Ok(()) => Ok(Json(json!({ "ok": true }))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn list_channels(Extension(broker): Extension<Broker>) -> impl IntoResponse {
    let channels = broker
        .subscriptions()
        .await
        .filter(|(_, count)| *count > 0)
        .map(|(channel, count)| {
            (
                channel,
                json!({
                    "subscription_count": count,
                    "user_count": count,
                }),
            )
        })
        .collect::<HashMap<String, serde_json::Value>>();

    Json(channels)
}

async fn handle_ws(
    Extension(broker): Extension<Broker>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
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
