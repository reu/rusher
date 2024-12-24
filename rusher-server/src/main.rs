use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
};

use anyhow::bail;
use async_trait::async_trait;
use authentication::check_signature_middleware;
use axum::{
    body::Body,
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
use rusher_pubsub::{AnyBroker, Broker, Connection};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::{net::TcpListener, sync::mpsc};

mod authentication;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppSecret(String);

#[tokio::main]
async fn main() {
    let port = env::var("PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(4444);

    let apps = env::var("APP")
        .ok()
        .map(|app| {
            app.split(',')
                .map(|app| app.trim())
                .filter_map(|app| app.split_once(':'))
                .map(|(id, secret)| (AppId(id.to_owned()), AppSecret(secret.to_owned())))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();

    let app_repo = match env::var("REDIS_URL") {
        Ok(redis_url) => {
            let mut repo = HashMap::new();
            for (id, secret) in apps {
                let broker = AnyBroker::redis(&redis_url, &id.0).await.unwrap();
                repo.insert(id, (secret, broker));
            }
            repo
        }
        _ => apps
            .into_iter()
            .map(|(id, secret)| (id, (secret, AnyBroker::memory())))
            .collect(),
    };

    let app = app(app_repo);

    axum::serve(listener, app).await.unwrap();
}

#[async_trait]
pub trait AppRepository: Send + Sync {
    async fn secret_for_app(&self, app_id: &AppId) -> Option<AppSecret>;
    async fn broker_for_app(&self, app_id: &AppId) -> Option<AnyBroker>;
}

#[async_trait]
impl AppRepository for HashMap<AppId, (AppSecret, AnyBroker)> {
    async fn secret_for_app(&self, app_id: &AppId) -> Option<AppSecret> {
        self.get(app_id).map(|(secret, _)| secret).cloned()
    }

    async fn broker_for_app(&self, app_id: &AppId) -> Option<AnyBroker> {
        self.get(app_id).map(|(_, broker)| broker).cloned()
    }
}

pub fn app<T: AppRepository + 'static>(app_repo: T) -> Router {
    Router::new()
        .route("/apps/:app/channels", get(list_channels))
        .route("/apps/:app/events", post(publish))
        .route_layer(middleware::from_fn(check_signature_middleware))
        .route("/app/:app", any(handle_ws))
        .route_layer(middleware::from_fn_with_state(
            Arc::new(app_repo) as Arc<dyn AppRepository>,
            broker_middleware,
        ))
}

async fn broker_middleware(
    State(app_repo): State<Arc<dyn AppRepository>>,
    Path(app): Path<String>,
    mut request: Request,
    next: Next,
) -> Response {
    let app_id = AppId(app);
    match (
        app_repo.secret_for_app(&app_id).await,
        app_repo.broker_for_app(&app_id).await,
    ) {
        (Some(secret), Some(broker)) => {
            request.extensions_mut().insert(app_id);
            request.extensions_mut().insert(secret);
            request.extensions_mut().insert(broker);
            next.run(request).await
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventPayload {
    pub name: String,
    pub data: JsonValue,
    pub channels: Option<HashSet<ChannelName>>,
    pub channel: Option<ChannelName>,
    pub socket_id: Option<SocketId>,
}

async fn publish(
    Extension(broker): Extension<AnyBroker>,
    Json(payload): Json<EventPayload>,
) -> Result<Json<JsonValue>, StatusCode> {
    let event = payload.name;
    let data = payload.data;

    let channels = match (payload.channel, payload.channels) {
        (Some(channel), Some(mut channels)) => {
            channels.insert(channel);
            channels
        }
        (Some(channel), None) => HashSet::from_iter([channel]),
        (None, Some(channels)) => channels,
        _ => HashSet::new(),
    };

    for channel in channels {
        let event = ServerEvent::ChannelEvent(CustomEvent {
            event: event.clone(),
            data: data.clone(),
            channel: channel.clone(),
            user_id: None,
        });

        broker
            .publish(channel.as_ref(), event)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    Ok(Json(json!({ "ok": true })))
}

async fn list_channels(Extension(broker): Extension<AnyBroker>) -> impl IntoResponse {
    let channels = broker
        .subscriptions()
        .await
        .into_iter()
        .map(|(channel, count)| {
            (
                channel,
                json!({
                    "subscription_count": count,
                    "user_count": count,
                }),
            )
        })
        .collect::<HashMap<String, JsonValue>>();

    Json(channels)
}

async fn handle_ws(
    Extension(broker): Extension<AnyBroker>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    match broker.connect().await {
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
