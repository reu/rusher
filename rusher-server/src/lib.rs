use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
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
use rusher_core::{
    signature::{sign_private_channel, sign_user_data},
    ChannelName, ClientEvent, ConnectionInfo, CustomEvent, ServerEvent, SigninInformation,
    SocketId, UserData,
};
use rusher_pubsub::{AnyBroker, Broker, Connection};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::sync::mpsc;

mod authentication;

pub use axum::serve;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct App {
    pub id: AppId,
    secret: AppSecret,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppId(String);

impl fmt::Display for AppId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppSecret(String);

impl App {
    pub fn new(id: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            id: AppId(id.into()),
            secret: AppSecret(secret.into()),
        }
    }
}

pub trait IntoAppRepository {
    type AppRepository: AppRepository + 'static;
    fn into_app_repository(self) -> Self::AppRepository;
}

impl<I: IntoIterator<Item = (App, AnyBroker)>> IntoAppRepository for I {
    type AppRepository = HashMap<AppId, (App, AnyBroker)>;

    fn into_app_repository(self) -> Self::AppRepository {
        self.into_iter()
            .map(|(app, broker)| (app.id.clone(), (app, broker)))
            .collect()
    }
}

#[async_trait]
pub trait AppRepository: Send + Sync {
    async fn secret_for_app(&self, app_id: &AppId) -> Option<AppSecret>;
    async fn broker_for_app(&self, app_id: &AppId) -> Option<AnyBroker>;
}

#[async_trait]
impl AppRepository for HashMap<AppId, (App, AnyBroker)> {
    async fn secret_for_app(&self, app_id: &AppId) -> Option<AppSecret> {
        self.get(app_id).map(|(app, _)| app.secret.clone())
    }

    async fn broker_for_app(&self, app_id: &AppId) -> Option<AnyBroker> {
        self.get(app_id).map(|(_, broker)| broker).cloned()
    }
}

pub fn app(app_repo: impl IntoAppRepository) -> Router {
    let app_repo = app_repo.into_app_repository();
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
    Extension(AppId(app_id)): Extension<AppId>,
    Extension(AppSecret(secret)): Extension<AppSecret>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    match broker.connect().await {
        Ok(mut connection) => Ok(ws.on_upgrade(move |ws| async move {
            let app_id = app_id.clone();
            let secret = secret.clone();
            let socket_id: SocketId = rand::thread_rng().gen();

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

            let read_messages = async move {
                let app_id = app_id.clone();
                let secret = secret.clone();

                tx.send(ServerEvent::ConnectionEstablished {
                    data: ConnectionInfo {
                        socket_id: socket_id.clone(),
                        activity_timeout: 120,
                    },
                })
                .await?;

                loop {
                    let app_id = app_id.clone();
                    let secret = secret.clone();

                    tokio::select! {
                        Ok(msg) = connection.recv() => {
                            tx.send(msg).await.ok();
                        },

                        Some(Ok(msg)) = read_ws.next() => {
                            match msg {
                                Message::Text(text) => {
                                    match serde_json::from_str(&text) {
                                        Ok(ClientEvent::Signin { auth, user_data }) => {
                                            let (sent_id, auth) = auth.split_once(":").unwrap_or_default();

                                            let valid_signature =
                                                sign_user_data(secret, &socket_id, &user_data)
                                                    .map(|signature| {
                                                        signature.verify(hex::decode(auth).unwrap_or_default())
                                                    })
                                                    .unwrap_or(false);

                                            if app_id != sent_id || !valid_signature {
                                                tx.send(ServerEvent::Error {
                                                    message: String::from("Invalid signature"),
                                                    code: Some(409),
                                                })
                                                .await?;
                                                continue
                                            }

                                            let user = serde_json::from_str::<UserData>(&user_data).unwrap();

                                            if connection.authenticate(&user.id, &user).await.is_err() {
                                                tx.send(ServerEvent::Error {
                                                    message: String::from("Failed to authenticate user"),
                                                    code: Some(409),
                                                })
                                                .await?;
                                                continue
                                            }

                                            tx.send(ServerEvent::SigninSucceeded {
                                                data: SigninInformation { user_data: user }
                                            })
                                            .await?;

                                            continue
                                        },
                                        Ok(ClientEvent::Ping) => tx.send(ServerEvent::Pong).await?,
                                        Ok(ClientEvent::Subscribe { channel, auth, .. }) => {
                                            match channel {
                                                ref channel @ ChannelName::Private(_) => {
                                                    let (sent_id, auth) = auth
                                                        .as_ref()
                                                        .and_then(|auth| auth.split_once(':'))
                                                        .unwrap_or_default();

                                                    let valid_signature =
                                                        sign_private_channel(secret, &socket_id, channel)
                                                            .map(|signature| {
                                                                signature.verify(hex::decode(auth).unwrap_or_default())
                                                            })
                                                            .unwrap_or(false);

                                                    if app_id != sent_id || !valid_signature {
                                                        tx.send(ServerEvent::Error {
                                                            message: String::from("Invalid signature"),
                                                            code: Some(409),
                                                        })
                                                        .await?;
                                                        continue
                                                    }
                                                },
                                                ChannelName::Presence(_) => {
                                                    tx.send(ServerEvent::Error {
                                                        message: String::from("Presence channels are not supported"),
                                                        code: None,
                                                    })
                                                    .await?;
                                                },
                                                ChannelName::Encrypted(_) => {
                                                    tx.send(ServerEvent::Error {
                                                        message: String::from("Encrypted channels are not supported"),
                                                        code: None,
                                                    })
                                                    .await?;
                                                },
                                                _ => {},
                                            };
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

                        else => break,
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
