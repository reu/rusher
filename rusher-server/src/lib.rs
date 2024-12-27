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
use rusher_core::{ChannelName, ConnectionInfo, CustomEvent, ServerEvent, SocketId};
use rusher_pubsub::{AnyBroker, Broker, Connection};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::sync::mpsc;

mod authentication;
mod websocket;

pub use axum::serve;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::{debug, error, info_span, Instrument, Level};
use websocket::ConnectionProtocol;

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
        .layer(
            TraceLayer::new_for_http()
                .on_response(DefaultOnResponse::default().level(Level::INFO))
                .make_span_with(|request: &Request<_>| {
                    info_span!(
                        "request",
                        uri = ?request.uri(),
                        method = ?request.method(),
                    )
                }),
        )
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
            request.extensions_mut().insert(app_id.clone());
            request.extensions_mut().insert(secret);
            request.extensions_mut().insert(broker);
            next.run(request)
                .instrument(info_span!("app_request", app_id = app_id.0))
                .await
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
            let socket_id: SocketId = rand::thread_rng().gen();
            let _span = info_span!("websocket", %app_id, %socket_id);

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
            }.instrument(info_span!("websocket_connection_write", %app_id, %socket_id));

            let mut proto = ConnectionProtocol {
                tx: tx.clone(),
                app_id: app_id.clone(),
                secret,
                socket_id: socket_id.clone(),
                current_user_id: None,
            };

            let connection_established = ServerEvent::ConnectionEstablished {
                data: ConnectionInfo {
                    socket_id: socket_id.clone(),
                    activity_timeout: 120,
                },
            };

            let read_messages = async move {
                tx.send(connection_established).await?;

                loop {
                    tokio::select! {
                        Ok(msg) = connection.recv() => {
                            tx.send(msg).await?;
                        },

                        Some(Ok(msg)) = read_ws.next() => {
                            match msg {
                                Message::Text(text) => {
                                    match serde_json::from_str(&text) {
                                        Ok(msg) => {
                                            if let Err(error) = proto.handle_message(&mut connection, msg).await {
                                                error!(%error);
                                                bail!(error)
                                            }
                                        },
                                        Err(error) => {
                                            debug!(msg = "could not decode message", %text, %error);
                                            continue
                                        },
                                    };
                                }
                                _ => continue,
                            }
                        }

                        else => break,
                    }
                }
                anyhow::Ok(())
            }.instrument(info_span!("websocket_connection_read", %app_id, %socket_id));

            tokio::select! {
                _ = write_messages => debug!("Writer finished"),
                _ = read_messages => debug!("Reader finished"),
            };

            debug!("Client disconnected");
        })),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
