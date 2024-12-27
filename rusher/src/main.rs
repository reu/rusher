use std::{
    collections::{HashMap, HashSet},
    env,
};

use futures::{Stream, StreamExt};
use rusher_core::ServerEvent;
use rusher_pubsub::{redis::RedisBroker, AnyBroker, Broker};
use rusher_server::App;
use tokio::net::TcpListener;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[tokio::main]
async fn main() {
    let log_level = env::var("LOG_LEVEL")
        .ok()
        .and_then(|level| level.parse().ok())
        .unwrap_or(Level::DEBUG);

    let log_format = env::var("LOG_FORMAT")
        .ok()
        .and_then(|f| {
            f.to_lowercase()
                .contains("json")
                .then(|| tracing_subscriber::fmt::layer().json().boxed())
        })
        .unwrap_or_else(|| tracing_subscriber::fmt::layer().pretty().boxed());

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
                .map(|(id, secret)| App::new(id, secret))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    tracing_subscriber::registry()
        .with(log_format)
        .with(
            tracing_subscriber::filter::Targets::new()
                .with_target("tower_http", log_level)
                .with_target("rusher_server", log_level)
                .with_default(Level::INFO),
        )
        .init();

    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();

    let app_repo = match env::var("REDIS_URL") {
        Ok(redis_url) => {
            let (publisher, subscriber) =
                RedisBroker::new_connection_pair(&redis_url).await.unwrap();

            let mut repo = HashMap::new();
            for app in apps {
                let namespace = app.id.to_string();
                repo.insert(
                    app,
                    AnyBroker::redis_single(publisher.clone(), subscriber.clone(), &namespace)
                        .await
                        .unwrap(),
                );
            }
            repo
        }
        _ => apps
            .into_iter()
            .map(|app| (app, AnyBroker::memory()))
            .collect(),
    };

    for (app, broker) in app_repo.iter() {
        tokio::spawn(send_webhooks(app.clone(), broker.all_messages()));
    }

    rusher_server::serve(listener, rusher_server::app(app_repo))
        .await
        .unwrap();
}

async fn send_webhooks(_app: App, mut messages: impl Stream<Item = ServerEvent> + Unpin) {
    while let Some(_msg) = messages.next().await {
        // TODO: send webhook
    }
}
