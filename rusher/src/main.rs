use std::{collections::HashMap, env};

use clap::Parser;
use futures::{Stream, StreamExt};
use rusher_core::ServerEvent;
use rusher_pubsub::{redis::RedisBroker, AnyBroker, Broker};
use rusher_server::App;
use tokio::{fs, net::TcpListener};
use tracing::{debug, info, instrument, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::config::{AppConfig, BrokerProtocol, Cli, Config, WebhookConfig};

mod config;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = env::var("LOG_LEVEL")
        .ok()
        .and_then(|level| level.parse().ok())
        .unwrap_or(Level::INFO);

    let log_format = env::var("LOG_FORMAT")
        .ok()
        .and_then(|f| {
            f.to_lowercase()
                .contains("json")
                .then(|| tracing_subscriber::fmt::layer().json().boxed())
        })
        .unwrap_or_else(|| tracing_subscriber::fmt::layer().compact().boxed());

    let config = match cli.config {
        Some(path) => {
            toml::from_str::<Config>(&fs::read_to_string(path).await.unwrap_or_default()).unwrap()
        }
        None => Config::default(),
    };

    let port = env::var("PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .or(config.server.port)
        .unwrap_or(8778);

    let binding = config.server.binding.unwrap_or(String::from("0.0.0.0"));

    let apps = env::var("APP")
        .ok()
        .map(|app| {
            app.split(',')
                .map(|app| app.trim())
                .filter_map(|app| app.split_once(':'))
                .map(|(key, secret)| AppConfig {
                    key: key.to_string(),
                    secret: secret.to_string(),
                    webhooks: vec![],
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| config.apps);

    tracing_subscriber::registry()
        .with(log_format)
        .with(
            tracing_subscriber::filter::Targets::new()
                .with_target("tower_http", log_level)
                .with_target("rusher_server", log_level)
                .with_default(log_level),
        )
        .init();

    let listener = TcpListener::bind((binding, port)).await.unwrap();

    let redis_url = env::var("REDIS_URL").ok().or_else(|| {
        (config.broker.protocol == BrokerProtocol::Redis)
            .then_some(config.broker.url)
            .flatten()
    });

    let app_repo = match redis_url {
        Some(redis_url) => {
            let (publisher, subscriber) =
                RedisBroker::new_connection_pair(&redis_url).await.unwrap();

            let mut repo = HashMap::new();
            for app in apps {
                let namespace = app.key.to_string();
                let broker =
                    AnyBroker::redis_single(publisher.clone(), subscriber.clone(), &namespace)
                        .await
                        .unwrap();

                tokio::spawn(send_webhooks(app.clone(), broker.all_messages()));

                debug!(app = app.key, url = redis_url, "using redis broker");

                repo.insert(App::new(app.key, app.secret), broker);
            }
            repo
        }
        _ => apps
            .into_iter()
            .map(|app| {
                let broker = AnyBroker::memory();
                tokio::spawn(send_webhooks(app.clone(), broker.all_messages()));
                debug!(app = app.key, "using memory broker");
                (App::new(app.key, app.secret), broker)
            })
            .collect(),
    };

    info!(port, "starting server");
    rusher_server::serve(listener, rusher_server::app(app_repo))
        .await
        .unwrap();
}

#[instrument(skip(app, messages), fields(key = app.key))]
async fn send_webhooks(app: AppConfig, mut messages: impl Stream<Item = ServerEvent> + Unpin) {
    while let Some(_msg) = messages.next().await {
        for WebhookConfig { url } in app.webhooks.iter() {
            debug!(url, "sending webhook");
            // TODO: send webhook
        }
    }
}
