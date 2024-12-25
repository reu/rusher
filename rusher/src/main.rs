use std::{
    collections::{HashMap, HashSet},
    env,
};

use rusher_pubsub::{redis::RedisBroker, AnyBroker};
use rusher_server::App;
use tokio::net::TcpListener;

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
                .map(|(id, secret)| App::new(id, secret))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

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

    axum::serve(listener, rusher_server::app(app_repo))
        .await
        .unwrap();
}
