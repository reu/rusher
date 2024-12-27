use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(short, long)]
    pub config: Option<PathBuf>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub broker: BrokerConfig,
    pub apps: Vec<AppConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub port: Option<u16>,
    pub binding: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BrokerConfig {
    #[serde(default)]
    pub protocol: BrokerProtocol,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Parser)]
#[serde(rename_all = "snake_case")]
pub enum BrokerProtocol {
    #[default]
    Memory,
    Redis,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AppConfig {
    pub key: String,
    pub secret: String,
    pub webhooks: Vec<WebhookConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
}
