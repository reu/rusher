#![allow(async_fn_in_trait)]

use std::{collections::HashSet, error::Error};

use fred::prelude::RedisClient;
use serde::{de::DeserializeOwned, Serialize};

pub mod memory;
pub mod redis;

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

pub trait Broker: Clone {
    type Conn: Connection;
    async fn connect(&self) -> Result<Self::Conn, BoxError>;
    async fn subscribers_count(&self, channel: &str) -> usize;
    async fn subscriptions(&self) -> HashSet<(String, usize)>;
    async fn publish(&self, channel: &str, msg: impl Serialize) -> Result<(), BoxError>;
}

pub trait Connection {
    async fn authenticate(&mut self, user_id: &str, data: impl Serialize) -> Result<(), BoxError>;
    async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError>;
    async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError>;
    async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError>;
    async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, BoxError>;
    async fn try_recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, BoxError>;
}

#[derive(Debug, Clone)]
pub enum AnyBroker {
    Memory(memory::MemoryBroker),
    Redis(redis::RedisBroker),
}

impl AnyBroker {
    pub fn memory() -> Self {
        Self::Memory(memory::MemoryBroker::default())
    }

    pub async fn redis(url: &str, namespace: &str) -> Result<Self, BoxError> {
        Ok(Self::Redis(
            redis::RedisBroker::from_url(url, namespace).await?,
        ))
    }

    pub async fn redis_single(
        publisher: RedisClient,
        subscriber: RedisClient,
        namespace: &str,
    ) -> Result<Self, BoxError> {
        Ok(Self::Redis(
            redis::RedisBroker::from_connection_pair(publisher, subscriber, namespace).await?,
        ))
    }
}

impl Broker for AnyBroker {
    type Conn = AnyConnection;

    async fn connect(&self) -> Result<Self::Conn, BoxError> {
        match self {
            Self::Memory(broker) => Ok(AnyConnection::Memory(broker.connect().await?)),
            Self::Redis(broker) => Ok(AnyConnection::Redis(broker.connect().await?)),
        }
    }

    async fn subscribers_count(&self, channel: &str) -> usize {
        match self {
            Self::Memory(broker) => broker.subscribers_count(channel).await,
            Self::Redis(broker) => broker.subscribers_count(channel).await,
        }
    }

    async fn subscriptions(&self) -> HashSet<(String, usize)> {
        match self {
            Self::Memory(broker) => broker.subscriptions().await,
            Self::Redis(broker) => broker.subscriptions().await,
        }
    }

    async fn publish(&self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        match self {
            Self::Memory(broker) => broker.publish(channel, msg).await,
            Self::Redis(broker) => broker.publish(channel, msg).await,
        }
    }
}

pub enum AnyConnection {
    Memory(memory::MemoryConnection),
    Redis(redis::RedisConnection),
}

impl Connection for AnyConnection {
    async fn authenticate(&mut self, user_id: &str, data: impl Serialize) -> Result<(), BoxError> {
        match self {
            Self::Memory(broker) => broker.authenticate(user_id, data).await,
            Self::Redis(broker) => broker.authenticate(user_id, data).await,
        }
    }

    async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        match self {
            Self::Memory(broker) => broker.publish(channel, msg).await,
            Self::Redis(broker) => broker.publish(channel, msg).await,
        }
    }

    async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        match self {
            Self::Memory(broker) => broker.subscribe(channel).await,
            Self::Redis(broker) => broker.subscribe(channel).await,
        }
    }

    async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        match self {
            Self::Memory(broker) => broker.unsubscribe(channel).await,
            Self::Redis(broker) => broker.unsubscribe(channel).await,
        }
    }

    async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, BoxError> {
        match self {
            Self::Memory(broker) => broker.recv().await,
            Self::Redis(broker) => broker.recv().await,
        }
    }

    async fn try_recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, BoxError> {
        match self {
            Self::Memory(broker) => broker.try_recv().await,
            Self::Redis(broker) => broker.try_recv().await,
        }
    }
}
