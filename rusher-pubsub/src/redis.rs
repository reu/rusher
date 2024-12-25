use std::{borrow::Borrow, collections::HashSet, mem, sync::Arc};

use fred::{
    clients::RedisClient,
    interfaces::{ClientLike, EventInterface, HashesInterface, PubsubInterface},
    types::{Message as RedisMessage, RedisConfig},
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{error::TryRecvError, Receiver as RedisReceiver};

use super::{BoxError, Broker, Connection};

#[derive(Debug, Clone)]
pub struct RedisBroker {
    redis: RedisClient,
    subscriber: RedisClient,
    namespace: Namespace,
}

impl RedisBroker {
    pub async fn from_url(url: &str, namespace: &str) -> Result<Self, BoxError> {
        let namespace = Namespace(Arc::new(namespace.to_string()));
        let (redis, subscriber) = RedisBroker::new_connection_pair(url).await?;
        subscriber
            .psubscribe(namespace.channels_messages_pattern())
            .await?;
        Ok(Self {
            redis,
            subscriber,
            namespace,
        })
    }

    pub async fn from_connection_pair(
        publisher: RedisClient,
        subscriber: RedisClient,
        namespace: &str,
    ) -> Result<Self, BoxError> {
        let namespace = Namespace(Arc::new(namespace.to_string()));
        subscriber
            .psubscribe(namespace.channels_messages_pattern())
            .await?;
        Ok(Self {
            redis: publisher,
            subscriber,
            namespace,
        })
    }

    pub async fn new_connection_pair(url: &str) -> Result<(RedisClient, RedisClient), BoxError> {
        let publisher = RedisClient::new(RedisConfig::from_url(url)?, None, None, None);
        let subscriber = publisher.clone_new();
        publisher.init().await?;
        subscriber.init().await?;
        Ok((publisher, subscriber))
    }
}

impl Broker for RedisBroker {
    type Conn = RedisConnection;

    async fn connect(&self) -> Result<Self::Conn, BoxError> {
        Ok(RedisConnection {
            namespace: self.namespace.clone(),
            publisher: self.redis.clone(),
            msgs: self.subscriber.message_rx(),
            subs: HashSet::default(),
        })
    }

    async fn subscribers_count(&self, channel: &str) -> usize {
        self.redis
            .hget::<usize, _, _>(self.namespace.subscriptions_key(), channel)
            .await
            .unwrap_or(0)
    }

    async fn subscriptions(&self) -> HashSet<(String, usize)> {
        self.redis
            .hgetall::<Vec<String>, _>(self.namespace.subscriptions_key())
            .await
            .unwrap_or_default()
            .chunks_exact(2)
            .filter_map(|res| Some((res[0].clone(), res[1].parse().ok()?)))
            .filter(|(_, count)| *count > 0)
            .collect()
    }

    async fn publish(&self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.redis
            .publish::<(), _, _>(
                self.namespace.channel_messages_key(channel),
                serde_json::to_string(&msg)?,
            )
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RedisConnection {
    namespace: Namespace,
    publisher: RedisClient,
    msgs: RedisReceiver<RedisMessage>,
    subs: HashSet<String>,
}

impl Connection for RedisConnection {
    async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.publisher
            .publish::<(), _, _>(
                self.namespace.channel_messages_key(channel),
                serde_json::to_string(&msg)?,
            )
            .await?;
        Ok(())
    }

    async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.publisher
            .hincrby::<(), _, _>(self.namespace.subscriptions_key(), channel, 1)
            .await?;
        self.subs.insert(channel.to_owned());
        Ok(())
    }

    async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.publisher
            .hincrby::<(), _, _>(self.namespace.subscriptions_key(), channel, -1)
            .await?;
        self.subs.remove(channel);
        Ok(())
    }

    async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, BoxError> {
        let namespace = self.namespace.as_str();
        loop {
            let msg = self.msgs.recv().await?;
            if self.subs.contains(&channel_name(&msg)) && namespace == channel_namespace(&msg) {
                let msg = msg.value.as_str().ok_or::<&str>("Invalid message")?;
                let msg = serde_json::from_str(msg.borrow())?;
                return Ok(msg);
            }
        }
    }

    async fn try_recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, BoxError> {
        let namespace = self.namespace.as_str();
        loop {
            match self.msgs.try_recv() {
                Ok(msg)
                    if msg.value.as_str().is_some()
                        && self.subs.contains(&channel_name(&msg))
                        && namespace == channel_namespace(&msg) =>
                {
                    match serde_json::from_str(&msg.value.as_str().unwrap()) {
                        Ok(msg) => return Ok(Some(msg)),
                        _ => return Ok(None),
                    }
                }
                Ok(_) => continue,
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Lagged(_)) => continue,
                Err(err) => return Err(err.into()),
            }
        }
    }
}

impl Drop for RedisConnection {
    fn drop(&mut self) {
        let redis = self.publisher.clone();
        let channels = mem::take(&mut self.subs);
        let namespace = mem::take(&mut self.namespace);
        tokio::spawn(async move {
            for channel in channels {
                redis
                    .hincrby::<(), _, _>(namespace.subscriptions_key(), channel, -1)
                    .await?;
            }
            Ok::<_, BoxError>(())
        });
    }
}

#[derive(Debug, Clone, Default)]
struct Namespace(Arc<String>);

impl Namespace {
    pub fn channels_messages_pattern(&self) -> String {
        format!("channels:{}:msgs:*", self.0)
    }

    pub fn channel_messages_key(&self, channel: &str) -> String {
        format!("channels:{}:msgs:{channel}", self.0)
    }

    pub fn subscriptions_key(&self) -> String {
        format!("subs:{}", self.0)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

fn channel_name(msg: &RedisMessage) -> String {
    msg.channel
        .split(':')
        .map(|part| part.to_owned())
        .last()
        .unwrap_or_default()
}

fn channel_namespace(msg: &RedisMessage) -> String {
    msg.channel
        .split(':')
        .map(|part| part.to_owned())
        .nth(1)
        .unwrap_or_default()
}

#[cfg(test)]
mod test {
    use std::env;

    use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage};
    use tokio::time;

    use super::*;

    async fn create_redis_container() -> ContainerAsync<GenericImage> {
        env::set_var("TESTCONTAINERS", "remove");
        GenericImage::new("redis", "latest")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
    }

    async fn create_broker(container: &ContainerAsync<GenericImage>) -> RedisBroker {
        let port = container.get_host_port_ipv4(6379).await;
        RedisBroker::from_url(&format!("redis://localhost:{port}/1"), "test")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_pubsub() {
        let container = create_redis_container().await;
        let broker1 = create_broker(&container).await;
        let broker2 = create_broker(&container).await;

        let mut conn1 = broker1.connect().await.unwrap();
        let mut conn2 = broker1.connect().await.unwrap();
        let mut conn3 = broker2.connect().await.unwrap();

        conn1.subscribe("channel_all").await.unwrap();
        conn2.subscribe("channel_all").await.unwrap();
        conn3.subscribe("channel_all").await.unwrap();

        conn2.subscribe("channel2").await.unwrap();

        conn3.subscribe("channel3").await.unwrap();

        conn1.publish("channel_all", "1").await.unwrap();
        conn2.publish("channel_all", "2").await.unwrap();
        conn3.publish("channel_all", "3").await.unwrap();

        conn1.publish("channel2", "only 2").await.unwrap();
        conn1.publish("channel3", "only 3").await.unwrap();

        assert_eq!("1", conn1.recv::<String>().await.unwrap());
        assert_eq!("2", conn1.recv::<String>().await.unwrap());
        assert_eq!("3", conn1.recv::<String>().await.unwrap());

        assert_eq!("1", conn2.recv::<String>().await.unwrap());
        assert_eq!("2", conn2.recv::<String>().await.unwrap());
        assert_eq!("3", conn2.recv::<String>().await.unwrap());
        assert_eq!("only 2", conn2.recv::<String>().await.unwrap());

        assert_eq!("1", conn3.recv::<String>().await.unwrap());
        assert_eq!("2", conn3.recv::<String>().await.unwrap());
        assert_eq!("3", conn3.recv::<String>().await.unwrap());
        assert_eq!("only 3", conn3.recv::<String>().await.unwrap());
    }

    #[tokio::test]
    async fn test_unsubsribe() {
        let container = create_redis_container().await;
        let broker1 = create_broker(&container).await;
        let broker2 = create_broker(&container).await;

        let mut conn1 = broker1.connect().await.unwrap();
        let mut conn2 = broker2.connect().await.unwrap();

        conn1.subscribe("channel").await.unwrap();
        conn2.subscribe("channel").await.unwrap();

        conn1.publish("channel", "1").await.unwrap();
        assert_eq!("1", conn1.recv::<String>().await.unwrap());
        assert_eq!("1", conn2.recv::<String>().await.unwrap());

        conn1.unsubscribe("channel").await.unwrap();

        conn2.publish("channel", "3").await.unwrap();

        assert_eq!("3", conn2.recv::<String>().await.unwrap());
        assert_eq!(None, conn1.try_recv::<String>().await.unwrap());
    }

    #[tokio::test]
    async fn test_broker_subscribers_count() {
        let container = create_redis_container().await;
        let broker1 = create_broker(&container).await;
        let broker2 = create_broker(&container).await;

        let mut conn1 = broker1.connect().await.unwrap();
        let mut conn2 = broker2.connect().await.unwrap();

        conn1.subscribe("channel1").await.unwrap();
        conn1.subscribe("channel2").await.unwrap();
        conn2.subscribe("channel1").await.unwrap();

        assert_eq!(0, broker1.subscribers_count("channel0").await);
        assert_eq!(2, broker1.subscribers_count("channel1").await);
        assert_eq!(1, broker1.subscribers_count("channel2").await);

        conn1.unsubscribe("channel1").await.unwrap();

        assert_eq!(1, broker2.subscribers_count("channel1").await);

        drop(conn2);
        time::sleep(time::Duration::from_secs(1)).await;

        assert_eq!(0, broker2.subscribers_count("channel1").await);
    }

    #[tokio::test]
    async fn test_subscriptions() {
        let container = create_redis_container().await;
        let broker1 = create_broker(&container).await;
        let broker2 = create_broker(&container).await;

        let mut conn1 = broker1.connect().await.unwrap();
        let mut conn2 = broker2.connect().await.unwrap();

        conn1.subscribe("channel1").await.unwrap();
        conn1.subscribe("channel2").await.unwrap();
        conn1.subscribe("channel3").await.unwrap();

        conn2.subscribe("channel1").await.unwrap();
        conn2.subscribe("channel3").await.unwrap();
        conn2.unsubscribe("channel3").await.unwrap();

        assert_eq!(
            HashSet::from_iter([
                (String::from("channel1"), 2),
                (String::from("channel2"), 1),
                (String::from("channel3"), 1)
            ]),
            broker1.subscriptions().await
        );

        drop(conn1);
        time::sleep(time::Duration::from_secs(1)).await;

        assert_eq!(
            HashSet::from_iter([(String::from("channel1"), 1)]),
            broker2.subscriptions().await
        );
    }
}
