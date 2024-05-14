use std::{borrow::Borrow, collections::HashSet, mem, sync::Arc};

use fred::{
    clients::RedisClient,
    interfaces::{ClientLike, EventInterface, KeysInterface, PubsubInterface},
    types::{Message as RedisMessage, RedisConfig, ScanType, Scanner},
};
use futures::TryStreamExt;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{error::TryRecvError, Receiver as RedisReceiver};

use super::{BoxError, Broker, Connection};

#[derive(Debug, Clone)]
pub struct RedisBroker {
    redis: RedisClient,
    namespace: Arc<String>,
}

impl RedisBroker {
    pub async fn from_url(url: &str, namespace: &str) -> Result<Self, BoxError> {
        let redis = RedisClient::new(RedisConfig::from_url(url)?, None, None, None);
        redis.init().await?;
        Ok(Self {
            redis,
            namespace: Arc::new(namespace.to_string()),
        })
    }
}

impl Broker for RedisBroker {
    type Conn = RedisConnection;

    async fn connect(&self) -> Result<Self::Conn, BoxError> {
        let publisher = self.redis.clone();
        let subscriber = self.redis.clone_new();
        publisher.init().await?;
        subscriber.init().await?;
        let msgs = subscriber.message_rx();
        Ok(RedisConnection {
            namespace: self.namespace.clone(),
            publisher,
            subscriber,
            msgs,
            subs: HashSet::default(),
        })
    }

    async fn subscribers_count(&self, channel: &str) -> usize {
        self.redis
            .get::<usize, _>(format!("subs:{}:{channel}:count", self.namespace))
            .await
            .unwrap_or(0)
    }

    async fn subscriptions(&self) -> Vec<(String, usize)> {
        self.redis
            .scan(
                format!("subs:{}:*:count", self.namespace),
                Some(256),
                Some(ScanType::String),
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|mut res| {
                let a = res.take_results().unwrap_or_default();
                dbg!(a);
                (String::from("lol"), 10)
            })
            .collect()
    }

    async fn publish(&self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.redis
            .publish(channel.to_owned(), serde_json::to_string(&msg)?)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RedisConnection {
    namespace: Arc<String>,
    publisher: RedisClient,
    subscriber: RedisClient,
    msgs: RedisReceiver<RedisMessage>,
    subs: HashSet<String>,
}

impl Connection for RedisConnection {
    async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.publisher
            .publish(channel.to_owned(), serde_json::to_string(&msg)?)
            .await?;
        Ok(())
    }

    async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.subscriber.subscribe(channel).await?;
        self.publisher
            .incr(format!("subs:{}:{channel}:count", self.namespace))
            .await?;
        self.subs.insert(channel.to_owned());
        Ok(())
    }

    async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.subscriber.unsubscribe(channel).await?;
        self.publisher
            .decr(format!("subs:{}:{channel}:count", self.namespace))
            .await?;
        self.subs.remove(channel);
        Ok(())
    }

    async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, BoxError> {
        let msg = self.msgs.recv().await?;
        let msg = msg.value.as_str().ok_or::<&str>("Invalid message")?;
        let msg = serde_json::from_str(msg.borrow())?;
        Ok(msg)
    }

    async fn try_recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, BoxError> {
        loop {
            match self.msgs.try_recv() {
                Ok(msg) if msg.value.as_str().is_some() => {
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
                    .decr(format!("subs:{namespace}:{channel}:count"))
                    .await?;
            }
            Ok::<_, BoxError>(())
        });
    }
}

#[cfg(test)]
mod test {
    use std::env;

    use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage};
    use tokio::time;

    use super::*;

    #[tokio::test]
    async fn test_pubsub() {
        env::set_var("TESTCONTAINERS", "remove");

        let container = GenericImage::new("redis", "latest")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await;

        let broker = {
            let port = container.get_host_port_ipv4(6379).await;
            RedisBroker::from_url(&format!("redis://localhost:{port}/1"), "test")
                .await
                .unwrap()
        };

        let mut conn1 = broker.connect().await.unwrap();
        let mut conn2 = broker.connect().await.unwrap();
        let mut conn3 = broker.connect().await.unwrap();

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
        env::set_var("TESTCONTAINERS", "remove");

        let container = GenericImage::new("redis", "latest")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await;

        let broker = {
            let port = container.get_host_port_ipv4(6379).await;
            RedisBroker::from_url(&format!("redis://localhost:{port}/1"), "test")
                .await
                .unwrap()
        };

        let mut conn1 = broker.connect().await.unwrap();
        let mut conn2 = broker.connect().await.unwrap();

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
        env::set_var("TESTCONTAINERS", "remove");

        let container = GenericImage::new("redis", "latest")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await;

        let broker = {
            let port = container.get_host_port_ipv4(6379).await;
            RedisBroker::from_url(&format!("redis://localhost:{port}/1"), "test")
                .await
                .unwrap()
        };

        let mut conn1 = broker.connect().await.unwrap();
        let mut conn2 = broker.connect().await.unwrap();

        conn1.subscribe("channel1").await.unwrap();
        conn1.subscribe("channel2").await.unwrap();
        conn2.subscribe("channel1").await.unwrap();

        assert_eq!(0, broker.subscribers_count("channel0").await);
        assert_eq!(2, broker.subscribers_count("channel1").await);
        assert_eq!(1, broker.subscribers_count("channel2").await);

        conn1.unsubscribe("channel1").await.unwrap();

        assert_eq!(1, broker.subscribers_count("channel1").await);

        drop(conn2);
        time::sleep(time::Duration::from_secs(1)).await;

        assert_eq!(0, broker.subscribers_count("channel1").await);
    }
}
