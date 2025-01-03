use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};

use async_stream::stream;
use futures::{stream::BoxStream, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{
    broadcast::{
        self,
        error::{RecvError, TryRecvError},
    },
    mpsc, Mutex,
};

use crate::{BoxError, Broker, Connection};

#[derive(Debug, Clone)]
pub struct MemoryBroker {
    broadcast: broadcast::Sender<(String, Vec<u8>)>,
    subscribers: Arc<Mutex<HashMap<String, usize>>>,
}

impl Default for MemoryBroker {
    fn default() -> Self {
        Self::with_capacity(1000)
    }
}

impl MemoryBroker {
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            broadcast: sender,
            subscribers: Default::default(),
        }
    }
}

impl Broker for MemoryBroker {
    type Conn = MemoryConnection;

    async fn connect(&self) -> Result<MemoryConnection, BoxError> {
        let sender = self.broadcast.clone();
        let receiver = sender.subscribe();
        let (events_tx, mut events_rx) = mpsc::unbounded_channel::<ConnectionEvent>();

        tokio::spawn({
            let subscribers = self.subscribers.clone();
            async move {
                while let Some(event) = events_rx.recv().await {
                    match event {
                        ConnectionEvent::Subscribe(channel) => {
                            subscribers
                                .lock()
                                .await
                                .entry(channel)
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                        ConnectionEvent::Unsubscribe(channel) => {
                            subscribers
                                .lock()
                                .await
                                .entry(channel)
                                .and_modify(|count| *count -= 1)
                                .or_default();
                        }
                    }
                }
            }
        });

        Ok(MemoryConnection {
            sender,
            receiver,
            events: events_tx,
            subs: HashSet::new(),
            user_id: None,
        })
    }

    async fn subscribers_count(&self, channel: &str) -> usize {
        self.subscribers
            .lock()
            .await
            .get(channel)
            .copied()
            .unwrap_or(0)
    }

    async fn subscriptions(&self) -> HashSet<(String, usize)> {
        self.subscribers
            .lock()
            .await
            .iter()
            .map(|(channel, count)| (channel.clone(), *count))
            .filter(|(_, count)| *count > 0)
            .collect()
    }

    async fn publish(&self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.broadcast
            .send((channel.to_owned(), serde_json::to_vec(&msg)?))?;
        Ok(())
    }

    fn all_messages<T: DeserializeOwned + Send + 'static>(&self) -> BoxStream<'static, T> {
        let mut msgs = self.broadcast.clone().subscribe();
        stream! {
            loop {
                match msgs.try_recv() {
                    Ok((_, msg)) => {
                        if let Ok(msg) = serde_json::from_slice(&msg) {
                            yield msg
                        }
                    }
                    Err(TryRecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone)]
enum ConnectionEvent {
    Subscribe(String),
    Unsubscribe(String),
}

#[derive(Debug)]
pub struct MemoryConnection {
    sender: broadcast::Sender<(String, Vec<u8>)>,
    receiver: broadcast::Receiver<(String, Vec<u8>)>,
    events: mpsc::UnboundedSender<ConnectionEvent>,
    subs: HashSet<String>,
    user_id: Option<String>,
}

impl Drop for MemoryConnection {
    fn drop(&mut self) {
        for channel in mem::take(&mut self.subs).into_iter() {
            self.events
                .send(ConnectionEvent::Unsubscribe(channel.to_owned()))
                .ok();
        }
    }
}

impl Connection for MemoryConnection {
    async fn authenticate(&mut self, user_id: &str, _data: impl Serialize) -> Result<(), BoxError> {
        match self.user_id.as_mut() {
            Some(current_user_id) if current_user_id != user_id => {
                Err("Connection already authenticated".into())
            }
            Some(current_user_id) => {
                *current_user_id = user_id.to_owned();
                Ok(())
            }
            None => {
                self.user_id = Some(user_id.to_string());
                Ok(())
            }
        }
    }

    async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.sender
            .send((channel.to_owned(), serde_json::to_vec(&msg)?))?;
        Ok(())
    }

    async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        if self.subs.insert(channel.to_owned()) {
            self.events
                .send(ConnectionEvent::Subscribe(channel.to_owned()))?;
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        if self.subs.remove(channel) {
            self.events
                .send(ConnectionEvent::Unsubscribe(channel.to_owned()))?;
        }
        Ok(())
    }

    async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, BoxError> {
        loop {
            match self.receiver.recv().await {
                Ok((channel, msg)) => match serde_json::from_slice(&msg) {
                    Ok(msg) if self.subs.contains(&channel) => return Ok(msg),
                    _ => continue,
                },
                Err(RecvError::Lagged(_)) => continue,
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn try_recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, BoxError> {
        loop {
            match self.receiver.try_recv() {
                Ok((channel, msg)) => match serde_json::from_slice(&msg) {
                    Ok(msg) if self.subs.contains(&channel) => return Ok(Some(msg)),
                    _ => return Ok(None),
                },
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Lagged(_)) => continue,
                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::time;

    use super::*;

    #[tokio::test]
    async fn test_pubsub() {
        let broker = MemoryBroker::default();
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
        let broker = MemoryBroker::default();
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
        let mut interval = time::interval(time::Duration::from_millis(1));
        let broker = MemoryBroker::default();
        let mut conn1 = broker.connect().await.unwrap();
        let mut conn2 = broker.connect().await.unwrap();

        conn1.subscribe("channel1").await.unwrap();
        conn1.subscribe("channel2").await.unwrap();
        conn2.subscribe("channel1").await.unwrap();
        interval.tick().await;

        assert_eq!(0, broker.subscribers_count("channel0").await);
        assert_eq!(2, broker.subscribers_count("channel1").await);
        assert_eq!(1, broker.subscribers_count("channel2").await);

        conn1.unsubscribe("channel1").await.unwrap();
        interval.tick().await;

        assert_eq!(1, broker.subscribers_count("channel1").await);
    }

    #[tokio::test]
    async fn test_subscriptions() {
        let mut interval = time::interval(time::Duration::from_millis(1));
        let broker = MemoryBroker::default();
        let mut conn1 = broker.connect().await.unwrap();
        let mut conn2 = broker.connect().await.unwrap();

        conn1.subscribe("channel1").await.unwrap();
        conn1.subscribe("channel2").await.unwrap();
        conn1.subscribe("channel3").await.unwrap();

        conn2.subscribe("channel1").await.unwrap();
        conn2.subscribe("channel3").await.unwrap();
        conn2.unsubscribe("channel3").await.unwrap();

        interval.tick().await;

        assert_eq!(
            HashSet::from_iter([
                (String::from("channel1"), 2),
                (String::from("channel2"), 1),
                (String::from("channel3"), 1)
            ]),
            broker.subscriptions().await
        );

        drop(conn1);
        interval.tick().await;

        assert_eq!(
            HashSet::from_iter([(String::from("channel1"), 1)]),
            broker.subscriptions().await
        );
    }
}
