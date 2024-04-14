use std::{
    collections::{HashMap, HashSet},
    error::Error,
    mem,
    sync::Arc,
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{
    broadcast::{
        self,
        error::{RecvError, TryRecvError},
    },
    mpsc, Mutex,
};

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone)]
pub struct Broker {
    broadcast: broadcast::Sender<(String, Vec<u8>)>,
    subscribers: Arc<Mutex<HashMap<String, usize>>>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::with_capacity(1000)
    }
}

impl Broker {
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            broadcast: sender,
            subscribers: Default::default(),
        }
    }
}

impl Broker {
    pub async fn new_connection(&self) -> Result<Connection, BoxError> {
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

        Ok(Connection {
            sender,
            receiver,
            events: events_tx,
            subs: HashSet::new(),
        })
    }

    pub async fn subscribers_count(&self, channel: &str) -> usize {
        self.subscribers
            .lock()
            .await
            .get(channel)
            .copied()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
enum ConnectionEvent {
    Subscribe(String),
    Unsubscribe(String),
}

#[derive(Debug)]
pub struct Connection {
    sender: broadcast::Sender<(String, Vec<u8>)>,
    receiver: broadcast::Receiver<(String, Vec<u8>)>,
    events: mpsc::UnboundedSender<ConnectionEvent>,
    subs: HashSet<String>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        for channel in mem::take(&mut self.subs).into_iter() {
            self.events
                .send(ConnectionEvent::Unsubscribe(channel.to_owned()))
                .ok();
        }
    }
}

impl Connection {
    pub async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.sender
            .send((channel.to_owned(), serde_json::to_vec(&msg)?))?;
        Ok(())
    }

    pub async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.subs.insert(channel.to_owned());
        self.events
            .send(ConnectionEvent::Subscribe(channel.to_owned()))?;
        Ok(())
    }

    pub async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.subs.remove(channel);
        self.events
            .send(ConnectionEvent::Unsubscribe(channel.to_owned()))?;
        Ok(())
    }

    pub async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, BoxError> {
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

    pub async fn try_recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, BoxError> {
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
        let broker = Broker::default();
        let mut conn1 = broker.new_connection().await.unwrap();
        let mut conn2 = broker.new_connection().await.unwrap();
        let mut conn3 = broker.new_connection().await.unwrap();

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
        let broker = Broker::default();
        let mut conn1 = broker.new_connection().await.unwrap();
        let mut conn2 = broker.new_connection().await.unwrap();

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
        let broker = Broker::default();
        let mut conn1 = broker.new_connection().await.unwrap();
        let mut conn2 = broker.new_connection().await.unwrap();

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
}
