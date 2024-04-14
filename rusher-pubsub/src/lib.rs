use std::{collections::HashSet, error::Error};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
    Receiver, Sender,
};

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone)]
pub struct Broker {
    broadcast: Sender<(String, Vec<u8>)>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::with_capacity(1000)
    }
}

impl Broker {
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { broadcast: sender }
    }
}

impl Broker {
    pub async fn new_connection(&self) -> Result<Connection, BoxError> {
        let sender = self.broadcast.clone();
        let receiver = sender.subscribe();
        Ok(Connection {
            sender,
            receiver,
            subs: HashSet::new(),
        })
    }
}

#[derive(Debug)]
pub struct Connection {
    sender: Sender<(String, Vec<u8>)>,
    receiver: Receiver<(String, Vec<u8>)>,
    subs: HashSet<String>,
}

impl Connection {
    pub async fn publish(&mut self, channel: &str, msg: impl Serialize) -> Result<(), BoxError> {
        self.sender
            .send((channel.to_owned(), serde_json::to_vec(&msg)?))?;
        Ok(())
    }

    pub async fn subscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.subs.insert(channel.to_owned());
        Ok(())
    }

    pub async fn unsubscribe(&mut self, channel: &str) -> Result<(), BoxError> {
        self.subs.remove(channel);
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
}
