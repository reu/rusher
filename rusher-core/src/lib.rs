use std::{collections::HashMap, fmt::Display, str::FromStr};

use rand::{distributions, prelude::Distribution};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SocketId(String);

impl Distribution<SocketId> for distributions::Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> SocketId {
        let digits = distributions::Uniform::from(0..=9)
            .sample_iter(rng)
            .take(32)
            .map(|s| s.to_string())
            .collect::<String>();
        let (p1, p2) = digits.split_at(16);
        SocketId(format!("{p1}.{p2}"))
    }
}

#[derive(Debug, Clone)]
pub enum SocketIdParseError {
    InvalidSocketId,
}

impl FromStr for SocketId {
    type Err = SocketIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.find('.') {
            Some(index) if index > 0 && index < s.len() => Ok(SocketId(s.to_owned())),
            _ => Err(SocketIdParseError::InvalidSocketId),
        }
    }
}

impl AsRef<str> for SocketId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChannelName {
    Public(String),
    Private(String),
    Presence(String),
    Encrypted(String),
}

impl AsRef<str> for ChannelName {
    fn as_ref(&self) -> &str {
        match self {
            ChannelName::Public(ref name) => name,
            ChannelName::Private(ref name) => name,
            ChannelName::Presence(ref name) => name,
            ChannelName::Encrypted(ref name) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelNameParseError {
    InvalidChannelName,
}

impl Display for ChannelNameParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelNameParseError::InvalidChannelName => f.write_str("Invalid channel name"),
        }
    }
}

impl FromStr for ChannelName {
    type Err = ChannelNameParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.splitn(3, '-').collect::<Vec<&str>>().as_slice() {
            ["private", "encrypted", name, ..] if !name.is_empty() => {
                Ok(ChannelName::Encrypted(s.to_owned()))
            }
            ["private", name, ..] if !name.is_empty() => Ok(ChannelName::Private(s.to_owned())),
            ["presence", name, ..] if !name.is_empty() => Ok(ChannelName::Presence(s.to_owned())),
            _ if !s.is_empty() => Ok(ChannelName::Public(s.to_owned())),
            _ => Err(ChannelNameParseError::InvalidChannelName),
        }
    }
}

impl Display for ChannelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelName::Public(channel) => f.write_str(channel),
            ChannelName::Private(channel) => f.write_str(channel),
            ChannelName::Presence(channel) => f.write_str(channel),
            ChannelName::Encrypted(channel) => f.write_str(channel),
        }
    }
}

impl Serialize for ChannelName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for ChannelName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        FromStr::from_str(&name).map_err(de::Error::custom)
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(from = "ClientEventJson")]
pub enum ClientEvent {
    Subscribe {
        channel: ChannelName,
        auth: Option<String>,
        channel_data: Option<serde_json::Value>,
    },
    Unsubscribe {
        channel: ChannelName,
    },
    Ping,
    ChannelEvent {
        event: String,
        channel: ChannelName,
        data: serde_json::Value,
    },
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(tag = "event", content = "data")]
enum PusherClientEvent {
    #[serde(rename = "pusher:subscribe")]
    Subscribe {
        channel: ChannelName,
        auth: Option<String>,
        channel_data: Option<serde_json::Value>,
    },
    #[serde(rename = "pusher:unsubscribe")]
    Unsubscribe { channel: ChannelName },
    #[serde(rename = "pusher:ping")]
    Ping,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomClientEvent {
    event: String,
    channel: ChannelName,
    data: serde_json::Value,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(untagged)]
enum ClientEventJson {
    PusherEvent(PusherClientEvent),
    CustomEvent(CustomClientEvent),
}

impl From<ClientEventJson> for ClientEvent {
    fn from(json: ClientEventJson) -> Self {
        use ClientEventJson::*;
        use PusherClientEvent::*;
        match json {
            PusherEvent(Subscribe {
                channel,
                auth,
                channel_data,
            }) => ClientEvent::Subscribe {
                channel,
                auth,
                channel_data,
            },
            PusherEvent(Unsubscribe { channel }) => ClientEvent::Unsubscribe { channel },
            PusherEvent(Ping) => ClientEvent::Ping,
            CustomEvent(CustomClientEvent {
                event,
                channel,
                data,
            }) => ClientEvent::ChannelEvent {
                event,
                channel,
                data,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PresenceInformation {
    ids: Vec<String>,
    hash: HashMap<String, HashMap<String, String>>,
    count: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PresenceUser {
    #[serde(rename = "user_id")]
    id: String,
    #[serde(rename = "user_info")]
    info: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemovedMember {
    #[serde(rename = "user_id")]
    id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomEvent {
    pub event: String,
    pub channel: ChannelName,
    #[serde(with = "json_string")]
    pub data: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub socket_id: SocketId,
    pub activity_timeout: u8,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(from = "ServerEventJson", into = "ServerEventJson")]
pub enum ServerEvent {
    #[serde(rename = "pusher:connection_established")]
    ConnectionEstablished {
        #[serde(with = "json_string")]
        data: ConnectionInfo,
    },

    #[serde(rename = "pusher:error")]
    Error {
        message: String,
        code: Option<u16>,
    },

    #[serde(rename = "pusher:pong")]
    Pong,

    #[serde(rename = "pusher_internal:subscription_succeeded")]
    SubscriptionSucceeded {
        channel: ChannelName,
        #[serde(with = "json_string")]
        data: Option<PresenceInformation>,
    },

    #[serde(rename = "pusher_internal:member_added")]
    MemberAdded {
        channel: ChannelName,
        #[serde(with = "json_string")]
        data: PresenceUser,
    },

    #[serde(rename = "pusher_internal:member_removed")]
    MemberRemoved {
        channel: ChannelName,
        #[serde(with = "json_string")]
        data: RemovedMember,
    },

    ChannelEvent(CustomEvent),
}

impl From<ServerEventJson> for ServerEvent {
    fn from(json: ServerEventJson) -> Self {
        use PusherServerEvent::*;
        use ServerEventJson::*;
        match json {
            PusherEvent(ConnectionEstablished { data }) => {
                ServerEvent::ConnectionEstablished { data }
            }
            PusherEvent(Error { message, code }) => ServerEvent::Error { message, code },
            PusherEvent(Pong) => ServerEvent::Pong,
            PusherEvent(SubscriptionSucceeded { channel, data }) => {
                ServerEvent::SubscriptionSucceeded { channel, data }
            }
            PusherEvent(MemberAdded { channel, data }) => {
                ServerEvent::MemberAdded { channel, data }
            }
            PusherEvent(MemberRemoved { channel, data }) => {
                ServerEvent::MemberRemoved { channel, data }
            }
            UserEvent(event) => ServerEvent::ChannelEvent(event),
        }
    }
}

impl From<ServerEvent> for ServerEventJson {
    fn from(value: ServerEvent) -> Self {
        use ServerEvent::*;
        use ServerEventJson::*;
        match value {
            ConnectionEstablished { data } => {
                PusherEvent(PusherServerEvent::ConnectionEstablished { data })
            }
            Error { message, code } => PusherEvent(PusherServerEvent::Error { message, code }),
            Pong => PusherEvent(PusherServerEvent::Pong),
            SubscriptionSucceeded { channel, data } => {
                PusherEvent(PusherServerEvent::SubscriptionSucceeded { channel, data })
            }
            MemberRemoved { channel, data } => {
                PusherEvent(PusherServerEvent::MemberRemoved { channel, data })
            }
            MemberAdded { channel, data } => {
                PusherEvent(PusherServerEvent::MemberAdded { channel, data })
            }
            ChannelEvent(event) => UserEvent(event),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum ServerEventJson {
    PusherEvent(PusherServerEvent),
    UserEvent(CustomEvent),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum PusherServerEvent {
    #[serde(rename = "pusher:connection_established")]
    ConnectionEstablished {
        #[serde(with = "json_string")]
        data: ConnectionInfo,
    },

    #[serde(rename = "pusher:error")]
    Error { message: String, code: Option<u16> },

    #[serde(rename = "pusher:pong")]
    Pong,

    #[serde(rename = "pusher_internal:subscription_succeeded")]
    SubscriptionSucceeded {
        channel: ChannelName,
        #[serde(with = "json_string")]
        data: Option<PresenceInformation>,
    },

    #[serde(rename = "pusher_internal:member_added")]
    MemberAdded {
        channel: ChannelName,
        #[serde(with = "json_string")]
        data: PresenceUser,
    },

    #[serde(rename = "pusher_internal:member_removed")]
    MemberRemoved {
        channel: ChannelName,
        #[serde(with = "json_string")]
        data: RemovedMember,
    },
}

mod json_string {
    use serde::{
        de::{self, DeserializeOwned},
        ser::{self, Serialize, Serializer},
        Deserialize, Deserializer,
    };

    pub fn serialize<T: Serialize, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let json = serde_json::to_string(value).map_err(ser::Error::custom)?;
        json.serialize(serializer)
    }

    pub fn deserialize<'de, T: DeserializeOwned, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        let json = String::deserialize(deserializer)?;
        serde_json::from_str(&json).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_channel_name() {
        assert_eq!(Ok(ChannelName::Public("lol".to_owned())), "lol".parse());
        assert_eq!(
            Ok(ChannelName::Private("private-lol".to_owned())),
            "private-lol".parse()
        );
        assert_eq!(
            Ok(ChannelName::Presence("presence-lol".to_owned())),
            "presence-lol".parse()
        );
        assert_eq!(
            Ok(ChannelName::Encrypted("private-encrypted-lol".to_owned())),
            "private-encrypted-lol".parse()
        );
        assert_eq!(
            Err(ChannelNameParseError::InvalidChannelName),
            "".parse::<ChannelName>()
        );
    }

    #[test]
    fn test_member_removed() {
        let event = ServerEvent::MemberRemoved {
            channel: "channel".parse().unwrap(),
            data: RemovedMember {
                id: "lolwut".to_owned(),
            },
        };

        let serialized = serde_json::to_value(&event).unwrap();

        let expected = json!({
            "event": "pusher_internal:member_removed",
            "channel": "channel",
            "data": r#"{"user_id":"lolwut"}"#,
        });

        assert_eq!(expected, serialized);

        let deserialized = serde_json::from_value(expected).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_custom_event() {
        let event = ServerEvent::ChannelEvent(CustomEvent {
            event: "client-message".to_owned(),
            channel: "channel".parse().unwrap(),
            data: json!({ "some": "data" }),
            user_id: Some("user".to_owned()),
        });

        let serialized = serde_json::to_value(&event).unwrap();

        let expected = json!({
            "event": "client-message",
            "channel": "channel",
            "data": r#"{"some":"data"}"#,
            "user_id": "user",
        });

        assert_eq!(expected, serialized);

        let deserialized = serde_json::from_value(expected).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_deserialize_ping() {
        let event = ClientEvent::Ping;
        let serialized = json!({ "event": "pusher:ping" });
        let deserialized = serde_json::from_value::<ClientEvent>(serialized).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_deserialize_subscribe() {
        let event = ClientEvent::Subscribe {
            channel: "lolwut".parse().unwrap(),
            auth: None,
            channel_data: Some(json!({ "lol": "wut" })),
        };
        let serialized = json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "lolwut",
                "channel_data": { "lol": "wut" },
            },
        });
        let deserialized = serde_json::from_value::<ClientEvent>(serialized).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_deserialize_unsubscribe() {
        let event = ClientEvent::Unsubscribe {
            channel: "lolwut".parse().unwrap(),
        };
        let serialized = json!({
            "event": "pusher:unsubscribe",
            "data": {
                "channel": "lolwut",
            },
        });
        let deserialized = serde_json::from_value::<ClientEvent>(serialized).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_deserialize_channel_event() {
        let event = ClientEvent::ChannelEvent {
            event: "client-lolwut".to_owned(),
            channel: "lolwut".parse().unwrap(),
            data: json!({ "lol": "wut" }),
        };
        let serialized = json!({
            "event": "client-lolwut",
            "channel": "lolwut",
            "data": { "lol": "wut" },
        });
        let deserialized = serde_json::from_value::<ClientEvent>(serialized).unwrap();
        assert_eq!(event, deserialized);
    }
}
