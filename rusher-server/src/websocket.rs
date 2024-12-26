use std::error::Error;

use rusher_core::{
    signature::{sign_private_channel, sign_user_data},
    ChannelName, ClientEvent, ServerEvent, SocketId, UserData,
};
use rusher_pubsub::Connection;
use tokio::sync::mpsc::Sender;

pub struct ConnectionProtocol {
    pub tx: Sender<ServerEvent>,
    pub socket_id: SocketId,
    pub app_id: String,
    pub secret: String,
    pub current_user_id: Option<String>,
}

impl ConnectionProtocol {
    pub async fn handle_message(
        &mut self,
        connection: &mut impl Connection,
        msg: ClientEvent,
    ) -> anyhow::Result<(), Box<dyn Error + Send + Sync>> {
        let tx = &self.tx;
        match msg {
            ClientEvent::Signin { auth, user_data } => {
                let (sent_id, auth) = auth.split_once(":").unwrap_or_default();

                let valid_signature = sign_user_data(&self.secret, &self.socket_id, &user_data)
                    .map(|signature| signature.verify(hex::decode(auth).unwrap_or_default()))
                    .unwrap_or(false);

                if self.app_id != sent_id || !valid_signature {
                    tx.send(ServerEvent::invalid_signature_error()).await?;
                    return Ok(());
                }

                let user = serde_json::from_str::<UserData>(&user_data).unwrap();

                if connection.authenticate(&user.id, &user).await.is_err() {
                    tx.send(ServerEvent::authentication_error(
                        "Failed to authenticate user",
                    ))
                    .await?;
                    return Ok(());
                }

                self.current_user_id = Some(user.id.clone());

                tx.send(ServerEvent::signin_succeeded(user)).await?;

                Ok(())
            }

            ClientEvent::Ping => {
                tx.send(ServerEvent::Pong).await?;
                Ok(())
            }

            ClientEvent::Subscribe { channel, auth, .. } => {
                match channel {
                    ref channel @ ChannelName::Private(_) => {
                        let (sent_id, auth) = auth
                            .as_ref()
                            .and_then(|auth| auth.split_once(':'))
                            .unwrap_or_default();

                        let valid_signature =
                            sign_private_channel(&self.secret, &self.socket_id, channel)
                                .map(|signature| {
                                    signature.verify(hex::decode(auth).unwrap_or_default())
                                })
                                .unwrap_or(false);

                        if self.app_id != sent_id || !valid_signature {
                            tx.send(ServerEvent::invalid_signature_error()).await?;
                            return Ok(());
                        }
                    }
                    ChannelName::Presence(_) => {
                        tx.send(ServerEvent::error(
                            "Presence channels are not supported",
                            None,
                        ))
                        .await?;
                        return Ok(());
                    }
                    ChannelName::Encrypted(_) => {
                        tx.send(ServerEvent::error(
                            "Encrypted channels are not supported",
                            None,
                        ))
                        .await?;
                        return Ok(());
                    }
                    _ => {}
                };

                connection.subscribe(channel.as_ref()).await?;

                tx.send(ServerEvent::subscription_succeeded(channel))
                    .await?;

                Ok(())
            }

            ClientEvent::Unsubscribe { channel } => connection.unsubscribe(channel.as_ref()).await,

            ClientEvent::ChannelEvent {
                event,
                channel,
                data,
            } => {
                connection
                    .publish(
                        channel.as_ref(),
                        ServerEvent::custom_event(
                            event,
                            channel.clone(),
                            data,
                            self.current_user_id.clone(),
                        ),
                    )
                    .await
            }
        }
    }
}
