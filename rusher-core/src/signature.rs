use std::fmt;

use hmac::{Hmac, Mac};
use http::Request;
use sha2::Sha256;
use url::Url;

use crate::{ChannelName, SocketId};

#[derive(Debug, Clone)]
pub struct Signature {
    mac: Hmac<Sha256>,
}

impl Signature {
    pub fn into_bytes(self) -> Vec<u8> {
        self.mac.finalize().into_bytes().to_vec()
    }

    pub fn verify(self, signature: impl AsRef<[u8]>) -> bool {
        self.mac.verify_slice(signature.as_ref()).is_ok()
    }
}

impl fmt::LowerHex for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.clone().into_bytes() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl fmt::UpperHex for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.clone().into_bytes() {
            write!(f, "{:02X}", byte)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    InvalidData,
    InvalidSecret,
}

pub fn sign_request<T>(
    secret: impl AsRef<[u8]>,
    req: &Request<T>,
) -> Result<Signature, SignatureError> {
    let method = req.method().to_string().to_uppercase();
    let path = req.uri().path();
    let query_params = {
        let url = Url::parse(&req.uri().to_string())
            // The url might be a relative one, so we try adding a dummy schema and host,
            // which is fine, since we only care about the query params at this point
            .or_else(|_| Url::parse(&format!("http://example{}", req.uri())))
            .map_err(|_| SignatureError::InvalidData)?;

        let mut params = url
            .query_pairs()
            .map(|(k, v)| (k.to_lowercase(), v))
            .filter(|(k, _)| k != "auth_signature")
            .collect::<Vec<_>>();

        params.sort_by_key(|(k, _)| k.clone());

        params
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    };

    sign_data(secret, format!("{method}\n{path}\n{query_params}"))
}

pub fn sign_private_channel(
    secret: impl AsRef<[u8]>,
    socket_id: &SocketId,
    channel: &ChannelName,
) -> Result<Signature, SignatureError> {
    sign_data(secret, format!("{socket_id}:{channel}"))
}

pub fn sign_user_data(
    secret: impl AsRef<[u8]>,
    socket_id: &SocketId,
    user_data: &str,
) -> Result<Signature, SignatureError> {
    sign_data(secret, format!("{socket_id}::user::{user_data}"))
}

fn sign_data(
    secret: impl AsRef<[u8]>,
    data: impl AsRef<[u8]>,
) -> Result<Signature, SignatureError> {
    hmac::Hmac::<Sha256>::new_from_slice(secret.as_ref())
        .map(|mac| mac.chain_update(data))
        .map(|mac| Signature { mac })
        .map_err(|_| SignatureError::InvalidSecret)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use http::method;

    use super::*;

    #[test]
    fn test_sign_request() {
        // Example took from the official docs:
        // https://pusher.com/docs/channels/library_auth_reference/rest-api/#worked-authentication-example

        let secret = "7ad3773142a6692b25b8";
        let auth_key = "278d425bdf160c739803";
        let auth_timestamp = "1353088179";
        let auth_version = "1.0";
        let body_md5 = "ec365a775a4cd0599faeb73354201b6f";
        let auth_signature = "da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c";

        let req = Request::builder()
            .method(method::Method::POST)
            .uri(format!("/apps/3/events?body_md5={body_md5}&auth_key={auth_key}&auth_version={auth_version}&auth_timestamp={auth_timestamp}"))
            .body(r##"{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}'"##)
            .unwrap();

        let signature = sign_request(secret, &req).unwrap();

        assert_eq!(auth_signature, format!("{:x}", signature));
        assert!(signature.verify(hex::decode(auth_signature).unwrap()));
    }

    #[test]
    fn test_sign_user() {
        // Example took from the official docs:
        // https://pusher.com/docs/channels/library_auth_reference/auth-signatures/#user-authentication

        let secret = "7ad3773142a6692b25b8";
        let data = r#"1234.1234::user::{"id":"12345"}"#;
        let auth_signature = "4708d583dada6a56435fb8bc611c77c359a31eebde13337c16ab43aa6de336ba";

        let signature = sign_data(secret, data).unwrap();

        assert_eq!(auth_signature, format!("{:x}", signature));
        assert!(signature.verify(hex::decode(auth_signature).unwrap()));
    }

    #[test]
    fn test_sign_channel() {
        // Example took from the official docs:
        // https://pusher.com/docs/channels/library_auth_reference/auth-signatures/#private-channel

        let secret = "7ad3773142a6692b25b8";
        let socket_id = SocketId(String::from("1234.1234"));
        let channel = ChannelName::from_str("private-foobar").unwrap();
        let auth_signature = "58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";

        let signature = sign_private_channel(secret, &socket_id, &channel).unwrap();

        assert_eq!(auth_signature, format!("{:x}", signature));
        assert!(signature.verify(hex::decode(auth_signature).unwrap()));
    }
}
