use std::fmt;

use hmac::{Hmac, Mac};
use http::Request;
use sha2::Sha256;
use url::Url;

#[derive(Debug, Clone)]
pub struct RequestSignature {
    mac: Hmac<Sha256>,
}

impl RequestSignature {
    pub fn into_bytes(self) -> Vec<u8> {
        self.mac.finalize().into_bytes().to_vec()
    }

    pub fn verify(self, signature: impl AsRef<[u8]>) -> bool {
        self.mac.verify_slice(signature.as_ref()).is_ok()
    }
}

impl fmt::LowerHex for RequestSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.clone().into_bytes() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl fmt::UpperHex for RequestSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.clone().into_bytes() {
            write!(f, "{:02X}", byte)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    InvalidUrl,
    InvalidSecret,
}

pub fn sign_request<T>(
    secret: &[u8],
    req: &Request<T>,
) -> Result<RequestSignature, SignatureError> {
    let method = req.method().to_string().to_uppercase();
    let path = req.uri().path();
    let query_params = {
        let url = Url::parse(&req.uri().to_string())
            // The url might be a relative one, so we try adding a dummy schema and host,
            // which is fine, since we only care about the query params at this point
            .or_else(|_| Url::parse(&format!("http://example{}", req.uri())))
            .map_err(|_| SignatureError::InvalidUrl)?;

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

    let signature = hmac::Hmac::<Sha256>::new_from_slice(secret)
        .map_err(|_| SignatureError::InvalidSecret)?
        .chain_update(format!("{method}\n{path}\n{query_params}").as_bytes());

    Ok(RequestSignature { mac: signature })
}

#[cfg(test)]
mod tests {
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

        let signature = sign_request(secret.as_bytes(), &req).unwrap();

        assert_eq!(auth_signature, format!("{:x}", signature));
        assert!(signature.verify(hex::decode(auth_signature).unwrap()));
    }
}
