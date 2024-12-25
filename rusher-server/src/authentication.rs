use axum::{
    body::Body,
    extract::{Query, Request},
    http::StatusCode,
    middleware::Next,
    response::Response,
    Extension, Json,
};
use futures::TryStreamExt;
use rusher_core::signature::{sign_request, SignatureError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::AppSecret;

#[derive(Debug, Deserialize)]
pub struct Params {
    auth_signature: String,
    body_md5: Option<String>,
}

pub async fn check_signature_middleware(
    Extension(AppSecret(secret)): Extension<AppSecret>,
    Query(Params {
        auth_signature,
        body_md5,
    }): Query<Params>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let signature = sign_request(secret.as_bytes(), &req).map_err(|err| match err {
        SignatureError::InvalidData => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "Invalid request" })),
        ),
        SignatureError::InvalidSecret => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": "Invalid app secret" })),
        ),
    })?;

    let (parts, body) = req.into_parts();

    // TODO: we have to limit the body size, otherwise we can run out of memory while buffering
    let body = body
        .into_data_stream()
        .map_ok(|data| data.to_vec())
        .try_concat()
        .await
        .map_err(|_err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": "Failed to read body" })),
            )
        })?;

    if !body.is_empty() {
        let body_md5 = body_md5
            .and_then(|body_md5| hex::decode(body_md5).ok())
            .ok_or((
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "missing body_md5" })),
            ))?;

        if *body_md5 != *md5::compute(&body) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "body_md5 does not match" })),
            ));
        }
    }

    match hex::decode(auth_signature) {
        Ok(ref sent_signature) if signature.verify(sent_signature) => {
            Ok(next.run(Request::from_parts(parts, Body::from(body))).await)
        }
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "ok": false, "error": "Invalid auth_signature" })),
        )),
    }
}
