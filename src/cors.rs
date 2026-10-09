use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;

use crate::{Cors, GatewayResponse};

pub(crate) fn preflight(cors: &Cors, request: &Request<Incoming>) -> Option<GatewayResponse> {
    let response = cors.preflight(brz_http_cors::PreflightRequest {
        method: request.method().as_str(),
        origin: request
            .headers()
            .get("origin")
            .map(http::HeaderValue::as_bytes),
        request_method: request
            .headers()
            .get("access-control-request-method")
            .map(http::HeaderValue::as_bytes),
        request_headers: request
            .headers()
            .get("access-control-request-headers")
            .map(http::HeaderValue::as_bytes),
    })?;
    let mut result = Response::new(
        Full::new(Bytes::from_static(response.body))
            .map_err(|never| match never {})
            .boxed(),
    );
    *result.status_mut() = response.status;
    *result.headers_mut() = response.headers;
    Some(result)
}
