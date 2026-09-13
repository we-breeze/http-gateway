use std::convert::Infallible;
use std::error::Error;
use std::net::SocketAddr;

use bytes::Bytes;
use http::header::{CONNECTION, HOST, HeaderMap, HeaderName, HeaderValue, UPGRADE};
use http::uri::{Authority, Scheme};
use http::{Request, Response, StatusCode, Uri};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::upgrade::OnUpgrade;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::copy_bidirectional;
use tracing::debug;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

pub type BoxError = Box<dyn Error + Send + Sync>;
pub type GatewayBody = BoxBody<Bytes, BoxError>;
pub type GatewayResponse = Response<GatewayBody>;

#[derive(Clone)]
pub(crate) struct FallbackProxy {
    client: Client<HttpConnector, Incoming>,
    scheme: Scheme,
    authority: Authority,
}

impl FallbackProxy {
    pub(crate) fn new(upstream: &Uri) -> Result<Self, ProxyConfigError> {
        let scheme = upstream
            .scheme()
            .cloned()
            .ok_or(ProxyConfigError::MissingScheme)?;
        if scheme != Scheme::HTTP {
            return Err(ProxyConfigError::UnsupportedScheme(scheme.to_string()));
        }
        let authority = upstream
            .authority()
            .cloned()
            .ok_or(ProxyConfigError::MissingAuthority)?;
        if upstream.path() != "/" || upstream.query().is_some() {
            return Err(ProxyConfigError::UnexpectedPath);
        }
        let client = Client::builder(TokioExecutor::new()).build_http();
        Ok(Self {
            client,
            scheme,
            authority,
        })
    }

    pub(crate) async fn forward(
        &self,
        mut request: Request<Incoming>,
        peer_addr: SocketAddr,
    ) -> GatewayResponse {
        let upgrade = is_upgrade(request.headers());
        let downstream_upgrade = upgrade.then(|| hyper::upgrade::on(&mut request));
        if let Err(error) = self.prepare_request(&mut request, peer_addr, upgrade) {
            return error_response(StatusCode::BAD_GATEWAY, error.to_string());
        }

        let mut response = match self.client.request(request).await {
            Ok(response) => response,
            Err(error) => {
                debug!(%error, "fallback upstream request failed");
                return error_response(StatusCode::BAD_GATEWAY, "fallback upstream unavailable");
            }
        };

        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            if let Some(downstream_upgrade) = downstream_upgrade {
                let upstream_upgrade = hyper::upgrade::on(&mut response);
                tokio::spawn(tunnel(downstream_upgrade, upstream_upgrade));
            }
        } else {
            remove_hop_by_hop_headers(response.headers_mut(), false);
        }
        response.map(|body| body.map_err(|error| Box::new(error) as BoxError).boxed())
    }

    fn prepare_request(
        &self,
        request: &mut Request<Incoming>,
        peer_addr: SocketAddr,
        upgrade: bool,
    ) -> Result<(), http::Error> {
        let path_and_query = request
            .uri()
            .path_and_query()
            .map_or("/", http::uri::PathAndQuery::as_str);
        *request.uri_mut() = Uri::builder()
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .path_and_query(path_and_query)
            .build()?;

        let headers = request.headers_mut();
        let original_host = headers.get(HOST).cloned();
        remove_hop_by_hop_headers(headers, upgrade);
        append_forwarded_for(headers, peer_addr.ip());
        headers
            .entry(X_FORWARDED_PROTO)
            .or_insert(HeaderValue::from_static("http"));
        if let Some(host) = original_host {
            headers.entry(X_FORWARDED_HOST).or_insert(host);
        }
        Ok(())
    }
}

fn append_forwarded_for(headers: &mut HeaderMap, address: std::net::IpAddr) {
    let address = address.to_string();
    if let Some(existing) = headers.get(&X_FORWARDED_FOR) {
        let Ok(existing) = existing.to_str() else {
            headers.remove(&X_FORWARDED_FOR);
            if let Ok(value) = HeaderValue::from_str(&address) {
                headers.insert(X_FORWARDED_FOR, value);
            }
            return;
        };
        let combined = format!("{existing}, {address}");
        if let Ok(value) = HeaderValue::from_str(&combined) {
            headers.insert(X_FORWARDED_FOR, value);
        }
    } else if let Ok(value) = HeaderValue::from_str(&address) {
        headers.insert(X_FORWARDED_FOR, value);
    }
}

fn is_upgrade(headers: &HeaderMap) -> bool {
    headers.contains_key(UPGRADE)
        && headers
            .get_all(CONNECTION)
            .iter()
            .any(|value| contains_token(value.as_bytes(), b"upgrade"))
}

fn remove_hop_by_hop_headers(headers: &mut HeaderMap, preserve_upgrade: bool) {
    let nominated = headers
        .get_all(CONNECTION)
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .filter_map(|name| HeaderName::from_bytes(trim_ascii(name)).ok())
        .collect::<Vec<_>>();
    for name in nominated {
        if !preserve_upgrade || name != UPGRADE {
            headers.remove(name);
        }
    }
    for name in [
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
    ] {
        headers.remove(name);
    }
    if !preserve_upgrade {
        headers.remove(CONNECTION);
        headers.remove(UPGRADE);
    }
}

fn contains_token(value: &[u8], expected: &[u8]) -> bool {
    value
        .split(|byte| *byte == b',')
        .any(|token| trim_ascii(token).eq_ignore_ascii_case(expected))
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while matches!(bytes.first(), Some(b' ' | b'\t')) {
        bytes = &bytes[1..];
    }
    while matches!(bytes.last(), Some(b' ' | b'\t')) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

async fn tunnel(downstream: OnUpgrade, upstream: OnUpgrade) {
    let (Ok(downstream), Ok(upstream)) = tokio::join!(downstream, upstream) else {
        return;
    };
    let mut downstream = TokioIo::new(downstream);
    let mut upstream = TokioIo::new(upstream);
    if let Err(error) = copy_bidirectional(&mut downstream, &mut upstream).await {
        debug!(%error, "upgraded fallback tunnel closed with an error");
    }
}

pub(crate) fn error_response(status: StatusCode, message: impl Into<Bytes>) -> GatewayResponse {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(
            Full::new(message.into())
                .map_err(|never: Infallible| match never {})
                .boxed(),
        )
        .expect("static gateway error response must be valid")
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyConfigError {
    #[error("fallback upstream URL must include a scheme")]
    MissingScheme,
    #[error("fallback upstream URL must include an authority")]
    MissingAuthority,
    #[error("fallback upstream scheme is not supported: {0}")]
    UnsupportedScheme(String),
    #[error("fallback upstream URL must not include a path or query")]
    UnexpectedPath,
}
