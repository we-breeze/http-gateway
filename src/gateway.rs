use std::future::Future;
use std::net::SocketAddr;

use http::{Request, StatusCode, Uri};
use hyper::body::Incoming;

use crate::config::RouteTable;
use crate::proxy::{FallbackProxy, GatewayResponse, ProxyConfigError, error_response};

/// Application service selected by configured route rules.
pub trait MatchedService: Clone + Send + Sync + 'static {
    fn call(
        &self,
        request: Request<Incoming>,
        peer_addr: SocketAddr,
    ) -> impl Future<Output = GatewayResponse> + Send;
}

/// Placeholder service useful while the configured route table is empty.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectMatched;

impl MatchedService for RejectMatched {
    async fn call(&self, _request: Request<Incoming>, _peer_addr: SocketAddr) -> GatewayResponse {
        error_response(
            StatusCode::NOT_IMPLEMENTED,
            "matched route has no service implementation",
        )
    }
}

/// A matched service that streams requests to another HTTP origin.
#[derive(Clone)]
pub struct OriginService {
    proxy: FallbackProxy,
}

impl OriginService {
    /// Creates a service backed by an origin-only HTTP URL.
    ///
    /// # Errors
    /// Returns an error when `origin` is not a supported origin URL.
    pub fn new(origin: &Uri) -> Result<Self, ProxyConfigError> {
        Ok(Self {
            proxy: FallbackProxy::new(origin)?,
        })
    }
}

impl MatchedService for OriginService {
    async fn call(&self, request: Request<Incoming>, peer_addr: SocketAddr) -> GatewayResponse {
        self.proxy.forward(request, peer_addr).await
    }
}

/// Dispatches configured routes to one service and all others to an HTTP origin.
#[derive(Clone)]
pub struct Gateway<S> {
    routes: RouteTable,
    matched: S,
    fallback: FallbackProxy,
}

impl<S> Gateway<S>
where
    S: MatchedService,
{
    /// Builds a gateway with a validated fallback origin URL.
    ///
    /// # Errors
    /// Returns an error when the fallback is not an origin-only HTTP URL.
    pub fn new(
        routes: RouteTable,
        matched: S,
        fallback_origin: &Uri,
    ) -> Result<Self, ProxyConfigError> {
        Ok(Self {
            routes,
            matched,
            fallback: FallbackProxy::new(fallback_origin)?,
        })
    }

    #[must_use]
    pub fn routes(&self) -> &RouteTable {
        &self.routes
    }

    pub async fn handle(
        &self,
        request: Request<Incoming>,
        peer_addr: SocketAddr,
    ) -> GatewayResponse {
        if self.routes.matches(request.method(), request.uri().path()) {
            self.matched.call(request, peer_addr).await
        } else {
            self.fallback.forward(request, peer_addr).await
        }
    }
}
