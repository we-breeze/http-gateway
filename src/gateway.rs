use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use http::{Request, StatusCode, Uri};
use hyper::body::Incoming;

use crate::admission::{
    ADMISSION_SCOPE_HEADER, ADMISSION_TOKEN_HEADER, AcquireOutcome, AdmissionError,
    AdmissionRegistry,
};
use crate::config::RouteTable;
use crate::proxy::{
    FallbackProxy, GatewayResponse, ProxyConfigError, RecordingFallback, error_response,
};

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
    fn call(
        &self,
        _request: Request<Incoming>,
        _peer_addr: SocketAddr,
    ) -> impl Future<Output = GatewayResponse> + Send {
        std::future::ready(error_response(
            StatusCode::NOT_IMPLEMENTED,
            "matched route has no service implementation",
        ))
    }
}

/// A matched service that streams requests to another HTTP origin.
#[derive(Clone)]
pub struct OriginService {
    proxy: FallbackProxy,
}

impl OriginService {
    /// Creates a service whose target must be supplied by an admission ticket.
    #[must_use]
    pub fn from_admission() -> Self {
        Self {
            proxy: FallbackProxy::from_admission(),
        }
    }
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
        self.proxy.forward_recording(request, peer_addr).await
    }
}

/// Dispatches configured routes to one service and all others to an HTTP origin.
#[derive(Clone)]
pub struct Gateway<S> {
    routes: RouteTable,
    matched: S,
    fallback: FallbackProxy,
    admission: AdmissionRegistry,
}

impl<S> Gateway<S>
where
    S: MatchedService,
{
    /// Builds a gateway with a validated fallback origin URL.
    ///
    /// # Errors
    /// Returns an error when the fallback is not an origin-only HTTP URL or
    /// routes reference an admission provider (use `with_admission` to register).
    pub fn new(
        routes: RouteTable,
        matched: S,
        fallback_origin: &Uri,
    ) -> Result<Self, GatewayBuildError> {
        Self::with_admission(routes, matched, fallback_origin, AdmissionRegistry::new())
    }

    /// Builds a gateway after validating every configured provider reference.
    /// Unknown providers are construction errors, including with `new`.
    pub fn with_admission(
        routes: RouteTable,
        matched: S,
        fallback_origin: &Uri,
        admission: AdmissionRegistry,
    ) -> Result<Self, GatewayBuildError> {
        for (route, rule) in routes.admissions().iter().enumerate() {
            if let Some(rule) = rule {
                if admission.get(&rule.provider).is_none() {
                    return Err(GatewayBuildError::MissingProvider {
                        route,
                        provider: rule.provider.clone(),
                    });
                }
            }
        }
        Ok(Self {
            routes,
            matched,
            fallback: FallbackProxy::new(fallback_origin)?,
            admission,
        })
    }

    #[must_use]
    pub fn routes(&self) -> &RouteTable {
        &self.routes
    }

    pub async fn handle(
        &self,
        mut request: Request<Incoming>,
        peer_addr: SocketAddr,
    ) -> GatewayResponse {
        request.headers_mut().remove(ADMISSION_SCOPE_HEADER);
        request.headers_mut().remove(ADMISSION_TOKEN_HEADER);
        request.extensions_mut().remove::<crate::AdmissionTicket>();
        let selected = self.routes.select(request.method(), request.uri().path());
        let mut matched = selected.is_some();
        if let Some(rule) = selected.and_then(|id| self.routes.admissions()[id].as_ref()) {
            let provider = self
                .admission
                .get(&rule.provider)
                .expect("configured providers were validated at construction");
            let result = tokio::time::timeout(
                Duration::from_millis(rule.acquire_timeout_ms),
                provider.try_acquire(&rule.scope),
            )
            .await
            .unwrap_or(Err(AdmissionError::Timeout));
            match result {
                Ok(AcquireOutcome::Acquired(ticket)) if ticket.scope() == rule.scope => {
                    ticket.attach(&mut request);
                    request
                        .extensions_mut()
                        .insert(RecordingFallback(self.fallback.clone()));
                }
                Ok(
                    AcquireOutcome::Busy
                    | AcquireOutcome::NoRecorder
                    | AcquireOutcome::NotParticipant,
                ) => matched = false,
                outcome => {
                    let error = match outcome {
                        Err(error) => error,
                        _ => AdmissionError::InvalidTicket,
                    };
                    tracing::warn!(provider = %rule.provider, scope = %rule.scope, %error,
                        "request admission failed before dispatch; using fallback");
                    matched = false;
                }
            }
        }
        #[cfg(feature = "fallback-log")]
        let fallback_log = (!matched).then(|| {
            let target = request
                .uri()
                .path_and_query()
                .map_or("/", http::uri::PathAndQuery::as_str)
                .to_owned();
            let request_len = request
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            (
                std::time::Instant::now(),
                request.method().clone(),
                target,
                request_len,
            )
        });
        let response = if matched {
            self.matched.call(request, peer_addr).await
        } else {
            self.fallback.forward(request, peer_addr).await
        };
        #[cfg(feature = "fallback-log")]
        if let Some((started, method, target, request_len)) = fallback_log {
            let response_len = response
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            tracing::info!(
                target: "breeze.fallback",
                "{} {} {} {}ms {} {}",
                method,
                target,
                response.status().as_u16(),
                started.elapsed().as_millis(),
                OptionalLength(request_len),
                OptionalLength(response_len),
            );
        }
        response
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayBuildError {
    #[error(transparent)]
    Proxy(#[from] ProxyConfigError),
    #[error("route {route} references an unregistered admission provider: {provider}")]
    MissingProvider { route: usize, provider: String },
}

#[cfg(feature = "fallback-log")]
struct OptionalLength(Option<u64>);

#[cfg(feature = "fallback-log")]
impl std::fmt::Display for OptionalLength {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(length) => length.fmt(formatter),
            None => formatter.write_str("-"),
        }
    }
}
