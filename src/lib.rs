//! A route-selective HTTP/1.1 gateway with a streaming fallback origin.
//!
//! The gateway is application-independent. A typed [`RouteTable`] selects
//! requests for a caller-provided [`MatchedService`]; unmatched requests are
//! proxied with streaming bodies, repeated headers, cancellation/backpressure,
//! and HTTP upgrades preserved. An empty route table forwards every request.

mod admission;
mod config;
mod gateway;
mod proxy;
#[cfg(feature = "redis-recording")]
pub mod redis_admission;
mod server;

pub use admission::{
    ADMISSION_SCOPE_HEADER, ADMISSION_TOKEN_HEADER, AcquireOutcome, AdmissionError,
    AdmissionFuture, AdmissionProvider, AdmissionRegistry, AdmissionTicket, RegistrationError,
};
pub use config::{AdmissionRule, ConfigError, ExclusionRule, RouteRule, RouteTable, RoutesConfig};
pub use gateway::{Gateway, GatewayBuildError, MatchedService, OriginService, RejectMatched};
pub use proxy::{BoxError, GatewayBody, GatewayResponse, ProxyConfigError};
pub use server::{GatewayConfig, bind, serve, serve_with_config};
