//! A route-selective HTTP/1.1 gateway with a streaming fallback origin.
//!
//! The gateway is application-independent. A typed `RouteTable` selects
//! requests for a caller-provided `MatchedService`; unmatched requests are
//! proxied with streaming bodies, repeated headers, cancellation/backpressure,
//! and HTTP upgrades preserved. An empty route table forwards every request
//! unless `Gateway::with_global_recording` enables global recording.

mod admission;
#[cfg(feature = "route-config")]
mod config;
#[cfg(feature = "http-gateway")]
mod cors;
#[cfg(feature = "http-gateway")]
mod gateway;
#[cfg(feature = "http-gateway")]
mod proxy;
#[cfg(feature = "redis-recording")]
pub mod redis_admission;
#[cfg(feature = "http-gateway")]
mod server;

pub use admission::{
    ADMISSION_SCOPE_HEADER, ADMISSION_TOKEN_HEADER, AcquireOutcome, AdmissionError,
    AdmissionFuture, AdmissionProvider, AdmissionRegistry, AdmissionTicket, RegistrationError,
};
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
#[cfg(feature = "http-gateway")]
pub use brz_http_cors::{Cors, ResponseCors};
#[cfg(feature = "route-config")]
pub use config::{AdmissionRule, ConfigError, ExclusionRule, RouteRule, RouteTable, RoutesConfig};
#[cfg(feature = "http-gateway")]
pub use gateway::{Gateway, GatewayBuildError, MatchedService, OriginService, RejectMatched};
#[cfg(feature = "http-gateway")]
pub use proxy::{GatewayBody, GatewayResponse, ProxyConfigError};
#[cfg(feature = "http-gateway")]
pub use server::{GatewayConfig, bind, serve, serve_with_config};
