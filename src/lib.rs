//! A route-selective HTTP/1.1 gateway with a streaming fallback origin.
//!
//! The gateway is application-independent. A typed [`RouteTable`] selects
//! requests for a caller-provided [`MatchedService`]; unmatched requests are
//! proxied with streaming bodies, repeated headers, cancellation/backpressure,
//! and HTTP upgrades preserved. An empty route table forwards every request.

mod config;
mod gateway;
mod proxy;
mod server;

pub use config::{ConfigError, RouteRule, RouteTable, RoutesConfig};
pub use gateway::{Gateway, MatchedService, OriginService, RejectMatched};
pub use proxy::{BoxError, GatewayBody, GatewayResponse, ProxyConfigError};
pub use server::{GatewayConfig, bind, serve, serve_with_config};
