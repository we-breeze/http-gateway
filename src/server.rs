use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::service::service_fn;
use tokio::net::TcpListener;

use crate::{Gateway, MatchedService};

const MIN_HTTP1_BUFFER_SIZE: usize = 8 * 1024;

/// Runtime limits and socket policy for a gateway listener.
///
/// The gateway streams request and response bodies, so these limits deliberately
/// protect connection and request-head resources without imposing a body-size or
/// whole-request timeout on uploads, downloads, or upgraded connections.
#[derive(Clone, Copy, Debug)]
pub struct GatewayConfig {
    /// Maximum simultaneously accepted downstream TCP connections.
    pub max_connections: usize,
    /// Maximum HTTP/1 read buffer size, including a request head.
    pub max_request_head_bytes: usize,
    /// Maximum time allowed to receive one HTTP request head.
    pub header_read_timeout: Duration,
    /// Whether to enable `TCP_NODELAY` on each accepted socket.
    pub tcp_nodelay: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_connections: 65_536,
            max_request_head_bytes: 32 * 1024,
            header_read_timeout: Duration::from_secs(15),
            tcp_nodelay: true,
        }
    }
}

impl GatewayConfig {
    fn validate(self) -> std::io::Result<()> {
        if self.max_connections == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_connections must be greater than zero",
            ));
        }
        if self.max_request_head_bytes < MIN_HTTP1_BUFFER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_request_head_bytes must be at least 8192",
            ));
        }
        if self.header_read_timeout.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "header_read_timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Accepts gateway connections until `shutdown` resolves, then drains active
/// connections, including proxied upgrade tunnels, up to `shutdown_grace`.
///
/// # Errors
/// Returns an error when accepting a connection fails.
pub async fn serve<S, F>(
    listener: TcpListener,
    gateway: Gateway<S>,
    shutdown: F,
    shutdown_grace: Duration,
) -> std::io::Result<()>
where
    S: MatchedService,
    F: Future<Output = ()>,
{
    serve_with_config(
        listener,
        gateway,
        shutdown,
        shutdown_grace,
        GatewayConfig::default(),
    )
    .await
}

/// Accepts gateway connections using explicit resource and socket limits until
/// `shutdown` resolves, then drains active connections up to `shutdown_grace`.
///
/// # Errors
/// Returns an error for an invalid configuration or when accepting a connection
/// fails.
pub async fn serve_with_config<S, F>(
    listener: TcpListener,
    gateway: Gateway<S>,
    shutdown: F,
    shutdown_grace: Duration,
    config: GatewayConfig,
) -> std::io::Result<()>
where
    S: MatchedService,
    F: Future<Output = ()>,
{
    config.validate()?;
    let gateway = Arc::new(gateway);
    brz_http_server::hyper::serve(
        listener,
        move |peer_addr| {
            let gateway = Arc::clone(&gateway);
            service_fn(move |request| {
                let gateway = Arc::clone(&gateway);
                async move { Ok::<_, Infallible>(gateway.handle(request, peer_addr).await) }
            })
        },
        shutdown,
        brz_http_server::hyper::Config {
            max_connections: config.max_connections,
            max_request_head_bytes: config.max_request_head_bytes,
            header_read_timeout: config.header_read_timeout,
            shutdown_grace,
            tcp_nodelay: config.tcp_nodelay,
        },
    )
    .await?;
    Ok(())
}

/// Binds a TCP listener for the gateway.
///
/// # Errors
/// Returns an error when the address cannot be bound.
pub async fn bind(address: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(address).await
}
