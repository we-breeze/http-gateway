use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::{Gateway, MatchedService};

/// Accepts gateway connections until `shutdown` resolves, then drains active
/// connections up to `shutdown_grace`.
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
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                let gateway = gateway.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let gateway = gateway.clone();
                        async move {
                            Ok::<_, Infallible>(gateway.handle(request, peer_addr).await)
                        }
                    });
                    let connection = http1::Builder::new()
                        .keep_alive(true)
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades();
                    if let Err(error) = connection.await {
                        debug!(%peer_addr, %error, "gateway connection closed with an error");
                    }
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    warn!(%error, "gateway connection task failed");
                }
            }
        }
    }

    if tokio::time::timeout(shutdown_grace, async {
        while let Some(joined) = connections.join_next().await {
            if let Err(error) = joined {
                warn!(%error, "gateway connection task failed during shutdown");
            }
        }
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    Ok(())
}

/// Binds a TCP listener for the gateway.
///
/// # Errors
/// Returns an error when the address cannot be bound.
pub async fn bind(address: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(address).await
}
