use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use brz_http_gateway::{
    Gateway, GatewayConfig, OriginService, RouteRule, RouteTable, RoutesConfig, serve_with_config,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const BOUND: Duration = Duration::from_secs(2);

async fn read_head(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await?);
        assert!(head.len() < 32 * 1024);
    }
    Ok(head)
}

async fn echo_upstream(count: usize) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        for _ in 0..count {
            let (mut stream, _) = listener.accept().await.unwrap();
            connections.spawn(async move {
                read_head(&mut stream).await.unwrap();
                stream
                    .write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: test\r\n\r\n")
                    .await
                    .unwrap();
                let (mut reader, mut writer) = stream.split();
                tokio::io::copy(&mut reader, &mut writer).await.unwrap();
            });
        }
        while let Some(result) = connections.join_next().await {
            result.unwrap();
        }
    });
    (address, task)
}

async fn gateway(
    upstream: SocketAddr,
    matched: bool,
    grace: Duration,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{upstream}").parse().unwrap();
    let routes = RouteTable::compile(RoutesConfig {
        routes: if matched {
            vec![RouteRule {
                path: "/socket".to_owned(),
                methods: vec!["GET".to_owned()],
                admission: None,
            }]
        } else {
            Vec::new()
        },
        ..RoutesConfig::default()
    })
    .unwrap();
    let gateway = Gateway::new(routes, OriginService::new(&origin).unwrap(), &origin).unwrap();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(serve_with_config(
        listener,
        gateway,
        async move {
            let _ = stopped.await;
        },
        grace,
        GatewayConfig {
            max_connections: 1,
            ..GatewayConfig::default()
        },
    ));
    (address, shutdown, task)
}

async fn upgrade(address: SocketAddr) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(address).await?;
    stream
        .write_all(
            b"GET /socket HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test\r\n\r\n",
        )
        .await?;
    let head = read_head(&mut stream).await?;
    assert!(head.starts_with(b"HTTP/1.1 101 "));
    Ok(stream)
}

async fn roundtrip(stream: &mut TcpStream) {
    stream.write_all(b"ping").await.unwrap();
    let mut response = [0; 4];
    tokio::time::timeout(BOUND, stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"ping");
}

async fn closed(stream: &mut TcpStream) {
    let mut response = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.is_empty());
}

#[tokio::test]
async fn upgrade_keeps_connection_permit_until_tunnel_closes() {
    for matched in [false, true] {
        let (upstream, upstream_task) = echo_upstream(2).await;
        let (address, shutdown, task) = gateway(upstream, matched, BOUND).await;
        let mut first = upgrade(address).await.unwrap();
        roundtrip(&mut first).await;

        let mut excess = TcpStream::connect(address).await.unwrap();
        closed(&mut excess).await;
        first.shutdown().await.unwrap();
        closed(&mut first).await;

        let mut next = tokio::time::timeout(BOUND, async {
            loop {
                match upgrade(address).await {
                    Ok(stream) => break stream,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
        roundtrip(&mut next).await;
        next.shutdown().await.unwrap();
        closed(&mut next).await;
        shutdown.send(()).unwrap();
        tokio::time::timeout(BOUND, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(BOUND, upstream_task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn shutdown_drains_a_live_tunnel_until_client_closes() {
    let (upstream, upstream_task) = echo_upstream(1).await;
    let (address, shutdown, mut task) = gateway(upstream, false, BOUND).await;
    let mut client = upgrade(address).await.unwrap();
    roundtrip(&mut client).await;
    shutdown.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut task)
            .await
            .is_err()
    );
    roundtrip(&mut client).await;
    client.shutdown().await.unwrap();
    closed(&mut client).await;
    tokio::time::timeout(Duration::from_millis(500), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(BOUND, upstream_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_timeout_closes_both_tunnel_sockets() {
    for matched in [false, true] {
        let (upstream, upstream_task) = echo_upstream(1).await;
        let (address, shutdown, mut task) =
            gateway(upstream, matched, Duration::from_millis(150)).await;
        let mut client = upgrade(address).await.unwrap();
        roundtrip(&mut client).await;
        shutdown.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut task)
                .await
                .is_err()
        );
        roundtrip(&mut client).await;
        tokio::time::timeout(BOUND, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        closed(&mut client).await;
        tokio::time::timeout(BOUND, upstream_task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn cancelling_server_also_cancels_its_tunnel() {
    let (upstream, upstream_task) = echo_upstream(1).await;
    let (address, _shutdown, task) = gateway(upstream, false, BOUND).await;
    let mut client = upgrade(address).await.unwrap();
    roundtrip(&mut client).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    closed(&mut client).await;
    tokio::time::timeout(BOUND, upstream_task)
        .await
        .unwrap()
        .unwrap();
}
