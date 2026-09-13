use std::net::SocketAddr;
use std::time::Duration;

use brz_http_gateway::{Gateway, RejectMatched, RouteTable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

async fn spawn_gateway(
    upstream: SocketAddr,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{upstream}").parse().unwrap();
    let gateway = Gateway::new(RouteTable::empty(), RejectMatched, &origin).unwrap();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(brz_http_gateway::serve(
        listener,
        gateway,
        async move {
            let _ = stopped.await;
        },
        Duration::from_secs(1),
    ));
    (address, shutdown, task)
}

async fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let byte = stream.read_u8().await.unwrap();
        head.push(byte);
        assert!(head.len() < 32 * 1024, "HTTP head exceeded test bound");
    }
    head
}

fn decode_chunked(mut encoded: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::new();
    loop {
        let line_end = encoded
            .windows(2)
            .position(|bytes| bytes == b"\r\n")
            .unwrap();
        let size =
            usize::from_str_radix(std::str::from_utf8(&encoded[..line_end]).unwrap(), 16).unwrap();
        encoded = &encoded[line_end + 2..];
        if size == 0 {
            break;
        }
        decoded.extend_from_slice(&encoded[..size]);
        encoded = &encoded[size + 2..];
    }
    decoded
}

#[tokio::test]
async fn empty_routes_stream_chunked_traffic_and_repeated_headers() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let head = String::from_utf8(read_head(&mut stream).await).unwrap();
        assert!(head.starts_with("POST /echo?part=1 HTTP/1.1\r\n"));
        assert!(
            head.to_ascii_lowercase()
                .contains("transfer-encoding: chunked")
        );
        assert!(head.contains("x-forwarded-host: public.example"));
        assert!(head.contains("x-forwarded-proto: http"));

        let mut encoded_body = Vec::new();
        while !encoded_body.ends_with(b"0\r\n\r\n") {
            encoded_body.push(stream.read_u8().await.unwrap());
        }
        assert_eq!(decode_chunked(&encoded_body), b"data");

        stream
            .write_all(
                b"HTTP/1.1 201 Created\r\n\
                  Transfer-Encoding: chunked\r\n\
                  Set-Cookie: first=1\r\n\
                  Set-Cookie: second=2\r\n\
                  Connection: close\r\n\r\n\
                  4\r\npong\r\n0\r\n\r\n",
            )
            .await
            .unwrap();
    });

    let (gateway_address, shutdown, gateway_task) = spawn_gateway(upstream_address).await;
    let mut client = TcpStream::connect(gateway_address).await.unwrap();
    client
        .write_all(
            b"POST /echo?part=1 HTTP/1.1\r\n\
              Host: public.example\r\n\
              Transfer-Encoding: chunked\r\n\
              Connection: close\r\n\r\n\
              2\r\nda\r\n2\r\nta\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 201 Created\r\n"));
    assert!(response.contains("set-cookie: first=1\r\n"));
    assert!(response.contains("set-cookie: second=2\r\n"));
    assert!(response.contains("pong"));

    upstream_task.await.unwrap();
    let _ = shutdown.send(());
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn empty_routes_tunnel_upgraded_connections() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let head = String::from_utf8(read_head(&mut stream).await).unwrap();
        assert!(head.starts_with("GET /socket HTTP/1.1\r\n"));
        assert!(head.to_ascii_lowercase().contains("connection: upgrade"));
        assert!(head.to_ascii_lowercase().contains("upgrade: test"));
        stream
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\n\
                  Connection: upgrade\r\n\
                  Upgrade: test\r\n\r\n",
            )
            .await
            .unwrap();
        let mut ping = [0_u8; 4];
        stream.read_exact(&mut ping).await.unwrap();
        assert_eq!(&ping, b"ping");
        stream.write_all(b"pong").await.unwrap();
    });

    let (gateway_address, shutdown, gateway_task) = spawn_gateway(upstream_address).await;
    let mut client = TcpStream::connect(gateway_address).await.unwrap();
    client
        .write_all(
            b"GET /socket HTTP/1.1\r\n\
              Host: public.example\r\n\
              Connection: upgrade\r\n\
              Upgrade: test\r\n\r\n",
        )
        .await
        .unwrap();
    let response_head = String::from_utf8(read_head(&mut client).await).unwrap();
    assert!(response_head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
    client.write_all(b"ping").await.unwrap();
    let mut pong = [0_u8; 4];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut pong))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&pong, b"pong");
    drop(client);

    upstream_task.await.unwrap();
    let _ = shutdown.send(());
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn empty_routes_deliver_stream_chunks_before_upstream_finishes() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let (release, released) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let _ = read_head(&mut stream).await;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\n\
                  Content-Type: text/event-stream\r\n\
                  Transfer-Encoding: chunked\r\n\r\n\
                  b\r\ndata: one\n\n\r\n",
            )
            .await
            .unwrap();
        released.await.unwrap();
        stream.write_all(b"0\r\n\r\n").await.unwrap();
    });

    let (gateway_address, shutdown, gateway_task) = spawn_gateway(upstream_address).await;
    let mut client = TcpStream::connect(gateway_address).await.unwrap();
    client
        .write_all(
            b"GET /events HTTP/1.1\r\n\
              Host: public.example\r\n\
              Connection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let head = String::from_utf8(read_head(&mut client).await).unwrap();
    assert!(head.contains("content-type: text/event-stream"));

    let mut first_chunk = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !first_chunk
            .windows(b"data: one\n\n".len())
            .any(|bytes| bytes == b"data: one\n\n")
        {
            first_chunk.push(client.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    client.read_to_end(&mut Vec::new()).await.unwrap();

    upstream_task.await.unwrap();
    let _ = shutdown.send(());
    gateway_task.await.unwrap().unwrap();
}
