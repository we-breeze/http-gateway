use std::net::SocketAddr;
use std::time::Duration;

use brz_http_gateway::{
    AcquireOutcome, AdmissionFuture, AdmissionProvider, AdmissionRegistry, AdmissionTicket,
    Gateway, OriginService, RouteTable, RoutesConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::oneshot;

struct Admitted;
impl AdmissionProvider for Admitted {
    fn try_acquire<'a>(&'a self, scope: &'a str) -> AdmissionFuture<'a> {
        Box::pin(async move {
            Ok(AcquireOutcome::Acquired(AdmissionTicket::new(
                scope, "ticket",
            )?))
        })
    }
}

async fn head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await.unwrap());
    }
    String::from_utf8(head).unwrap()
}

async fn write(address: SocketAddr) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            b"POST /write?part=1 HTTP/1.1\r\nHost: public.example\r\nContent-Length: 7\r\n\
        Authorization: Bearer example\r\nConnection: close\r\n\r\npayload",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(response).unwrap()
}

async fn exercise(status: Option<u16>, original_status: u16, admission: bool, unreachable: bool) {
    let original = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let original_origin = format!("http://{}", original.local_addr().unwrap())
        .parse()
        .unwrap();
    let recorder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    // A bound socket that never listens refuses connections while reserving
    // the port, so concurrent fixtures cannot accidentally reuse it.
    let unavailable = unreachable.then(|| {
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        socket
    });
    let recorder_address = unavailable.as_ref().map_or_else(
        || recorder.local_addr().unwrap(),
        |socket| socket.local_addr().unwrap(),
    );
    let recorder_origin = format!("http://{recorder_address}").parse().unwrap();
    let recorder_task = if unreachable {
        // Use the reserved non-listening socket for connection refusal.
        drop(recorder);
        None
    } else {
        Some(tokio::spawn(async move {
            let (mut stream, _) = recorder.accept().await.unwrap();
            let headers = head(&mut stream).await.to_ascii_lowercase();
            assert!(headers.starts_with("post /write?part=1 http/1.1"));
            assert!(headers.contains("authorization: bearer example"));
            assert_eq!(
                headers.contains("x-breeze-admission-token: ticket"),
                admission
            );
            let mut body = [0; 7];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(&body, b"payload");
            if let Some(status) = status {
                stream.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: 8\r\nConnection: close\r\n\r\nrecorded").as_bytes()).await.unwrap();
            }
        }))
    };
    let routes: RoutesConfig = toml::from_str(if admission {
        r#"[[routes]]
            methods = ["POST"]
            path = "/write"
            admission = { provider = "recorder", scope = "writes" }"#
    } else {
        r#"[[routes]]
            methods = ["POST"]
            path = "/write""#
    })
    .unwrap();
    let mut registry = AdmissionRegistry::new();
    registry.register("recorder", Admitted).unwrap();
    let gateway = Gateway::with_admission(
        RouteTable::compile(routes).unwrap(),
        OriginService::new(&recorder_origin).unwrap(),
        &original_origin,
        registry,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let gateway_task = tokio::spawn(brz_http_gateway::serve(
        listener,
        gateway,
        async move {
            let _ = stopped.await;
        },
        Duration::from_secs(1),
    ));
    let request = tokio::spawn(write(address));
    if (status == Some(404) || unreachable) && admission {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), original.accept())
            .await
            .unwrap()
            .unwrap();
        let headers = head(&mut stream).await.to_ascii_lowercase();
        assert!(headers.starts_with("post /write?part=1 http/1.1"));
        assert!(headers.contains("host: public.example"));
        assert!(headers.contains("authorization: bearer example"));
        assert_eq!(headers.matches("x-forwarded-for:").count(), 1);
        assert!(!headers.contains("x-breeze-admission"));
        let mut body = [0; 7];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"payload");
        stream.write_all(format!("HTTP/1.1 {original_status} Fixture\r\nContent-Length: 8\r\nConnection: close\r\n\r\noriginal").as_bytes()).await.unwrap();
        let response = request.await.unwrap();
        assert!(response.starts_with(&format!("HTTP/1.1 {original_status}")));
        assert!(response.ends_with("original"));
    } else {
        let response = request.await.unwrap();
        assert!(response.starts_with(&format!("HTTP/1.1 {}", status.unwrap_or(502))));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), original.accept())
            .await
            .is_err()
    );
    if let Some(task) = recorder_task {
        task.await.unwrap();
    }
    stop.send(()).unwrap();
    gateway_task.await.unwrap().unwrap();
    drop(unavailable);
}

#[tokio::test]
async fn recording_404_replays_the_original_write_once() {
    exercise(Some(404), 201, true, false).await;
    exercise(Some(404), 404, true, false).await;
}

#[tokio::test]
async fn other_statuses_and_errors_after_dispatch_do_not_fallback() {
    exercise(Some(200), 200, true, false).await;
    exercise(Some(500), 200, true, false).await;
    exercise(Some(504), 200, true, false).await;
    exercise(None, 200, true, false).await;
}

#[tokio::test]
async fn recorder_connect_failure_uses_original_path_once() {
    exercise(None, 201, true, true).await;
    exercise(None, 404, true, true).await;
}

#[tokio::test]
async fn ordinary_migration_routes_do_not_replay_a_404() {
    exercise(Some(404), 200, false, false).await;
    exercise(None, 200, false, true).await;
}

#[tokio::test]
async fn recording_connect_failure_preserves_the_fallback_upgrade_tunnel() {
    let original = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let original_origin = format!("http://{}", original.local_addr().unwrap())
        .parse()
        .unwrap();
    let recorder = TcpSocket::new_v4().unwrap();
    recorder.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let recorder_origin = format!("http://{}", recorder.local_addr().unwrap())
        .parse()
        .unwrap();
    let original_task = tokio::spawn(async move {
        let (mut stream, _) = original.accept().await.unwrap();
        let headers = head(&mut stream).await.to_ascii_lowercase();
        assert!(headers.starts_with("get /upgrade http/1.1"));
        assert!(headers.contains("upgrade: example"));
        assert!(!headers.contains("x-breeze-admission"));
        stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: example\r\n\r\n").await.unwrap();
        let mut message = [0; 4];
        stream.read_exact(&mut message).await.unwrap();
        assert_eq!(&message, b"ping");
        stream.write_all(b"pong").await.unwrap();
    });
    let routes: RoutesConfig = toml::from_str(
        r#"[[routes]]
        methods = ["GET"]
        path = "/upgrade"
        admission = { provider = "recorder", scope = "writes" }"#,
    )
    .unwrap();
    let mut registry = AdmissionRegistry::new();
    registry.register("recorder", Admitted).unwrap();
    let gateway = Gateway::with_admission(
        RouteTable::compile(routes).unwrap(),
        OriginService::new(&recorder_origin).unwrap(),
        &original_origin,
        registry,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(brz_http_gateway::serve(
        listener,
        gateway,
        async move {
            let _ = stopped.await;
        },
        Duration::from_secs(1),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(b"GET /upgrade HTTP/1.1\r\nHost: public.example\r\nConnection: upgrade\r\nUpgrade: example\r\n\r\n").await.unwrap();
        assert!(head(&mut stream).await.starts_with("HTTP/1.1 101"));
        stream.write_all(b"ping").await.unwrap();
        let mut message = [0; 4];
        stream.read_exact(&mut message).await.unwrap();
        assert_eq!(&message, b"pong");
        original_task.await.unwrap();
    }).await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    drop(recorder);
}
