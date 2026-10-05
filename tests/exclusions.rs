use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brz_http_gateway::{
    AcquireOutcome, AdmissionFuture, AdmissionProvider, AdmissionRegistry, Gateway,
    GatewayBuildError, RejectMatched, RouteTable, RoutesConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

struct Provider(Arc<AtomicUsize>);

impl AdmissionProvider for Provider {
    fn try_acquire<'a>(&'a self, _: &'a str) -> AdmissionFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(AcquireOutcome::Busy) })
    }
}

async fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await.unwrap());
        assert!(head.len() < 32 * 1024);
    }
    String::from_utf8(head).unwrap()
}

async fn exercise(admission: bool) {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", upstream.local_addr().unwrap())
        .parse()
        .unwrap();
    let (release, released) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let head = read_head(&mut stream).await;
        assert!(head.starts_with("GET /api/events?cursor=1 HTTP/1.1"));
        assert!(!head.to_ascii_lowercase().contains("x-breeze-admission-"));
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                  Transfer-Encoding: chunked\r\n\r\nb\r\ndata: one\n\n\r\n",
            )
            .await
            .unwrap();
        released.await.unwrap();
        stream.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let admission_rule = if admission {
        "admission = { provider = \"recorder\", scope = \"all-apis\" }"
    } else {
        ""
    };
    let routes = RouteTable::compile(
        toml::from_str::<RoutesConfig>(&format!(
            "[[routes]]\npath = \"/api/*path\"\n{admission_rule}\n\
             [[exclude]]\nmethods = [\"GET\"]\npath = \"/api/events\""
        ))
        .unwrap(),
    )
    .unwrap();
    let acquisitions = Arc::new(AtomicUsize::new(0));
    let mut registry = AdmissionRegistry::new();
    registry
        .register("recorder", Provider(Arc::clone(&acquisitions)))
        .unwrap();
    let gateway = Gateway::with_admission(routes, RejectMatched, &origin, registry).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(brz_http_gateway::serve(
        listener,
        gateway,
        async {
            let _ = stopped.await;
        },
        Duration::from_secs(1),
    ));
    let mut client = TcpStream::connect(address).await.unwrap();
    client
        .write_all(
            b"GET /api/events?cursor=1 HTTP/1.1\r\nHost: public.example\r\n\
              Connection: close\r\nx-breeze-admission-token: forged\r\n\r\n",
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let head = read_head(&mut client).await;
        assert!(head.starts_with("HTTP/1.1 200"));
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: text/event-stream")
        );
        let mut received = Vec::new();
        while !received.windows(11).any(|bytes| bytes == b"data: one\n\n") {
            received.push(client.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    assert_eq!(acquisitions.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    client.read_to_end(&mut Vec::new()).await.unwrap();
    upstream_task.await.unwrap();
    shutdown.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn excluded_requests_stream_without_calling_admission_or_the_selected_service() {
    exercise(true).await;
    exercise(false).await;
}

#[test]
fn exclusions_do_not_hide_unregistered_providers() {
    let config: RoutesConfig = toml::from_str(
        "[[routes]]\npath = \"/api/events\"\n\
         admission = { provider = \"missing\", scope = \"all-apis\" }\n\
         [[exclude]]\npath = \"/api/events\"",
    )
    .unwrap();
    let origin = "http://127.0.0.1:9000".parse().unwrap();
    assert!(matches!(
        Gateway::new(RouteTable::compile(config).unwrap(), RejectMatched, &origin),
        Err(GatewayBuildError::MissingProvider { .. })
    ));
}
