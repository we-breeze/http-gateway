use super::*;
use brz_http_gateway::{
    AdmissionRegistry, AdmissionRule, ExclusionRule, Gateway, OriginService, RouteTable,
    RoutesConfig, serve,
};
use std::net::SocketAddr;
use tokio::net::TcpStream;

struct Server {
    address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Received {
    head: String,
    body: Vec<u8>,
}

struct Origin {
    server: Server,
    received: Arc<Mutex<Vec<Received>>>,
}

async fn origin(provider: Option<RedisAdmissionProvider>) -> Origin {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::clone(&received);
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let provider = provider.clone();
            let requests = Arc::clone(&requests);
            connections.spawn(async move {
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    head.push(stream.read_u8().await.unwrap());
                }
                let head = String::from_utf8(head).unwrap();
                let length = head.lines().find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                }).unwrap_or(0);
                let mut payload = vec![0; length];
                stream.read_exact(&mut payload).await.unwrap();
                let body = if let Some(provider) = provider {
                    let token = head.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("x-breeze-admission-token").then(|| value.trim())
                    }).unwrap();
                    assert_ne!(token, "forged");
                    let ticket = AdmissionTicket::new("all-apis", token).unwrap();
                    assert!(provider.owns(&ticket).await.unwrap());
                    assert!(provider.complete(&ticket).await.unwrap());
                    "recorded"
                } else {
                    assert!(!head.to_ascii_lowercase().contains("x-breeze-admission-"));
                    "original"
                };
                requests.lock().unwrap().push(Received { head, body: payload });
                stream.write_all(format!("HTTP/1.1 200 OK\r\nX-Origin: {body}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    Origin {
        server: Server { address, task },
        received,
    }
}

async fn request(address: SocketAddr, method: &str, path: &str) -> String {
    request_with_body(address, method, path, b"").await
}

async fn request_with_body(
    address: SocketAddr,
    method: &str,
    path: &str,
    payload: &[u8],
) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: public.example\r\nContent-Length: {}\r\nAuthorization: Bearer example\r\nConnection: close\r\nx-breeze-admission-scope: forged\r\nx-breeze-admission-token: forged\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
    stream.write_all(payload).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

async fn gateway(
    provider: RedisAdmissionProvider,
    fallback: &Origin,
    routes: RouteTable,
) -> Server {
    let mut registry = AdmissionRegistry::new();
    registry.register("recorder", provider).unwrap();
    let gateway = Gateway::with_admission(
        routes,
        OriginService::from_admission(),
        &format!("http://{}", fallback.server.address)
            .parse()
            .unwrap(),
        registry,
    )
    .unwrap()
    .with_recording_admission(AdmissionRule {
        provider: "recorder".into(),
        scope: "all-apis".into(),
        acquire_timeout_ms: 1000,
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    Server {
        address,
        task: tokio::spawn(async move {
            serve(
                listener,
                gateway,
                std::future::pending(),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        }),
    }
}

pub(super) async fn exercise() {
    let fixture = Fixture::new(Some(r#"{"case-limit":2,"includes":[{"path":"/api/:id","match":"exact"}],"excludes":[{"path":"/api/private"}]}"#)).await;
    let recorder = origin(Some(fixture.provider.clone())).await;
    let fallback = origin(None).await;
    *fixture.state.url.lock().unwrap() = Some(format!("http://{}", recorder.server.address));
    fixture.provider.load_policy().await.unwrap();
    let routes = RouteTable::compile(RoutesConfig {
        exclude: vec![ExclusionRule {
            methods: vec![],
            path: "/api/excluded".into(),
        }],
        ..RoutesConfig::default()
    })
    .unwrap();
    let server = gateway(fixture.provider.clone(), &fallback, routes).await;
    for (method, path) in [("GET", "/api/123?first=1"), ("POST", "/api/456?second=2")] {
        assert!(
            request(server.address, method, path)
                .await
                .ends_with("recorded")
        );
    }
    assert_eq!(
        fixture
            .state
            .counts
            .lock()
            .unwrap()
            .get("/api/:id")
            .unwrap(),
        "2"
    );
    assert!(
        request(server.address, "DELETE", "/api/789")
            .await
            .ends_with("original")
    );
    fixture.state.take_commands();
    for path in ["/api/private", "/api/excluded", "/outside"] {
        assert!(
            request(server.address, "GET", path)
                .await
                .ends_with("original")
        );
    }
    assert!(fixture.state.take_commands().is_empty());
}

#[derive(Debug)]
enum Fault {
    Redis(&'static str),
    Reply(&'static str, &'static [u8]),
    Url(&'static str),
    Count(&'static str),
    Disconnected,
}

fn assert_original(response: &str) {
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response
            .to_ascii_lowercase()
            .contains("x-origin: original\r\n")
    );
    assert!(response.ends_with("original"), "{response}");
}

pub(super) async fn admission_faults() {
    for fault in [
        Fault::Redis("GET"),
        Fault::Redis("HGET"),
        Fault::Redis("SET"),
        Fault::Redis("HINCRBY"),
        Fault::Reply("GET", b":123\r\n"),
        Fault::Reply("HGET", b":123\r\n"),
        Fault::Reply("SET", b"+unexpected\r\n"),
        Fault::Reply("HINCRBY", b":-1\r\n"),
        Fault::Url("broken URL"),
        Fault::Url("https://recorder.example"),
        Fault::Url("http://recorder.example/path"),
        Fault::Url(r#"{"url":"http://recorder.example","owner":""}"#),
        Fault::Count("-1"),
        Fault::Count("wrong"),
        Fault::Count("18446744073709551616"),
        Fault::Disconnected,
    ] {
        let fixture = Fixture::new(Some(r#"{"case-limit":2}"#)).await;
        let recorder = origin(Some(fixture.provider.clone())).await;
        let fallback = origin(None).await;
        *fixture.state.url.lock().unwrap() = Some(format!("http://{}", recorder.server.address));
        fixture.provider.load_policy().await.unwrap();
        fixture.state.take_commands();
        let server = gateway(fixture.provider.clone(), &fallback, RouteTable::empty()).await;
        match fault {
            Fault::Redis(command) => *fixture.state.fail.lock().unwrap() = Some(command.into()),
            Fault::Reply(command, reply) => {
                *fixture.state.override_reply.lock().unwrap() =
                    Some((command.into(), reply.into()));
            }
            Fault::Url(url) => *fixture.state.url.lock().unwrap() = Some(url.into()),
            Fault::Count(count) => {
                fixture
                    .state
                    .counts
                    .lock()
                    .unwrap()
                    .insert("/api/123".into(), count.into());
            }
            Fault::Disconnected => {
                fixture.task.abort();
                // Also drops its connection tasks, closing already connected clients.
                while !fixture.task.is_finished() {
                    tokio::task::yield_now().await;
                }
            }
        }
        let payload = b"{\"value\":\"unchanged\"}\0\xff";
        let response =
            request_with_body(server.address, "POST", "/api/123?q=one%20two", payload).await;
        assert_original(&response);
        // The same gateway continues serving subsequent requests after a failure.
        assert_original(&request(server.address, "GET", "/health?check=1").await);
        assert!(recorder.received.lock().unwrap().is_empty(), "{fault:?}");
        let received = fallback.received.lock().unwrap();
        assert_eq!(received.len(), 2, "{fault:?}");
        assert!(
            received[0]
                .head
                .starts_with("POST /api/123?q=one%20two HTTP/1.1\r\n")
        );
        assert_eq!(received[0].body, payload, "{fault:?}");
        let headers = received[0].head.to_ascii_lowercase();
        assert!(headers.contains("host: public.example\r\n"));
        assert!(headers.contains("authorization: bearer example\r\n"));
        assert!(!headers.contains("x-breeze-admission-"));
        assert!(
            received[1]
                .head
                .starts_with("GET /health?check=1 HTTP/1.1\r\n")
        );
    }
}

pub(super) async fn policy_faults() {
    let fixture = Fixture::new(Some(
        r#"{"case-limit":1,"includes":[{"path":"/api/:id","match":"exact"}]}"#,
    ))
    .await;
    let recorder = origin(Some(fixture.provider.clone())).await;
    let fallback = origin(None).await;
    *fixture.state.url.lock().unwrap() = Some(format!("http://{}", recorder.server.address));
    fixture.provider.load_policy().await.unwrap();
    let server = gateway(fixture.provider.clone(), &fallback, RouteTable::empty()).await;
    for policy in [
        "invalid JSON",
        r#"{"case-limit":-1}"#,
        r#"{"includes":"/api"}"#,
        r#"{"includes":[{"path":"relative"}]}"#,
        r#"{"includes":[{"path":"/api","match":"unsupported"}]}"#,
        r#"{"includes":[{"path":"/api/*rest/invalid","match":"exact"}]}"#,
    ] {
        *fixture.state.policy.lock().unwrap() = Some(policy.into());
        assert!(fixture.provider.load_policy().await.is_err(), "{policy}");
        fixture.state.counts.lock().unwrap().clear();
        assert_retained_policy(&fixture, &server).await;
    }
    // Transport errors and wrong Redis types preserve the same compiled snapshot.
    *fixture.state.fail.lock().unwrap() = Some("GET".into());
    assert!(fixture.provider.load_policy().await.is_err());
    *fixture.state.fail.lock().unwrap() = None;
    fixture.state.counts.lock().unwrap().clear();
    assert_retained_policy(&fixture, &server).await;
    *fixture.state.override_reply.lock().unwrap() = Some(("GET".into(), b":1\r\n".to_vec()));
    assert!(fixture.provider.load_policy().await.is_err());
    *fixture.state.override_reply.lock().unwrap() = None;
    fixture.state.counts.lock().unwrap().clear();
    assert_retained_policy(&fixture, &server).await;
}

async fn assert_retained_policy(fixture: &Fixture, server: &Server) {
    fixture.state.take_commands();
    assert_original(&request(server.address, "POST", "/outside").await);
    assert!(fixture.state.take_commands().is_empty());
    assert!(
        request(server.address, "POST", "/api/123")
            .await
            .ends_with("recorded")
    );
    assert_original(&request(server.address, "PUT", "/api/456").await);
    assert_eq!(
        fixture
            .state
            .counts
            .lock()
            .unwrap()
            .get("/api/:id")
            .unwrap(),
        "1"
    );
}

pub(super) async fn invalid_initial_policy() {
    let fixture = Fixture::new(Some("invalid JSON")).await;
    let recorder = origin(Some(fixture.provider.clone())).await;
    let fallback = origin(None).await;
    *fixture.state.url.lock().unwrap() = Some(format!("http://{}", recorder.server.address));
    assert!(fixture.provider.load_policy().await.is_err());
    fixture.state.take_commands();
    let server = gateway(fixture.provider.clone(), &fallback, RouteTable::empty()).await;
    for _ in 0..2 {
        assert_original(
            &request_with_body(server.address, "POST", "/api/123?x=1", b"payload").await,
        );
    }
    assert!(fixture.state.take_commands().is_empty());
    assert!(recorder.received.lock().unwrap().is_empty());
    *fixture.state.policy.lock().unwrap() = Some(r#"{"case-limit":1}"#.into());
    fixture.provider.load_policy().await.unwrap();
    assert!(
        request(server.address, "POST", "/api/123")
            .await
            .ends_with("recorded")
    );
}
