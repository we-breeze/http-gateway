use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brz_http_gateway::{
    ADMISSION_SCOPE_HEADER, ADMISSION_TOKEN_HEADER, AcquireOutcome, AdmissionError,
    AdmissionFuture, AdmissionProvider, AdmissionRegistry, AdmissionTicket, Gateway,
    GatewayBuildError, GatewayResponse, MatchedService, RejectMatched, RouteTable, RoutesConfig,
};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

fn routes() -> RouteTable {
    RouteTable::compile(
        toml::from_str::<RoutesConfig>(
            r#"
        [[routes]]
        methods = ["POST", "PUT"]
        path = "/write/:id"
        admission = { provider = "recorder", scope = "all-writes", acquire_timeout_ms = 20 }
        [[routes]]
        methods = ["POST"]
        path = "/unrestricted"
    "#,
        )
        .unwrap(),
    )
    .unwrap()
}

#[derive(Clone, Copy)]
enum Decision {
    Busy,
    NoRecorder,
    NotParticipant,
    Acquired,
    Error,
    Timeout,
    WrongScope,
}

impl AdmissionProvider for Decision {
    fn try_acquire<'a>(&'a self, scope: &'a str) -> AdmissionFuture<'a> {
        Box::pin(async move {
            match self {
                Self::Busy => Ok(AcquireOutcome::Busy),
                Self::NoRecorder => Ok(AcquireOutcome::NoRecorder),
                Self::NotParticipant => Ok(AcquireOutcome::NotParticipant),
                Self::Acquired => Ok(AcquireOutcome::Acquired(AdmissionTicket::new(
                    scope,
                    "valid-token",
                )?)),
                Self::Error => Err(AdmissionError::backend(std::io::Error::other(
                    "backend error",
                ))),
                Self::Timeout => std::future::pending().await,
                Self::WrongScope => Ok(AcquireOutcome::Acquired(AdmissionTicket::new(
                    "other", "token",
                )?)),
            }
        })
    }
}

#[derive(Clone)]
struct Selected {
    calls: Arc<AtomicUsize>,
    status: StatusCode,
}

impl MatchedService for Selected {
    fn call(
        &self,
        request: Request<Incoming>,
        _: SocketAddr,
    ) -> impl Future<Output = GatewayResponse> + Send {
        let calls = Arc::clone(&self.calls);
        let status = self.status;
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            if request.uri().path() != "/unrestricted" {
                let ticket = request.extensions().get::<AdmissionTicket>().unwrap();
                assert_eq!(ticket.scope(), "all-writes");
                assert_eq!(ticket.token(), "valid-token");
                assert_eq!(request.headers()[ADMISSION_TOKEN_HEADER], "valid-token");
            } else {
                assert!(request.extensions().get::<AdmissionTicket>().is_none());
                assert!(!request.headers().contains_key(ADMISSION_TOKEN_HEADER));
            }
            let body = request.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, "payload");
            Response::builder()
                .status(status)
                .body(
                    Full::new(Bytes::from_static(b"selected"))
                        .map_err(|never| match never {})
                        .boxed(),
                )
                .unwrap()
        }
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

async fn request(address: SocketAddr, method: &str, path: &str) -> String {
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(format!(
        "{method} {path} HTTP/1.1\r\nHost: public.example\r\nContent-Length: 7\r\nConnection: close\r\n\
         x-breeze-admission-scope: forged\r\nx-breeze-admission-token: forged\r\n\r\npayload"
    ).as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(response).unwrap()
}

async fn exercise(decision: Decision, method: &str, path: &str, selected_status: StatusCode) {
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", fallback.local_addr().unwrap())
        .parse()
        .unwrap();
    let fallback_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&fallback_calls);
    let fallback_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = fallback.accept().await.unwrap();
            calls.fetch_add(1, Ordering::SeqCst);
            let head = read_head(&mut stream).await.to_ascii_lowercase();
            assert!(!head.contains(ADMISSION_SCOPE_HEADER));
            assert!(!head.contains(ADMISSION_TOKEN_HEADER));
            let mut body = [0; 7];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(&body, b"payload");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\noriginal",
                )
                .await
                .unwrap();
        }
    });
    let selected_calls = Arc::new(AtomicUsize::new(0));
    let selected = Selected {
        calls: Arc::clone(&selected_calls),
        status: selected_status,
    };
    let mut registry = AdmissionRegistry::new();
    registry.register("recorder", decision).unwrap();
    let gateway = Gateway::with_admission(routes(), selected, &origin, registry).unwrap();
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

    let response = request(address, method, path).await;
    if path == "/unrestricted" || matches!(decision, Decision::Acquired) && method != "GET" {
        assert!(response.starts_with(&format!("HTTP/1.1 {}", selected_status.as_u16())));
        assert!(response.ends_with("selected"));
        assert_eq!(selected_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    } else {
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.ends_with("original"));
        assert_eq!(selected_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 1);
    }
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    fallback_task.abort();
}

#[tokio::test]
async fn busy_uses_original_path_without_forwarding_a_ticket() {
    exercise(Decision::Busy, "POST", "/write/1", StatusCode::OK).await;
    exercise(Decision::NoRecorder, "POST", "/write/1", StatusCode::OK).await;
    exercise(Decision::NotParticipant, "POST", "/write/1", StatusCode::OK).await;
}

#[tokio::test]
async fn acquired_transfers_ticket_and_body_to_selected_service() {
    exercise(
        Decision::Acquired,
        "PUT",
        "/write/2?x=1",
        StatusCode::CREATED,
    )
    .await;
}

#[tokio::test]
async fn acquisition_errors_and_timeouts_use_fallback_before_dispatch() {
    exercise(Decision::Error, "POST", "/write/1", StatusCode::OK).await;
    exercise(Decision::Timeout, "POST", "/write/1", StatusCode::OK).await;
    exercise(Decision::WrongScope, "POST", "/write/1", StatusCode::OK).await;
}

#[tokio::test]
async fn matched_failure_does_not_retry_on_original_path() {
    exercise(
        Decision::Acquired,
        "POST",
        "/write/1",
        StatusCode::BAD_GATEWAY,
    )
    .await;
}

#[tokio::test]
async fn method_mismatch_and_unrestricted_routes_keep_existing_dispatch() {
    exercise(Decision::Error, "GET", "/write/1", StatusCode::OK).await;
    exercise(Decision::Error, "POST", "/unrestricted", StatusCode::OK).await;
}

#[test]
fn unregistered_provider_is_a_construction_error() {
    let origin = "http://127.0.0.1:9000".parse().unwrap();
    assert!(matches!(
        Gateway::new(routes(), RejectMatched, &origin),
        Err(GatewayBuildError::MissingProvider { route: 0, .. })
    ));
}

#[test]
fn registration_rejects_duplicate_and_empty_names() {
    let mut registry = AdmissionRegistry::new();
    registry.register("recorder", Decision::Busy).unwrap();
    assert!(registry.register("recorder", Decision::Acquired).is_err());
    assert!(registry.register(" ", Decision::Busy).is_err());
}

#[test]
fn tickets_validate_headers_and_redact_tokens() {
    assert!(AdmissionTicket::new("scope", "bad\r\ntoken").is_err());
    assert!(AdmissionTicket::new("", "token").is_err());
    let ticket = AdmissionTicket::new("scope", "secret-token").unwrap();
    assert!(!format!("{ticket:?}").contains("secret-token"));
    let mut headers = http::HeaderMap::new();
    assert!(AdmissionTicket::from_headers(&headers).unwrap().is_none());
    headers.insert(ADMISSION_SCOPE_HEADER, "scope".parse().unwrap());
    assert!(AdmissionTicket::from_headers(&headers).is_err());
    headers.insert(ADMISSION_TOKEN_HEADER, "token".parse().unwrap());
    assert_eq!(
        AdmissionTicket::from_headers(&headers)
            .unwrap()
            .unwrap()
            .token(),
        "token"
    );
    headers.append(ADMISSION_TOKEN_HEADER, "second".parse().unwrap());
    assert!(AdmissionTicket::from_headers(&headers).is_err());
}
