use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brz_http_gateway::{
    AcquireOutcome, AdmissionFuture, AdmissionProvider, AdmissionRegistry, Cors, Gateway,
    GatewayBuildError, GatewayResponse, MatchedService, RouteTable, RoutesConfig,
};
use bytes::Bytes;
use http::{Request, Response, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[derive(Clone, Default)]
struct Selected(Arc<AtomicUsize>);

impl MatchedService for Selected {
    async fn call(&self, _: Request<Incoming>, _: SocketAddr) -> GatewayResponse {
        self.0.fetch_add(1, Ordering::SeqCst);
        Response::builder()
            .status(202)
            .header("vary", "Accept-Encoding")
            .header("access-control-allow-origin", "*")
            .header("set-cookie", "a=1")
            .header("set-cookie", "b=2")
            .body(
                Full::new(Bytes::from_static(b"selected"))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap()
    }
}

struct Provider(Arc<AtomicUsize>);

impl AdmissionProvider for Provider {
    fn try_acquire<'a>(&'a self, _: &'a str) -> AdmissionFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(AcquireOutcome::Busy) })
    }
}

fn policy() -> Cors {
    Cors {
        allow_origins: vec!["https://app.example".into()],
        allow_credentials: true,
        expose_headers: vec!["X-Request-ID".into()],
        extra_preflight_vary: vec!["X-Preflight-Variant".into()],
        ..Cors::permissive()
    }
}

fn unavailable() -> Uri {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap()
}

fn routes(config: &str) -> RouteTable {
    RouteTable::compile(toml::from_str::<RoutesConfig>(config).unwrap()).unwrap()
}

async fn request(gateway: Gateway<Selected>, head: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
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
            format!("{head}\r\nHost: gateway.example\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    String::from_utf8(response).unwrap()
}

fn values<'a>(response: &'a str, name: &str) -> Vec<&'a str> {
    response
        .split_once("\r\n\r\n")
        .unwrap()
        .0
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
        .collect()
}

const PREFLIGHT: &str = "OPTIONS /api/apps/installed HTTP/1.1\r\n\
    Origin: https://app.example\r\nAccess-Control-Request-Method: GET\r\n\
    Access-Control-Request-Headers: authorization,content-type,x-request-id";

#[tokio::test]
async fn preflight_succeeds_without_routes_or_a_live_upstream() {
    let selected = Selected::default();
    let gateway = Gateway::new(RouteTable::empty(), selected.clone(), &unavailable())
        .unwrap()
        .with_cors(policy())
        .unwrap();
    let response = request(gateway, PREFLIGHT).await;
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.ends_with("OK"));
    assert_eq!(
        values(&response, "access-control-allow-origin"),
        ["https://app.example"]
    );
    assert_eq!(
        values(&response, "access-control-allow-credentials"),
        ["true"]
    );
    assert_eq!(
        values(&response, "access-control-allow-headers"),
        ["authorization,content-type,x-request-id"]
    );
    assert_eq!(selected.0.load(Ordering::SeqCst), 0);
    assert!(values(&response, "access-control-expose-headers").is_empty());
    assert_eq!(
        values(&response, "vary"),
        [
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers, X-Preflight-Variant"
        ]
    );
}

#[tokio::test]
async fn preflight_does_not_call_admission_or_the_matched_service() {
    let acquisitions = Arc::new(AtomicUsize::new(0));
    let mut admission = AdmissionRegistry::new();
    admission
        .register("recorder", Provider(Arc::clone(&acquisitions)))
        .unwrap();
    let selected = Selected::default();
    let gateway = Gateway::with_admission(routes(
        "[[routes]]\npath = \"/api/*path\"\nadmission = { provider = \"recorder\", scope = \"all\" }"
    ), selected.clone(), &unavailable(), admission).unwrap().with_cors(policy()).unwrap();
    assert!(
        request(gateway, PREFLIGHT)
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert_eq!(acquisitions.load(Ordering::SeqCst), 0);
    assert_eq!(selected.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn disallowed_preflight_is_rejected_locally() {
    for head in [
        PREFLIGHT.replace("https://app.example", "https://other.example"),
        PREFLIGHT.replace("Method: GET", "Method: QUERY"),
    ] {
        let selected = Selected::default();
        let gateway = Gateway::new(RouteTable::empty(), selected.clone(), &unavailable())
            .unwrap()
            .with_cors(policy())
            .unwrap();
        let response = request(gateway, &head).await;
        assert!(response.starts_with("HTTP/1.1 400"));
        assert!(response.ends_with("Disallowed CORS request"));
        assert_eq!(selected.0.load(Ordering::SeqCst), 0);
        assert!(values(&response, "access-control-expose-headers").is_empty());
        assert_eq!(
            values(&response, "vary"),
            [
                "Origin, Access-Control-Request-Method, Access-Control-Request-Headers, X-Preflight-Variant"
            ]
        );
    }
}

#[tokio::test]
async fn disabled_cors_preserves_fallback_behavior() {
    let gateway = Gateway::new(RouteTable::empty(), Selected::default(), &unavailable()).unwrap();
    let response = request(gateway, PREFLIGHT).await;
    assert!(response.starts_with("HTTP/1.1 502"));
    assert!(values(&response, "access-control-allow-origin").is_empty());
}

#[tokio::test]
async fn ordinary_options_and_get_keep_business_routing_and_response_bodies() {
    for head in [
        "OPTIONS /api/apps/installed HTTP/1.1\r\nOrigin: https://app.example",
        "GET /api/apps/installed HTTP/1.1\r\nOrigin: https://app.example\r\nAccess-Control-Request-Method: GET",
    ] {
        let selected = Selected::default();
        let gateway = Gateway::new(
            routes("[[routes]]\npath = \"/api/apps/installed\""),
            selected.clone(),
            &unavailable(),
        )
        .unwrap()
        .with_cors(policy())
        .unwrap();
        let response = request(gateway, head).await;
        assert!(response.starts_with("HTTP/1.1 202"));
        assert!(response.contains("selected"));
        assert_eq!(selected.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            values(&response, "access-control-allow-origin"),
            ["https://app.example"]
        );
        assert_eq!(values(&response, "vary"), ["Accept-Encoding", "Origin"]);
        assert_eq!(
            values(&response, "access-control-expose-headers"),
            ["X-Request-ID"]
        );
        assert_eq!(values(&response, "set-cookie"), ["a=1", "b=2"]);
    }
}

#[tokio::test]
async fn fallback_errors_also_receive_cors_headers() {
    let gateway = Gateway::new(RouteTable::empty(), Selected::default(), &unavailable())
        .unwrap()
        .with_cors(policy())
        .unwrap();
    let response = request(
        gateway,
        "GET /api/apps/installed HTTP/1.1\r\nOrigin: https://app.example",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 502"));
    assert_eq!(
        values(&response, "access-control-allow-origin"),
        ["https://app.example"]
    );
}

#[tokio::test]
async fn python_fallback_responses_use_the_outer_policy() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", upstream.local_addr().unwrap())
        .parse()
        .unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(stream.read_u8().await.unwrap());
        }
        assert!(head.starts_with(b"GET /python HTTP/1.1"));
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\
            Access-Control-Allow-Origin: *\r\nVary: Accept-Encoding\r\n\
            Set-Cookie: a=1\r\nSet-Cookie: b=2\r\nConnection: close\r\n\r\npython",
            )
            .await
            .unwrap();
    });
    let gateway = Gateway::new(RouteTable::empty(), Selected::default(), &origin)
        .unwrap()
        .with_cors(policy())
        .unwrap();
    let response = request(
        gateway,
        "GET /python HTTP/1.1\r\nOrigin: https://app.example",
    )
    .await;
    task.await.unwrap();
    assert!(response.ends_with("python"));
    assert_eq!(
        values(&response, "access-control-allow-origin"),
        ["https://app.example"]
    );
    assert_eq!(values(&response, "vary"), ["Accept-Encoding", "Origin"]);
    assert_eq!(values(&response, "set-cookie"), ["a=1", "b=2"]);
}

#[test]
fn invalid_policy_fails_at_gateway_construction() {
    let cors = Cors {
        allow_origins: vec!["invalid\r\norigin".into()],
        ..policy()
    };
    assert!(matches!(
        Gateway::new(RouteTable::empty(), Selected::default(), &unavailable())
            .unwrap()
            .with_cors(cors),
        Err(GatewayBuildError::InvalidCorsPolicy)
    ));
}
