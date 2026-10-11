//! Real server -> HTTP origin -> gateway -> client response decisions.
use brz_http_gateway::{
    Cors, Gateway, GatewayResponse, MatchedService, OriginService, ResponseCors, RouteTable,
    RoutesConfig,
};
use brz_http_server::{
    Handler, HeaderBlock, NoAuthenticator, Request as ApiRequest, Response as ApiResponse,
    ResponseStream, Server, ServerConfig, StatusCode,
};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

struct Chunks(std::vec::IntoIter<Bytes>);
impl brz_http_server::Stream for Chunks {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.next().map(Ok))
    }
}
struct Api;
impl Handler for Api {
    async fn call<'a>(&'a self, request: ApiRequest<'a>, _: &'a NoAuthenticator) -> ApiResponse {
        assert!(request.header("x-breeze-response-cors").is_none());
        let path = request.path();
        let response = if path.ends_with("/stream") {
            ApiResponse::stream(
                StatusCode::OK,
                ResponseStream::new(Chunks(
                    vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")].into_iter(),
                )),
            )
        } else if path.ends_with("/ok") {
            ApiResponse::static_bytes(StatusCode::OK, b"ok")
        } else {
            ApiResponse::static_bytes(StatusCode::INTERNAL_SERVER_ERROR, b"error")
        };
        let mut headers = request.response_bytes(128);
        headers
            .extend_from_slice(b"Vary: Accept-Encoding\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n");
        let response = response.headers(HeaderBlock::new(headers.freeze()).unwrap());
        if path.ends_with("/unhandled") || path.ends_with("/stream") {
            response.without_cors()
        } else {
            response
        }
    }
}
fn policy() -> Cors {
    Cors {
        allow_credentials: true,
        expose_headers: vec!["X-Request-ID".into()],
        ..Cors::permissive()
    }
}
fn routes() -> RouteTable {
    RouteTable::compile(
        toml::from_str::<RoutesConfig>("[[routes]]\npath = \"/api/*path\"").unwrap(),
    )
    .unwrap()
}
async fn request(address: SocketAddr, target: &str, origin: bool) -> String {
    let mut client = TcpStream::connect(address).await.unwrap();
    // A client cannot choose the response policy or relay this reserved control.
    let origin = if origin {
        "Origin: https://app.example\r\n"
    } else {
        ""
    };
    client.write_all(format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n{origin}X-Breeze-Response-Cors: skip\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
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

#[tokio::test]
async fn http_hop_requires_export_and_trust_preserves_errors_cookies_and_streams() {
    for export in [false, true] {
        let api = Server::bind_with_config(
            "127.0.0.1:0".parse().unwrap(),
            Api,
            ServerConfig {
                export_cors_policy: export,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let api_origin = format!("http://{}", api.local_addr().unwrap())
            .parse()
            .unwrap();
        let (stop_api, stopped_api) = oneshot::channel();
        let api_task = tokio::spawn(api.serve_until(async {
            let _ = stopped_api.await;
        }));
        for trust in [false, true] {
            for enabled in [false, true] {
                let mut origin = OriginService::new(&api_origin).unwrap();
                if trust {
                    origin = origin.with_response_cors();
                }
                let mut gateway = Gateway::new(routes(), origin, &api_origin).unwrap();
                if enabled {
                    gateway = gateway.with_cors(policy()).unwrap();
                }
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
                for has_origin in [false, true] {
                    for path in [
                        "/api/unhandled",
                        "/api/business",
                        "/api/ok",
                        "/api/stream",
                        "/fallback/unhandled",
                    ] {
                        let response = request(address, path, has_origin).await;
                        let skip =
                            export && trust && matches!(path, "/api/unhandled" | "/api/stream");
                        let applies = enabled && !skip;
                        assert!(
                            values(&response, "x-breeze-response-cors").is_empty(),
                            "{response}"
                        );
                        let mut expected_vary = vec!["Accept-Encoding"];
                        if applies {
                            expected_vary.push("Origin");
                        }
                        assert_eq!(
                            values(&response, "vary"),
                            expected_vary,
                            "{path} export={export} trust={trust} enabled={enabled}"
                        );
                        assert_eq!(
                            values(&response, "access-control-allow-origin").is_empty(),
                            !(applies && has_origin)
                        );
                        assert_eq!(
                            values(&response, "access-control-allow-credentials").is_empty(),
                            !(applies && has_origin)
                        );
                        assert_eq!(
                            values(&response, "access-control-expose-headers").is_empty(),
                            !(applies && has_origin)
                        );
                        assert_eq!(values(&response, "set-cookie"), ["a=1", "b=2"]);
                        if path.ends_with("/stream") {
                            assert!(response.starts_with("HTTP/1.1 200"));
                            assert!(response.contains("first") && response.contains("second"));
                            assert_eq!(values(&response, "transfer-encoding"), ["chunked"]);
                        } else {
                            assert!(response.starts_with(if path.ends_with("/ok") {
                                "HTTP/1.1 200"
                            } else {
                                "HTTP/1.1 500"
                            }));
                            assert!(response.ends_with(if path.ends_with("/ok") {
                                "ok"
                            } else {
                                "error"
                            }));
                        }
                    }
                }
                // Preflights remain gateway-owned even with a trusted skip-capable origin.
                let mut client = TcpStream::connect(address).await.unwrap();
                client.write_all(b"OPTIONS /api/unhandled HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n").await.unwrap();
                let mut response = Vec::new();
                tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut response))
                    .await
                    .unwrap()
                    .unwrap();
                let response = String::from_utf8(response).unwrap();
                assert!(values(&response, "x-breeze-response-cors").is_empty());
                if enabled {
                    assert!(response.starts_with("HTTP/1.1 200"));
                    assert!(response.ends_with("OK"));
                }
                stop.send(()).unwrap();
                task.await.unwrap().unwrap();
            }
        }
        stop_api.send(()).unwrap();
        api_task.await.unwrap().unwrap();
    }
}

#[derive(Clone)]
struct Direct;
impl MatchedService for Direct {
    async fn call(&self, _: Request<Incoming>, _: SocketAddr) -> GatewayResponse {
        let mut response = Response::new(
            Full::new(Bytes::from_static(b"direct error"))
                .map_err(|never| match never {})
                .boxed(),
        );
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        response.extensions_mut().insert(ResponseCors::Skip);
        response
    }
}
#[tokio::test]
async fn direct_matched_response_can_skip_without_wire_metadata() {
    let api_origin = "http://127.0.0.1:1".parse().unwrap();
    let gateway = Gateway::new(routes(), Direct, &api_origin)
        .unwrap()
        .with_cors(policy())
        .unwrap();
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
    let response = request(address, "/api/unhandled", true).await;
    assert!(response.starts_with("HTTP/1.1 500"));
    assert!(response.ends_with("direct error"));
    for name in [
        "vary",
        "access-control-allow-origin",
        "access-control-allow-credentials",
        "x-breeze-response-cors",
    ] {
        assert!(values(&response, name).is_empty());
    }
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

struct ReceiverStream(tokio::sync::mpsc::Receiver<Bytes>);
impl brz_http_server::Stream for ReceiverStream {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context).map(|chunk| chunk.map(Ok))
    }
}
struct StreamingApi(std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<Bytes>>>);
impl Handler for StreamingApi {
    async fn call<'a>(&'a self, _: ApiRequest<'a>, _: &'a NoAuthenticator) -> ApiResponse {
        let receiver = self.0.lock().unwrap().take().unwrap();
        ApiResponse::stream(
            StatusCode::OK,
            ResponseStream::new(ReceiverStream(receiver)),
        )
        .without_cors()
    }
}

#[tokio::test]
async fn response_metadata_keeps_streaming_before_the_origin_finishes() {
    let (chunks, receiver) = tokio::sync::mpsc::channel(1);
    chunks.send(Bytes::from_static(b"first")).await.unwrap();
    let api = Server::bind_with_config(
        "127.0.0.1:0".parse().unwrap(),
        StreamingApi(std::sync::Mutex::new(Some(receiver))),
        ServerConfig {
            export_cors_policy: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let origin = format!("http://{}", api.local_addr().unwrap())
        .parse()
        .unwrap();
    let (stop_api, stopped_api) = oneshot::channel();
    let api_task = tokio::spawn(api.serve_until(async {
        let _ = stopped_api.await;
    }));
    let gateway = Gateway::new(
        routes(),
        OriginService::new(&origin).unwrap().with_response_cors(),
        &origin,
    )
    .unwrap()
    .with_cors(policy())
    .unwrap();
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
    client.write_all(b"GET /api/stream HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !received.windows(5).any(|part| part == b"first") {
            let mut buffer = [0; 1024];
            let count = client.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "response ended before its first chunk");
            received.extend_from_slice(&buffer[..count]);
        }
    })
    .await
    .expect("first chunk must arrive before the source produces the second");
    let first = std::str::from_utf8(&received).unwrap();
    assert!(values(first, "access-control-allow-origin").is_empty());
    assert!(values(first, "vary").is_empty());
    assert!(values(first, "x-breeze-response-cors").is_empty());
    chunks.send(Bytes::from_static(b"second")).await.unwrap();
    drop(chunks);
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8(received).unwrap().contains("second"));
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    stop_api.send(()).unwrap();
    api_task.await.unwrap().unwrap();
}
