//! HTTP routes select per request; successful upgrades bind a connection pair.
use brz_http_gateway::{
    Gateway, GatewayResponse, MatchedService, OriginService, RouteRule, RouteTable, RoutesConfig,
};
use brz_http_server::{Handler, NoAuthenticator, Request, Response, Server, StatusCode, Upgrade};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
struct Backend(&'static [u8]);
impl Handler for Backend {
    async fn call(&self, request: Request<'_>, _: &NoAuthenticator) -> Response {
        if request.path() == "/http" {
            return Response::static_bytes(StatusCode::OK, self.0);
        }
        let name = self.0;
        Upgrade::accept(&request, "test", move |mut io| async move {
            loop {
                let mut byte = [0];
                if io.read(&mut byte).await? == 0 {
                    return Ok(());
                }
                io.write_all(name).await?;
            }
        })
        .unwrap_or_else(|_| Response::empty(StatusCode::BAD_REQUEST))
    }
}
#[derive(Clone)]
struct Select {
    origins: [OriginService; 2],
    calls: Arc<AtomicUsize>,
}
impl MatchedService for Select {
    async fn call(
        &self,
        request: http::Request<hyper::body::Incoming>,
        peer: SocketAddr,
    ) -> GatewayResponse {
        let index = self.calls.fetch_add(1, Ordering::SeqCst) % 2;
        self.origins[index].call(request, peer).await
    }
}
async fn head(client: &mut TcpStream) -> String {
    timeout(Duration::from_secs(3), async {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(client.read_u8().await.unwrap());
        }
        String::from_utf8(head).unwrap()
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn ordinary_keepalive_routes_per_request_and_two_upgrades_remain_on_selected_backends() {
    let a = Server::bind("127.0.0.1:0".parse().unwrap(), Backend(b"A"))
        .await
        .unwrap();
    let b = Server::bind("127.0.0.1:0".parse().unwrap(), Backend(b"B"))
        .await
        .unwrap();
    let uri_a = format!("http://{}", a.local_addr().unwrap())
        .parse()
        .unwrap();
    let uri_b = format!("http://{}", b.local_addr().unwrap())
        .parse()
        .unwrap();
    let task_a = tokio::spawn(a.serve_until(std::future::pending()));
    let task_b = tokio::spawn(b.serve_until(std::future::pending()));
    let calls = Arc::new(AtomicUsize::new(0));
    let matched = Select {
        origins: [
            OriginService::new(&uri_a).unwrap(),
            OriginService::new(&uri_b).unwrap(),
        ],
        calls: calls.clone(),
    };
    let routes = RouteTable::compile(RoutesConfig {
        routes: ["/http", "/ws"]
            .map(|path| RouteRule {
                path: path.into(),
                methods: vec!["GET".into()],
                admission: None,
            })
            .into(),
        ..Default::default()
    })
    .unwrap();
    let gateway = Gateway::new(routes, matched, &uri_a).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(brz_http_gateway::serve(
        listener,
        gateway,
        async {
            stopped.await.ok();
        },
        Duration::from_millis(100),
    ));
    let mut http = TcpStream::connect(address).await.unwrap();
    for expected in *b"AB" {
        http.write_all(b"GET /http HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .unwrap();
        assert!(head(&mut http).await.starts_with("HTTP/1.1 200"));
        assert_eq!(http.read_u8().await.unwrap(), expected);
    }
    drop(http);
    let mut one = TcpStream::connect(address).await.unwrap();
    let mut two = TcpStream::connect(address).await.unwrap();
    for client in [&mut one, &mut two] {
        client
            .write_all(
                b"GET /ws HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n",
            )
            .await
            .unwrap();
        assert!(head(client).await.starts_with("HTTP/1.1 101"));
    }
    for _ in 0..3 {
        one.write_all(b"x").await.unwrap();
        two.write_all(b"x").await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(3), one.read_u8())
                .await
                .unwrap()
                .unwrap(),
            b'A'
        );
        assert_eq!(
            timeout(Duration::from_secs(3), two.read_u8())
                .await
                .unwrap()
                .unwrap(),
            b'B'
        );
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "frames must not trigger route selection"
    );
    stop.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Forced gateway shutdown closes both established tunnels.
    assert_eq!(
        timeout(Duration::from_secs(3), one.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        timeout(Duration::from_secs(3), two.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    task_a.abort();
    task_b.abort();
}
