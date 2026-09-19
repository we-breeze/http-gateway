# Breeze HTTP Gateway

`brz-http-gateway` is a reusable HTTP/1.1 migration gateway. Typed route rules
select requests for an application service; every unmatched request is streamed
to a fallback origin. The transport preserves request and response streaming,
repeated headers such as `Set-Cookie`, connection cancellation/backpressure, and
HTTP upgrades such as WebSocket.

The crate contains no Wegent or Python-specific behavior. An empty route table
is a transparent all-fallback configuration.

```rust,no_run
use brz_http_gateway::{Gateway, RejectMatched, RouteTable};
use http::Uri;

let upstream: Uri = "http://127.0.0.1:9000".parse()?;
let gateway = Gateway::new(RouteTable::empty(), RejectMatched, &upstream)?;
# Ok::<_, Box<dyn std::error::Error>>(gateway)
```

Use `OriginService` as the selected service when matched routes are hosted by a
separate HTTP server. This keeps the gateway protocol layer independent from
the application framework behind it.

Route configuration is TOML:

```toml
routes = []

# [[routes]]
# methods = ["GET", "HEAD"]
# path = "/api/health"
# Literal paths are exact. Use `:name` for one path segment or terminal
# `*name` for the remaining path segments.
```

Only an origin-only `http://` fallback URL is accepted. TLS termination and
HTTP/2 can be provided by an outer ingress; the local fallback hop remains
HTTP/1.1 so upgrade tunneling is explicit and deterministic.

`serve` applies bounded defaults: 65,536 downstream connections, a 32 KiB
request-head buffer, a 15 second request-head timeout, and `TCP_NODELAY`.
Use `serve_with_config` with `GatewayConfig` when an application needs different
listener limits. Request and response bodies remain streamed rather than being
buffered or subject to a whole-request timeout.

Enable `fallback-log` to emit access events only when a request is forwarded to
the fallback origin. With `brz-logs`, they are written to `fallback.log` in
positional form with method, raw target, status, elapsed time, request length,
and response length. Unknown lengths use `-`:

```text
2026-09-19 18:13:02 [FALLBACK] GET /api/quota?q=a 200 102ms - 133
```

## Crate naming

The package is `brz-http-gateway`; import it as `brz_http_gateway`.
