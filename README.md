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
# match_kind = "exact" # or "prefix"
```

Only an origin-only `http://` fallback URL is accepted. TLS termination and
HTTP/2 can be provided by an outer ingress; the local fallback hop remains
HTTP/1.1 so upgrade tunneling is explicit and deterministic.

## Crate naming

The package is `brz-http-gateway`; import it as `brz_http_gateway`.
