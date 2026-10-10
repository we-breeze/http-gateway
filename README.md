# Breeze HTTP Gateway

## Application-wide CORS

CORS is disabled by default. Enable it on the gateway to handle preflights
before business route selection and recording admission:

```rust,ignore
use brz_http_gateway::{Cors, Gateway};

let mut cors = Cors::permissive();
cors.allow_credentials = true;
cors.expose_headers = vec!["Content-Disposition".into(), "X-Request-ID".into()];
// Optional application-specific request headers that affect preflight caching.
cors.extra_preflight_vary = vec!["X-Preflight-Variant".into()];
let gateway = Gateway::new(routes, api, &python_upstream)?.with_cors(cors)?;
```

OPTIONS requests with Origin and Access-Control-Request-Method are answered
locally. They need no OPTIONS route entry or business handler, work with an
empty route table and unavailable origins, and do not acquire recording slots
or reach either upstream. This also means downstream Recorders will not capture
those preflights. Ordinary OPTIONS and actual business requests keep their normal
routing. A denied preflight returns 400; a permitted one returns 200 with `OK`.
Preflight responses omit Access-Control-Expose-Headers and vary by request
method/header names, plus Origin when reflected. Additional Vary members can
be configured with `extra_preflight_vary`; they grant no extension permissions.
Actual responses use `expose_headers` and do not inherit preflight-only Vary
members.

The gateway applies the same policy to matched responses, fallback responses,
and gateway error responses, without buffering response bodies. It replaces
upstream CORS fields and preserves other headers, including repeated Set-Cookie
and existing Vary values. Configure CORS once at the public boundary; inner
servers can leave CORS disabled.

Method names and their order are explicit and configurable, including QUERY.
The application supplies its policy; enabling CORS does not discover business
routes or change which methods a handler accepts.

The shared policy is a pinned registry dependency; a sibling checkout is not
required to build or publish this package.

## Overview

`brz-http-gateway` is a reusable HTTP/1.1 migration gateway. Typed route rules
select requests for an application service; every unmatched request is streamed
to a fallback origin. Ordinary proxy paths preserve request and response streaming,
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

## Route exclusions

Use `[[exclude]]` to send matching requests directly to the fallback origin,
even when an exact or wildcard `[[routes]]` entry selects them:

```toml
[[exclude]]
methods = ["GET"]
path = "/api/events"

[[routes]]
path = "/api/*path"
admission = { provider = "recorder", scope = "all-apis", acquire_timeout_ms = 50 }
```

Exclusions take precedence regardless of configuration order. Excluded requests
skip the selected service and `try_acquire`, retaining the fallback's streaming
behavior. In this example, `POST /api/events` remains eligible for recording.
Exclusions use the same exact paths, `:name` parameters and terminal `*name`
templates as routes; omitted or empty `methods` means all methods. Matching uses
only the raw request path, without the query string. Invalid exclusions fail
configuration loading. Provider registration is still validated for every route.

Rust callers constructing `RoutesConfig` literals can use
`RoutesConfig { routes, ..RoutesConfig::default() }` when no exclusions are needed.

## Recording admission

Selected APIs can share non-waiting admission across gateway processes:

```toml
[[routes]]
methods = ["POST"]
path = "/api/orders"
admission = { provider = "recorder", scope = "all-writes", acquire_timeout_ms = 50 }

[[routes]]
methods = ["PUT", "DELETE"]
path = "/api/orders/:id"
admission = { provider = "recorder", scope = "all-writes", acquire_timeout_ms = 50 }
```

Register named `AdmissionProvider` implementations in an `AdmissionRegistry`
and pass it to `Gateway::with_admission`. An unknown provider returns
`GatewayBuildError::MissingProvider` at construction, including with
`Gateway::new`. Overlapping routes use the first matching entry. Rules without
admission retain normal matched-service dispatch; unmatched requests use the
fallback origin.

Rust callers constructing `RouteRule` literals must include `admission: None`
for unrestricted routes. `Gateway::new` now returns `GatewayBuildError`, which
wraps origin validation errors as well as missing-provider errors.

`AcquireOutcome::Busy`, `NoRecorder` and `NotParticipant` send a selected request
to the fallback. Backend errors, invalid recorder URLs, acquisition timeouts
and participation loss during acquisition also use the fallback: the
application request has not been sent to the recorder. `acquire_timeout_ms`
limits only acquisition and defaults to 50 ms. A timed-out Redis reservation
may remain unused until its TTL expires; it does not cause a recorder write.

For acquired recording routes, `OriginService` forwards to the recorder and
replays the request once to the original origin on an explicit HTTP 404.
Connection failures before request dispatch, such as a stopped recorder or
connection refusal, also replay once to the original origin. Failures after
dispatch and other status codes, including 504, do not trigger fallback.
Ordinary migration routes without admission do not retry on either outcome.
Recording requests retain their complete body and trailers in memory for this
retry; responses and other proxy paths remain streamed. HTTP upgrade handshakes
also use the connection-failure retry, but skip the 404 retry and keep the
streaming tunnel after acceptance. Custom `MatchedService`
implementations own their response handling.

An acquired `AdmissionTicket` travels in request extensions and in the reserved
`x-breeze-admission-scope` / `x-breeze-admission-token` headers. Client-supplied
values are removed before dispatch. Dropping a ticket or ending a gateway
connection does not release its reservation. A completion adapter releases it
after case finalization, or Redis reclaims it when its TTL expires.

Enable `redis-recording` for `redis_admission::RedisAdmissionProvider`, which
uses the published `brz-redis` crate as an optional dependency:

```rust,no_run
# #[cfg(feature = "redis-recording")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use std::path::Path;
use brz_http_gateway::{AdmissionRegistry, Gateway, OriginService, RouteTable, RoutesConfig};
use brz_http_gateway::redis_admission::{GatewayLeaseConfig, RedisAdmissionProvider};
use brz_redis::RedisService;

let redis = RedisService::single("127.0.0.1:6379").await?;
let provider = RedisAdmissionProvider::new(
    redis, "recording:orders", "recording:orders:url",
)?
.with_gateway_limit(GatewayLeaseConfig::new("gw-17"))?;
let mut registry = AdmissionRegistry::new();
registry.register("recorder", provider)?;
let routes = RouteTable::compile(RoutesConfig::load(Path::new("routes.toml"))?)?;
let recorder = OriginService::from_admission();
let gateway = Gateway::with_admission(
    routes, recorder, &"http://original:9000".parse()?, registry,
)?;
# let _ = gateway;
# Ok(())
# }
```

Every gateway in a recorder group must enable `with_gateway_limit` before
cloning/registering its provider. This starts one background task shared by its
clones. Use a unique `instance_id` for each gateway, or leave it empty (including
whitespace-only values) to generate a random `gw-<32 hex>` ID at provider
startup. The generated ID stays unchanged across renewals and provider clones;
a new provider startup generates a new ID. Explicit IDs, such as a Pod name or
an application-supplied IP/process label, are preserved. Automatic IDs require
no network-interface discovery. All gateways use the same Redis topology
and key prefix across the group. The global limit N is read from the Redis
string `<key_prefix>:max-gateways`, independent of API rules in `routes.toml`.
Its value must be a nonnegative decimal integer; `0` or a missing key pauses
participation. Invalid values or configuration lookup failures revoke local
eligibility. There is no gateway-local default for N.

At most N gateways own participation slots. Leases default to 30 seconds, with
renewal attempts approximately every 10 seconds and 20% jitter. Standbys start
with a randomized delay and wait 10–12 seconds between unsuccessful elections,
so an incoming request never triggers a quota lookup or election.
`GatewayLeaseConfig::lease_ttl` and `refresh_interval` are configurable; refresh
must be at most one third of TTL. Nonparticipants use a local eligibility check
and send requests straight to the original path without contacting Redis or
the recorder. Renewal failure revokes local eligibility; local eligibility also
expires even if the background task is delayed. Unknown acquisition/renewal
outcomes reconcile the same slot before attempting another.

Dropping the last provider clone stops renewal. Replacement happens after the
slot expires, so the group can temporarily have fewer than N participants.
Each new provider generates a fresh boot token, including when an instance ID
is reused. Successful renewal atomically compares this token before `PEXPIRE`.
Each background cycle reads N from the Redis writer. Increasing N lets standbys
fill the additional slots. Decreasing N revokes slots whose index is outside
`0..N` and releases them with a token-checked delete. Limit changes take effect
as gateways refresh; a lower limit is not enforced instantaneously across all
instances. Already admitted recordings continue and retain their request slot.
`is_participating()` reports local eligibility without a Redis request.
A provider without `with_gateway_limit` is unrestricted; this is
useful for completion adapters, which should not take gateway slots.

With one shared request scope, there are N + 3 keys (all Redis strings). Old
participation keys can briefly remain during shrinking or TTL recovery:

| Key | Value | Expiration |
| --- | --- | --- |
| `recording:orders:url` (configurable) | Plain HTTP origin, or JSON `{"url":"http://10.0.0.12:9001","owner":"<boot token>"}` | Managed by the address publisher |
| `recording:orders:max-gateways` | Decimal N, e.g. `2` | Configuration, no default TTL |
| `recording:orders:gateway:0` through `:gateway:N-1` | `<instance-id>:<32 hex boot token>` | 30 seconds, renewed |
| `recording:orders:inflight:all-writes` | `<64 hex request token>` | 60 seconds |

Publish the recorder's origin-only HTTP URL and global participation limit:

```text
SET recording:orders:url http://10.0.0.12:9001
SET recording:orders:max-gateways 2
```

For a recorder with readiness and heartbeat, `RecorderRegistration::new(redis,
url_key, origin, Duration::from_secs(30))` supplies an exclusive address lease.
`register()` uses SET NX PX with a fresh boot owner in the JSON value; `renew()`
and `unregister()` compare the full value before changing it. Callers publish
after readiness and schedule renewal (for example every 10 seconds). A stale
owner cannot renew or delete a replacement; expiry permits replacement after a
crash. These primitives create no background task on their own. The externally
managed plain URL format remains supported.

For participating gateways, each request first uses `GET <recorder_url_key>` on
the Redis writer unless a local retry cooldown is active. A missing address
permits fallback; failures or invalid URLs are errors and the gateway also
falls back. Missing/invalid discovery, Redis errors and acquisition timeouts
start a 10 second cooldown shared by all provider clones and request scopes.
During that period requests fall back locally without any request-level Redis
commands. The next request after expiry may retry; other concurrent requests
fall back immediately while that attempt runs. Cancellation during either GET
or SET also starts the cooldown. Successful discovery and reservation, including
an explicit SET NX Busy rejection, clear the cooldown. Busy request slots are
not cached, allowing admission as soon as the recorder releases its slot.
After discovery, acquisition uses one
`SET <prefix>:inflight:<scope> <token> NX PX 60000`. All APIs recorded by one
single-case recorder must share the same scope; separate scopes permit separate
concurrent requests.

The recorder needs no start handshake. An outer completion adapter can retain
the ticket from `AdmissionTicket::from_headers` and call
`provider.complete(&ticket)` after the case and its dependencies finalize,
including failed cases whose work has ended. Completion uses a token-checked
atomic Lua delete so a late callback cannot release a newer request. The
business recorder does not need to know the originating gateway or Redis.
`provider.owns(&ticket)` supplies writer-side token validation before dispatch
when the receiving adapter requires it.

`RedisAdmissionProvider::with_ttl` can override the default 60 second request
TTL. There is no request-slot renewal: a case exceeding TTL can overlap the
next admitted request. The recorder retains its local single-case admission
check. If no completion callback runs, including a rejected recording request,
the request slot is recovered by TTL. This crate supplies the provider and
completion API; integration into `traffic-e2e` remains application-owned.

Validation:

```sh
cargo test --locked
BREEZE_REDIS_TEST_ENDPOINT=127.0.0.1:6379 cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
```

Live Redis tests skip when `BREEZE_REDIS_TEST_ENDPOINT` is unset. Redis failure
and malformed-reply fixtures run without an external server.

`serve` applies bounded defaults: 65,536 downstream connections, a 32 KiB
request-head buffer, a 15 second request-head timeout, and `TCP_NODELAY`.
Use `serve_with_config` with `GatewayConfig` when an application needs different
listener limits. Ordinary proxy requests and all responses remain streamed
without a whole-request timeout; acquired recording requests buffer their body
to support the connection-failure and explicit 404 retries described above.

Enable `fallback-log` to emit access events only when a request is forwarded to
the fallback origin. With `brz-logs`, they are written to `fallback.log` in
positional form with method, raw target, status, elapsed time, request length,
and response length. Unknown lengths use `-`:

```text
2026-09-19 18:13:02 [FALLBACK] GET /api/quota?q=a 200 102ms - 133
```

## Crate naming

The package is `brz-http-gateway`; import it as `brz_http_gateway`.
