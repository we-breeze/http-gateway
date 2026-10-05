#![cfg(feature = "redis-recording")]

use std::time::Duration;

use brz_http_gateway::redis_admission::{
    GatewayLeaseConfig, GatewayLeaseConfigError, RecorderRegistration, RedisAdmissionConfigError,
    RedisAdmissionProvider,
};
use brz_http_gateway::{AcquireOutcome, AdmissionProvider, AdmissionTicket};
use brz_redis::{Redis, RedisService, RedisServiceOptions, cmd};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn provider(prefix: &str, ttl: Duration) -> Option<RedisAdmissionProvider> {
    let endpoint = std::env::var("BREEZE_REDIS_TEST_ENDPOINT").ok()?;
    let redis = RedisService::single_with_options(
        endpoint,
        RedisServiceOptions::default().with_timeout(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    let url_key = format!("{prefix}:recorder-url");
    redis.set(&url_key, "http://127.0.0.1:9001").await.unwrap();
    Some(RedisAdmissionProvider::with_ttl(redis, prefix, url_key, ttl).unwrap())
}

fn prefix() -> String {
    format!("http-gateway-test:{:032x}", rand::random::<u128>())
}

async fn acquire(provider: &RedisAdmissionProvider, scope: &str) -> AdmissionTicket {
    match provider.try_acquire(scope).await.unwrap() {
        AcquireOutcome::Acquired(ticket) => ticket,
        AcquireOutcome::Busy => panic!("expected a free slot"),
        AcquireOutcome::NoRecorder => panic!("expected a registered recorder"),
        AcquireOutcome::NotParticipant => panic!("expected a participating gateway"),
    }
}

// brz-net's shared DNS resolver must outlive all clients in this test binary.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    })
}

macro_rules! redis_test {
    ($($name:ident),* $(,)?) => { $(
        #[test]
        fn $name() { runtime().block_on(cases::$name()); }
    )* };
}

redis_test!(
    independent_clients_share_one_slot_and_recorder_owns_completion,
    expiry_recovers_a_slot_and_stale_completion_cannot_release_new_owner,
    discovery_controls_admission_and_default_ttl_is_sixty_seconds,
    redis_errors_propagate_as_errors_not_busy,
    malformed_redis_replies_are_errors_not_busy,
    invalid_provider_configuration_is_rejected,
    two_gateway_instances_share_a_slot_until_recorder_finalization,
    gateway_limit_renews_and_replaces_a_failed_instance,
    standby_requests_do_not_issue_request_level_redis_commands,
    renewal_failure_disables_new_recording_requests,
    renewal_cannot_extend_a_replacement_token,
    gateway_limit_configuration_is_validated,
    unknown_election_outcome_reconciles_the_same_slot,
    redis_limit_controls_growth_shrink_disable_and_recovery,
    malformed_redis_limits_never_enable_participation,
    empty_gateway_ids_are_generated_once_and_unique,
    missing_recorder_is_cached_across_clones_and_recovers_after_ten_seconds,
    redis_acquisition_timeouts_back_off_across_provider_clones,
    invalid_recorder_discovery_and_redis_errors_back_off,
    standby_elections_wait_a_full_refresh_interval,
    registration_is_exclusive_and_owned_renewal_preserves_replacement,
    registration_expiry_permits_replacement_and_json_discovery,
    registration_configuration_is_validated,
);

mod cases {
    use super::*;

    pub async fn registration_is_exclusive_and_owned_renewal_preserves_replacement() {
        let prefix = prefix();
        let Some(provider) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        let redis = RedisService::single(std::env::var("BREEZE_REDIS_TEST_ENDPOINT").unwrap())
            .await
            .unwrap();
        let key = format!("{prefix}:online");
        let first = RecorderRegistration::new(
            redis.clone(),
            &key,
            "http://127.0.0.1:4080".parse().unwrap(),
            Duration::from_secs(30),
        )
        .unwrap();
        let second = RecorderRegistration::new(
            redis.clone(),
            &key,
            "http://127.0.0.1:4090".parse().unwrap(),
            Duration::from_secs(30),
        )
        .unwrap();
        assert!(first.register().await.unwrap());
        assert!(!second.register().await.unwrap());
        assert!(first.renew().await.unwrap());
        let mut ttl = cmd("PTTL");
        ttl.arg(&key);
        assert!(redis.command::<i64>(ttl).await.unwrap() > 20_000);
        assert!(first.unregister().await.unwrap());
        assert!(second.register().await.unwrap());
        assert!(!first.renew().await.unwrap());
        assert!(!first.unregister().await.unwrap());
        let discovery = RedisAdmissionProvider::new(redis, &prefix, &key).unwrap();
        let ticket = acquire(&discovery, "all-apis").await;
        assert_eq!(
            ticket.origin().unwrap().to_string(),
            "http://127.0.0.1:4090/"
        );
        assert!(provider.owns(&ticket).await.unwrap());
        let forged = AdmissionTicket::new("all-apis", "wrong-token").unwrap();
        assert!(!provider.owns(&forged).await.unwrap());
        assert!(provider.complete(&ticket).await.unwrap());
        assert!(!provider.owns(&ticket).await.unwrap());
        assert!(second.unregister().await.unwrap());
    }

    pub async fn registration_expiry_permits_replacement_and_json_discovery() {
        let prefix = prefix();
        let Some(_) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        let redis = RedisService::single(std::env::var("BREEZE_REDIS_TEST_ENDPOINT").unwrap())
            .await
            .unwrap();
        let key = format!("{prefix}:online");
        let first = RecorderRegistration::new(
            redis.clone(),
            &key,
            "http://127.0.0.1:4080".parse().unwrap(),
            Duration::from_millis(200),
        )
        .unwrap();
        let second = RecorderRegistration::new(
            redis.clone(),
            &key,
            "http://127.0.0.1:4090".parse().unwrap(),
            Duration::from_secs(30),
        )
        .unwrap();
        assert!(first.register().await.unwrap());
        tokio::time::timeout(Duration::from_secs(2), async {
            while redis.exists(&key).await.unwrap() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(second.register().await.unwrap());
        assert!(!first.renew().await.unwrap());
        assert!(!first.unregister().await.unwrap());
        assert!(second.unregister().await.unwrap());
    }

    pub async fn registration_configuration_is_validated() {
        let (redis, fixture) = reply_fixture(b"-ERR unused\r\n").await;
        for (key, url, ttl) in [
            ("", "http://127.0.0.1:4080", Duration::from_secs(30)),
            ("url", "http://127.0.0.1:4080/path", Duration::from_secs(30)),
            ("url", "http://127.0.0.1:4080", Duration::ZERO),
            ("url", "http://127.0.0.1:4080", Duration::from_nanos(1)),
        ] {
            assert!(
                RecorderRegistration::new(redis.clone(), key, url.parse().unwrap(), ttl).is_err()
            );
        }
        fixture.abort();
    }

    pub async fn two_gateway_instances_share_a_slot_until_recorder_finalization() {
        use brz_http_gateway::{
            AdmissionRegistry, Gateway, OriginService, RouteTable, RoutesConfig,
        };
        use tokio::sync::oneshot;

        let prefix = prefix();
        let Some(first_provider) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        set_limit(&prefix, 2).await;
        let first_provider = first_provider
            .with_gateway_limit(lease_config("first"))
            .unwrap();
        let second_provider = provider(&prefix, Duration::from_secs(5))
            .await
            .unwrap()
            .with_gateway_limit(lease_config("second"))
            .unwrap();
        wait_until(|| first_provider.is_participating() && second_provider.is_participating())
            .await;
        let recorder_provider = provider(&prefix, Duration::from_secs(5)).await.unwrap();
        let recorder = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recorder_address = recorder.local_addr().unwrap();
        let redis = RedisService::single(std::env::var("BREEZE_REDIS_TEST_ENDPOINT").unwrap())
            .await
            .unwrap();
        redis
            .set(
                format!("{prefix}:recorder-url"),
                format!("http://{recorder_address}"),
            )
            .await
            .unwrap();
        let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback_address = fallback.local_addr().unwrap();
        let (started, ready) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let recorder_task = tokio::spawn(async move {
            let mut started = Some(started);
            let mut released = Some(released);
            for _ in 0..2 {
                let (mut stream, _) = recorder.accept().await.unwrap();
                let head = read_http_head(&mut stream).await;
                let scope = header(&head, "x-breeze-admission-scope").unwrap();
                let token = header(&head, "x-breeze-admission-token").unwrap();
                assert_eq!(scope, "all-writes");
                assert_ne!(token, "forged");
                let ticket = AdmissionTicket::new(scope, token).unwrap();
                let mut body = [0; 7];
                stream.read_exact(&mut body).await.unwrap();
                assert_eq!(&body, b"payload");
                stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n8\r\nrecorded\r\n").await.unwrap();
                if let Some(started) = started.take() {
                    started.send(()).unwrap();
                    released.take().unwrap().await.unwrap();
                }
                assert!(recorder_provider.complete(&ticket).await.unwrap());
                stream.write_all(b"0\r\n\r\n").await.unwrap();
            }
        });
        let original_task = tokio::spawn(async move {
            let (mut stream, _) = fallback.accept().await.unwrap();
            let head = read_http_head(&mut stream).await;
            assert!(header(&head, "x-breeze-admission-token").is_none());
            let mut body = [0; 7];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(&body, b"payload");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\noriginal",
                )
                .await
                .unwrap();
        });
        let mut gateways = Vec::new();
        for provider in [first_provider, second_provider] {
            let config: RoutesConfig = toml::from_str(
                r#"
                [[routes]]
                methods = ["POST"]
                path = "/write"
                admission = { provider = "recorder", scope = "all-writes", acquire_timeout_ms = 2000 }
            "#,
            )
            .unwrap();
            let mut registry = AdmissionRegistry::new();
            registry.register("recorder", provider).unwrap();
            let selected = OriginService::from_admission();
            let gateway = Gateway::with_admission(
                RouteTable::compile(config).unwrap(),
                selected,
                &format!("http://{fallback_address}").parse().unwrap(),
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
            gateways.push((address, stop, task));
        }
        let first_address = gateways[0].0;
        let first_request = tokio::spawn(http_write(first_address));
        tokio::time::timeout(Duration::from_secs(3), ready)
            .await
            .unwrap()
            .unwrap();
        let second_response = http_write(gateways[1].0).await;
        assert!(
            second_response.ends_with("original"),
            "busy request uses original path"
        );
        assert!(
            !first_request.is_finished(),
            "receiving headers/chunks does not release a slot"
        );
        release.send(()).unwrap();
        assert!(first_request.await.unwrap().contains("recorded"));
        assert!(
            http_write(gateways[1].0).await.contains("recorded"),
            "completion admits the next request"
        );
        recorder_task.await.unwrap();
        original_task.await.unwrap();
        for (_, stop, task) in gateways {
            stop.send(()).unwrap();
            task.await.unwrap().unwrap();
        }
    }

    async fn read_http_head(stream: &mut tokio::net::TcpStream) -> String {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(stream.read_u8().await.unwrap());
            assert!(head.len() < 32 * 1024);
        }
        String::from_utf8(head).unwrap()
    }

    fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    }

    async fn http_write(address: std::net::SocketAddr) -> String {
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                b"POST /write HTTP/1.1\r\nHost: public.example\r\nContent-Length: 7\r\n\
            Connection: close, x-breeze-admission-token, x-breeze-admission-scope\r\n\
            x-breeze-admission-token: forged\r\nx-breeze-admission-scope: forged\r\n\r\npayload",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(response).unwrap()
    }

    pub async fn independent_clients_share_one_slot_and_recorder_owns_completion() {
        let prefix = prefix();
        let Some(first) = provider(&prefix, Duration::from_secs(2)).await else {
            return;
        };
        let second = provider(&prefix, Duration::from_secs(2)).await.unwrap();
        let recorder = provider(&prefix, Duration::from_secs(2)).await.unwrap();
        let mut requests = Vec::new();
        for index in 0..20 {
            let provider = if index % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            requests.push(tokio::spawn(async move {
                provider.try_acquire("all-writes").await.unwrap()
            }));
        }
        let mut tickets = Vec::new();
        for request in requests {
            if let AcquireOutcome::Acquired(ticket) = request.await.unwrap() {
                tickets.push(ticket);
            }
        }
        assert_eq!(tickets.len(), 1);
        let ticket = tickets.pop().unwrap();
        assert!(matches!(
            second.try_acquire("all-writes").await.unwrap(),
            AcquireOutcome::Busy
        ));

        // A separate API scope may use a separate slot.
        let other = acquire(&second, "other").await;
        assert!(recorder.complete(&other).await.unwrap());
        assert!(recorder.complete(&ticket).await.unwrap());
        let next = acquire(&second, "all-writes").await;
        assert!(
            !recorder.complete(&ticket).await.unwrap(),
            "old owner cannot release a new slot"
        );
        assert!(!recorder.complete(&ticket).await.unwrap());
        assert!(matches!(
            first.try_acquire("all-writes").await.unwrap(),
            AcquireOutcome::Busy
        ));
        assert!(recorder.complete(&next).await.unwrap());
    }

    pub async fn expiry_recovers_a_slot_and_stale_completion_cannot_release_new_owner() {
        let prefix = prefix();
        let Some(gateway) = provider(&prefix, Duration::from_millis(80)).await else {
            return;
        };
        let recorder = provider(&prefix, Duration::from_millis(80)).await.unwrap();
        let stale = acquire(&gateway, "writes").await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let ticket = acquire(&gateway, "writes").await;
        assert!(!recorder.complete(&stale).await.unwrap());
        assert!(recorder.complete(&ticket).await.unwrap());
        let next = acquire(&gateway, "writes").await;
        assert!(!recorder.complete(&ticket).await.unwrap());
        assert!(recorder.complete(&next).await.unwrap());
    }

    pub async fn redis_errors_propagate_as_errors_not_busy() {
        let (redis, fixture) = reply_fixture(b"-ERR fixture failure\r\n").await;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url").unwrap();
        assert!(provider.try_acquire("writes").await.is_err());
        let invalid = AdmissionTicket::new("writes", "foreign").unwrap();
        assert!(provider.complete(&invalid).await.is_err());
        fixture.abort();
    }

    pub async fn invalid_provider_configuration_is_rejected() {
        let (redis, fixture) = reply_fixture(b"-ERR fixture failure\r\n").await;
        assert!(matches!(
            RedisAdmissionProvider::new(redis.clone(), "", "recorder-url"),
            Err(RedisAdmissionConfigError::EmptyPrefix)
        ));
        assert!(matches!(
            RedisAdmissionProvider::with_ttl(
                redis.clone(),
                "prefix",
                "recorder-url",
                Duration::ZERO
            ),
            Err(RedisAdmissionConfigError::InvalidTtl)
        ));
        assert!(matches!(
            RedisAdmissionProvider::with_ttl(
                redis,
                "prefix",
                "recorder-url",
                Duration::from_nanos(1)
            ),
            Err(RedisAdmissionConfigError::InvalidTtl)
        ));
        fixture.abort();
    }

    pub async fn malformed_redis_replies_are_errors_not_busy() {
        let (redis, fixture) = reply_fixture(b"+unexpected\r\n").await;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url").unwrap();
        assert!(provider.try_acquire("writes").await.is_err());
        let ticket = AdmissionTicket::new("writes", "token").unwrap();
        assert!(provider.complete(&ticket).await.is_err());
        fixture.abort();
    }

    pub async fn discovery_controls_admission_and_default_ttl_is_sixty_seconds() {
        let Ok(endpoint) = std::env::var("BREEZE_REDIS_TEST_ENDPOINT") else {
            return;
        };
        let prefix = prefix();
        let url_key = format!("{prefix}:recorder-url");
        let redis = RedisService::single(endpoint).await.unwrap();
        let provider = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        assert!(matches!(
            provider.try_acquire("writes").await.unwrap(),
            AcquireOutcome::NoRecorder
        ));
        assert!(
            !redis
                .exists(format!("{prefix}:inflight:writes"))
                .await
                .unwrap()
        );
        redis.set(&url_key, "https://127.0.0.1:9001").await.unwrap();
        let invalid = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        assert!(invalid.try_acquire("writes").await.is_err());
        redis.set(&url_key, "http://127.0.0.1:9001").await.unwrap();
        for cached in [&provider, &invalid] {
            assert!(matches!(
                cached.try_acquire("writes").await.unwrap(),
                AcquireOutcome::NoRecorder
            ));
        }
        tokio::time::sleep(Duration::from_millis(10_050)).await;
        let ticket = acquire(&provider, "writes").await;
        assert_eq!(
            ticket.origin().unwrap(),
            &"http://127.0.0.1:9001".parse::<http::Uri>().unwrap()
        );
        let mut ttl = cmd("PTTL");
        ttl.arg(format!("{prefix}:inflight:writes"));
        let ttl: i64 = redis.command(ttl).await.unwrap();
        assert!((55_000..=60_000).contains(&ttl));
        assert!(provider.complete(&ticket).await.unwrap());
        redis.del(url_key).await.unwrap();
    }

    fn lease_config(instance: &str) -> GatewayLeaseConfig {
        let mut config = GatewayLeaseConfig::new(instance);
        config.lease_ttl = Duration::from_millis(900);
        config.refresh_interval = Duration::from_millis(200);
        config
    }

    async fn set_limit(prefix: &str, limit: usize) -> RedisService {
        let redis = RedisService::single(std::env::var("BREEZE_REDIS_TEST_ENDPOINT").unwrap())
            .await
            .unwrap();
        redis
            .set(format!("{prefix}:max-gateways"), limit.to_string())
            .await
            .unwrap();
        redis
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("participation did not reach the expected state");
    }

    pub async fn gateway_limit_renews_and_replaces_a_failed_instance() {
        let prefix = prefix();
        let Some(first) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        set_limit(&prefix, 2).await;
        let mut gateways = vec![Some(
            first.with_gateway_limit(lease_config("gw-0")).unwrap(),
        )];
        for id in 1..6 {
            gateways.push(Some(
                provider(&prefix, Duration::from_secs(5))
                    .await
                    .unwrap()
                    .with_gateway_limit(lease_config(&format!("gw-{id}")))
                    .unwrap(),
            ));
        }
        let count = || {
            gateways
                .iter()
                .flatten()
                .filter(|p| p.is_participating())
                .count()
        };
        wait_until(|| count() == 2).await;
        let selected: Vec<_> = gateways
            .iter()
            .enumerate()
            .filter(|(_, p)| p.as_ref().unwrap().is_participating())
            .map(|(index, _)| index)
            .collect();
        // Renew across multiple TTL periods without replacing healthy owners.
        for _ in 0..100 {
            assert_eq!(count(), 2);
            for index in &selected {
                assert!(gateways[*index].as_ref().unwrap().is_participating());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Both participating gateways still share one request slot.
        let ticket = acquire(gateways[selected[0]].as_ref().unwrap(), "all-writes").await;
        assert!(matches!(
            gateways[selected[1]]
                .as_ref()
                .unwrap()
                .try_acquire("all-writes")
                .await
                .unwrap(),
            AcquireOutcome::Busy
        ));

        // The last clone's drop stops renewal. A standby replaces it after TTL.
        gateways[selected[0]].take();
        wait_until(|| {
            gateways
                .iter()
                .flatten()
                .filter(|p| p.is_participating())
                .count()
                == 2
        })
        .await;
        assert!(gateways[selected[1]].as_ref().unwrap().is_participating());
        assert!(gateways.iter().enumerate().any(|(index, p)| {
            !selected.contains(&index) && p.as_ref().is_some_and(|p| p.is_participating())
        }));
        for candidate in gateways.iter().flatten().filter(|p| p.is_participating()) {
            assert!(
                matches!(
                    candidate.try_acquire("all-writes").await.unwrap(),
                    AcquireOutcome::Busy
                ),
                "replacing a gateway must not release its in-flight recording"
            );
        }
        assert!(
            gateways[selected[1]]
                .as_ref()
                .unwrap()
                .complete(&ticket)
                .await
                .unwrap()
        );
        for _ in 0..30 {
            assert!(
                gateways
                    .iter()
                    .flatten()
                    .filter(|p| p.is_participating())
                    .count()
                    <= 2
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn standby_requests_do_not_issue_request_level_redis_commands() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let election_calls = Arc::new(AtomicUsize::new(0));
        let request_calls = Arc::new(AtomicUsize::new(0));
        let elections = Arc::clone(&election_calls);
        let requests = Arc::clone(&request_calls);
        let (redis, fixture) = command_fixture(move |args| {
            if args[0] == b"GET" && args[1].ends_with(b":max-gateways") {
                Some(b"$1\r\n2\r\n".to_vec())
            } else if args[0] == b"SET" && args[1].windows(9).any(|w| w == b":gateway:") {
                elections.fetch_add(1, Ordering::SeqCst);
                Some(b"$-1\r\n".to_vec())
            } else {
                requests.fetch_add(1, Ordering::SeqCst);
                Some(b"-ERR unexpected request command\r\n".to_vec())
            }
        })
        .await;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url")
            .unwrap()
            .with_gateway_limit(lease_config("standby"))
            .unwrap();
        wait_until(|| election_calls.load(Ordering::SeqCst) >= 2).await;
        for _ in 0..100 {
            assert!(matches!(
                provider.try_acquire("all-writes").await.unwrap(),
                AcquireOutcome::NotParticipant
            ));
        }
        assert_eq!(request_calls.load(Ordering::SeqCst), 0);
        drop(provider);
        fixture.abort();
    }

    pub async fn renewal_failure_disables_new_recording_requests() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let renewal_calls = Arc::new(AtomicUsize::new(0));
        let request_calls = Arc::new(AtomicUsize::new(0));
        let renewals = Arc::clone(&renewal_calls);
        let requests = Arc::clone(&request_calls);
        let (redis, fixture) = command_fixture(move |args| match args[0].as_slice() {
            b"GET" if args[1].ends_with(b":max-gateways") => Some(b"$1\r\n1\r\n".to_vec()),
            b"SET" => Some(b"+OK\r\n".to_vec()),
            b"EVAL" => {
                renewals.fetch_add(1, Ordering::SeqCst);
                None // The renewal has an unknown outcome and times out.
            }
            _ => {
                requests.fetch_add(1, Ordering::SeqCst);
                Some(b"-ERR unexpected request command\r\n".to_vec())
            }
        })
        .await;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url")
            .unwrap()
            .with_gateway_limit(lease_config("gateway"))
            .unwrap();
        wait_until(|| provider.is_participating()).await;
        wait_until(|| renewal_calls.load(Ordering::SeqCst) > 0 && !provider.is_participating())
            .await;
        assert!(matches!(
            provider.try_acquire("all-writes").await.unwrap(),
            AcquireOutcome::NotParticipant
        ));
        assert_eq!(request_calls.load(Ordering::SeqCst), 0);
        drop(provider);
        fixture.abort();
    }

    pub async fn renewal_cannot_extend_a_replacement_token() {
        let prefix = prefix();
        let Some(provider) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        set_limit(&prefix, 1).await;
        let provider = provider
            .with_gateway_limit(lease_config("gateway"))
            .unwrap();
        wait_until(|| provider.is_participating()).await;
        let redis = RedisService::single(std::env::var("BREEZE_REDIS_TEST_ENDPOINT").unwrap())
            .await
            .unwrap();
        let key = format!("{prefix}:gateway:0");
        let original: Option<String> = redis.get(&key).await.unwrap();
        assert!(original.unwrap().starts_with("gateway:"));
        let mut replace = cmd("SET");
        replace
            .arg(&key)
            .arg("replacement:token")
            .arg("PX")
            .arg(3000);
        let _: brz_redis::Value = redis.command(replace).await.unwrap();
        wait_until(|| !provider.is_participating()).await;
        assert_eq!(
            redis.get::<_, String>(&key).await.unwrap().as_deref(),
            Some("replacement:token")
        );
        let mut ttl = cmd("PTTL");
        ttl.arg(&key);
        let ttl: i64 = redis.command(ttl).await.unwrap();
        assert!(
            ttl > 2000,
            "old gateway must not renew the replacement token with its own 900ms TTL"
        );
    }

    pub async fn gateway_limit_configuration_is_validated() {
        let (redis, fixture) = reply_fixture(b"-ERR fixture failure\r\n").await;
        let provider =
            RedisAdmissionProvider::new(redis.clone(), "prefix", "recorder-url").unwrap();
        for key in ["prefix:max-gateways", "prefix:gateway:0"] {
            assert!(matches!(
                RedisAdmissionProvider::new(redis.clone(), "prefix", key)
                    .unwrap()
                    .with_gateway_limit(GatewayLeaseConfig::new("gw")),
                Err(GatewayLeaseConfigError::RecorderKeyConflict)
            ));
        }
        assert!(
            provider
                .clone()
                .with_gateway_limit(GatewayLeaseConfig::new(" "))
                .is_ok()
        );
        let mut config = lease_config("gw");
        config.refresh_interval = config.lease_ttl;
        assert!(matches!(
            provider.clone().with_gateway_limit(config),
            Err(GatewayLeaseConfigError::InvalidTiming)
        ));
        let provider = provider.with_gateway_limit(lease_config("gw")).unwrap();
        assert!(matches!(
            provider.with_gateway_limit(lease_config("gw")),
            Err(GatewayLeaseConfigError::AlreadyConfigured)
        ));
        fixture.abort();
    }

    pub async fn unknown_election_outcome_reconciles_the_same_slot() {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        let set_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&set_calls);
        let owner = Mutex::new(None);
        let (redis, fixture) = command_fixture(move |args| match args[0].as_slice() {
            b"GET" if args[1].ends_with(b":max-gateways") => Some(b"$1\r\n3\r\n".to_vec()),
            b"SET" => {
                calls.fetch_add(1, Ordering::SeqCst);
                *owner.lock().unwrap() = Some((args[1].clone(), args[2].clone()));
                None // Redis accepted the lease, but its response was lost.
            }
            b"EVAL" => {
                let current = owner.lock().unwrap();
                let (key, token) = current.as_ref().unwrap();
                assert_eq!(&args[3], key);
                assert_eq!(&args[4], token);
                Some(b":1\r\n".to_vec())
            }
            _ => panic!("unexpected command"),
        })
        .await;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url")
            .unwrap()
            .with_gateway_limit(lease_config("gateway"))
            .unwrap();
        wait_until(|| provider.is_participating()).await;
        assert_eq!(
            set_calls.load(Ordering::SeqCst),
            1,
            "unknown acquisition must reconcile its key before attempting another slot"
        );
        drop(provider);
        fixture.abort();
    }

    pub async fn redis_limit_controls_growth_shrink_disable_and_recovery() {
        let prefix = prefix();
        let Some(first) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        let redis = RedisService::single(std::env::var("BREEZE_REDIS_TEST_ENDPOINT").unwrap())
            .await
            .unwrap();
        let key = format!("{prefix}:max-gateways");
        let mut gateways = vec![first.with_gateway_limit(lease_config("gw-0")).unwrap()];
        for id in 1..5 {
            gateways.push(
                provider(&prefix, Duration::from_secs(5))
                    .await
                    .unwrap()
                    .with_gateway_limit(lease_config(&format!("gw-{id}")))
                    .unwrap(),
            );
        }
        let count = || gateways.iter().filter(|p| p.is_participating()).count();
        // A missing central configuration does not enable a local default N.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(count(), 0);
        assert!(!redis.exists(format!("{prefix}:gateway:0")).await.unwrap());

        redis.set(&key, "1").await.unwrap();
        wait_until(|| count() == 1).await;
        redis.set(&key, "3").await.unwrap();
        wait_until(|| count() == 3).await;
        redis.set(&key, "1").await.unwrap();
        wait_until(|| count() == 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while redis.exists(format!("{prefix}:gateway:1")).await.unwrap()
                || redis.exists(format!("{prefix}:gateway:2")).await.unwrap()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        redis.set(&key, "0").await.unwrap();
        wait_until(|| count() == 0).await;
        for gateway in &gateways {
            assert!(matches!(
                gateway.try_acquire("all-writes").await.unwrap(),
                AcquireOutcome::NotParticipant
            ));
        }
        redis.del(&key).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(count(), 0);

        redis.set(&key, "2").await.unwrap();
        wait_until(|| count() == 2).await;
        redis.set(&key, "invalid").await.unwrap();
        wait_until(|| count() == 0).await;
        redis.set(&key, "2").await.unwrap();
        wait_until(|| count() == 2).await;
    }

    pub async fn malformed_redis_limits_never_enable_participation() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        for reply in [
            b"$0\r\n\r\n".as_slice(),
            b"$2\r\n-1\r\n".as_slice(),
            b"$2\r\n+1\r\n".as_slice(),
            b"$21\r\n184467440737095516160\r\n".as_slice(),
            b"-ERR config lookup failed\r\n".as_slice(),
        ] {
            let reads = Arc::new(AtomicUsize::new(0));
            let writes = Arc::new(AtomicUsize::new(0));
            let read_calls = Arc::clone(&reads);
            let write_calls = Arc::clone(&writes);
            let (redis, fixture) = command_fixture(move |args| {
                if args[0] == b"GET" && args[1].ends_with(b":max-gateways") {
                    read_calls.fetch_add(1, Ordering::SeqCst);
                    Some(reply.to_vec())
                } else {
                    write_calls.fetch_add(1, Ordering::SeqCst);
                    Some(b"-ERR unexpected command\r\n".to_vec())
                }
            })
            .await;
            let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url")
                .unwrap()
                .with_gateway_limit(lease_config("gateway"))
                .unwrap();
            wait_until(|| reads.load(Ordering::SeqCst) >= 2).await;
            assert!(!provider.is_participating());
            assert_eq!(writes.load(Ordering::SeqCst), 0);
            assert!(matches!(
                provider.try_acquire("all-writes").await.unwrap(),
                AcquireOutcome::NotParticipant
            ));
            drop(provider);
            fixture.abort();
        }
    }

    pub async fn empty_gateway_ids_are_generated_once_and_unique() {
        let prefix = prefix();
        let Some(first) = provider(&prefix, Duration::from_secs(5)).await else {
            return;
        };
        let redis = set_limit(&prefix, 2).await;
        let first = first.with_gateway_limit(lease_config("")).unwrap();
        let second = provider(&prefix, Duration::from_secs(5))
            .await
            .unwrap()
            .with_gateway_limit(lease_config("  \t"))
            .unwrap();
        wait_until(|| first.is_participating() && second.is_participating()).await;
        let first_key = format!("{prefix}:gateway:0");
        let second_key = format!("{prefix}:gateway:1");
        let first_owner = redis.get::<_, String>(&first_key).await.unwrap().unwrap();
        let second_owner = redis.get::<_, String>(&second_key).await.unwrap().unwrap();
        let first_id = first_owner.rsplit_once(':').unwrap().0;
        let second_id = second_owner.rsplit_once(':').unwrap().0;
        assert!(!first_id.trim().is_empty());
        assert!(!second_id.trim().is_empty());
        assert_ne!(
            first_id, second_id,
            "independent gateways on the same host need distinct IDs"
        );
        let cloned = first.clone();
        drop(first);
        // The generated ID and boot token remain stable across cloning and
        // more than one lease TTL; renewal must not regenerate either value.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(cloned.is_participating() && second.is_participating());
        assert_eq!(
            redis.get::<_, String>(&first_key).await.unwrap().as_deref(),
            Some(first_owner.as_str())
        );
        assert_eq!(
            redis
                .get::<_, String>(&second_key)
                .await
                .unwrap()
                .as_deref(),
            Some(second_owner.as_str())
        );
    }

    fn recorder_url_reply() -> Vec<u8> {
        let url = "http://127.0.0.1:9001";
        format!("${}\r\n{url}\r\n", url.len()).into_bytes()
    }

    pub async fn missing_recorder_is_cached_across_clones_and_recovers_after_ten_seconds() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        let available = Arc::new(AtomicBool::new(false));
        let busy = Arc::new(AtomicBool::new(false));
        let gets = Arc::new(AtomicUsize::new(0));
        let sets = Arc::new(AtomicUsize::new(0));
        let (is_available, is_busy, get_calls, set_calls) = (
            Arc::clone(&available),
            Arc::clone(&busy),
            Arc::clone(&gets),
            Arc::clone(&sets),
        );
        let (redis, fixture) = command_fixture(move |args| match args[0].as_slice() {
            b"GET" => {
                get_calls.fetch_add(1, Ordering::SeqCst);
                Some(if is_available.load(Ordering::SeqCst) {
                    recorder_url_reply()
                } else {
                    b"$-1\r\n".to_vec()
                })
            }
            b"SET" => {
                set_calls.fetch_add(1, Ordering::SeqCst);
                Some(if is_busy.load(Ordering::SeqCst) {
                    b"$-1\r\n".to_vec()
                } else {
                    b"+OK\r\n".to_vec()
                })
            }
            _ => panic!("unexpected discovery command"),
        })
        .await;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url").unwrap();
        assert!(matches!(
            provider.try_acquire("writes").await.unwrap(),
            AcquireOutcome::NoRecorder
        ));
        available.store(true, Ordering::SeqCst);
        let mut requests = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let cloned = provider.clone();
            requests.spawn(async move {
                assert!(matches!(
                    cloned.try_acquire("another-scope").await.unwrap(),
                    AcquireOutcome::NoRecorder
                ));
            });
        }
        while let Some(result) = requests.join_next().await {
            result.unwrap();
        }
        assert_eq!(gets.load(Ordering::SeqCst), 1);
        assert_eq!(sets.load(Ordering::SeqCst), 0);
        tokio::time::sleep(Duration::from_millis(10_050)).await;
        let _ticket = acquire(&provider, "writes").await;
        assert_eq!(gets.load(Ordering::SeqCst), 2);
        assert_eq!(sets.load(Ordering::SeqCst), 1);
        // Request-slot contention is not an unavailable-recorder cooldown.
        busy.store(true, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(matches!(
                provider.try_acquire("writes").await.unwrap(),
                AcquireOutcome::Busy
            ));
        }
        assert_eq!(gets.load(Ordering::SeqCst), 4);
        assert_eq!(sets.load(Ordering::SeqCst), 3);
        fixture.abort();
    }

    pub async fn redis_acquisition_timeouts_back_off_across_provider_clones() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        for blocked in [b"GET".as_slice(), b"SET".as_slice()] {
            let commands = Arc::new(AtomicUsize::new(0));
            let calls = Arc::clone(&commands);
            let (redis, fixture) = command_fixture(move |args| {
                calls.fetch_add(1, Ordering::SeqCst);
                if args[0] == blocked {
                    None
                } else {
                    Some(recorder_url_reply())
                }
            })
            .await;
            let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url").unwrap();
            let active = provider.clone();
            let request = tokio::spawn(async move {
                tokio::time::timeout(Duration::from_millis(50), active.try_acquire("writes")).await
            });
            let expected = if blocked == b"GET" { 1 } else { 2 };
            wait_until(|| commands.load(Ordering::SeqCst) == expected).await;
            // Concurrent requests neither wait nor duplicate the Redis attempt.
            for _ in 0..100 {
                assert!(matches!(
                    provider.try_acquire("writes").await.unwrap(),
                    AcquireOutcome::Busy | AcquireOutcome::NoRecorder
                ));
            }
            assert!(request.await.unwrap().is_err());
            for _ in 0..100 {
                assert!(matches!(
                    provider.clone().try_acquire("writes").await.unwrap(),
                    AcquireOutcome::NoRecorder
                ));
            }
            assert_eq!(commands.load(Ordering::SeqCst), expected);
            fixture.abort();
        }
    }

    pub async fn invalid_recorder_discovery_and_redis_errors_back_off() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        for reply in [
            b"-ERR discovery unavailable\r\n".as_slice(),
            b"$22\r\nhttps://127.0.0.1:9001\r\n".as_slice(),
        ] {
            let commands = Arc::new(AtomicUsize::new(0));
            let calls = Arc::clone(&commands);
            let (redis, fixture) = command_fixture(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Some(reply.to_vec())
            })
            .await;
            let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url").unwrap();
            assert!(provider.try_acquire("writes").await.is_err());
            for _ in 0..100 {
                assert!(matches!(
                    provider.clone().try_acquire("writes").await.unwrap(),
                    AcquireOutcome::NoRecorder
                ));
            }
            assert_eq!(commands.load(Ordering::SeqCst), 1);
            fixture.abort();
        }
    }

    pub async fn standby_elections_wait_a_full_refresh_interval() {
        use std::sync::{Arc, Mutex};
        let reads = Arc::new(Mutex::new(Vec::new()));
        let read_times = Arc::clone(&reads);
        let (redis, fixture) = command_fixture(move |args| match args[0].as_slice() {
            b"GET" if args[1].ends_with(b":max-gateways") => {
                read_times.lock().unwrap().push(tokio::time::Instant::now());
                Some(b"$1\r\n1\r\n".to_vec())
            }
            b"SET" => Some(b"$-1\r\n".to_vec()),
            _ => panic!("standby issued a request-level Redis command"),
        })
        .await;
        assert_eq!(
            GatewayLeaseConfig::new("standby").refresh_interval,
            Duration::from_secs(10)
        );
        let config = lease_config("standby");
        let interval = config.refresh_interval;
        let provider = RedisAdmissionProvider::new(redis, prefix(), "recorder-url")
            .unwrap()
            .with_gateway_limit(config)
            .unwrap();
        wait_until(|| !reads.lock().unwrap().is_empty()).await;
        for _ in 0..100 {
            assert!(matches!(
                provider.clone().try_acquire("writes").await.unwrap(),
                AcquireOutcome::NotParticipant
            ));
        }
        wait_until(|| reads.lock().unwrap().len() >= 3).await;
        for pair in reads.lock().unwrap().windows(2) {
            assert!(pair[1].duration_since(pair[0]) >= interval);
        }
        drop(provider);
        fixture.abort();
    }

    async fn reply_fixture(reply: &'static [u8]) -> (RedisService, tokio::task::JoinHandle<()>) {
        command_fixture(move |args| {
            if args[0] == b"GET" && args[1].ends_with(b":max-gateways") {
                Some(b"$1\r\n1\r\n".to_vec())
            } else if args[0] == b"GET" {
                let url = "http://127.0.0.1:9001";
                Some(format!("${}\r\n{url}\r\n", url.len()).into_bytes())
            } else {
                Some(reply.to_vec())
            }
        })
        .await
    }

    async fn command_fixture(
        reply: impl Fn(Vec<Vec<u8>>) -> Option<Vec<u8>> + Send + Sync + 'static,
    ) -> (RedisService, tokio::task::JoinHandle<()>) {
        use std::sync::Arc;
        use tokio::io::{AsyncBufReadExt, BufReader};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let reply = Arc::new(reply);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let reply = Arc::clone(&reply);
                connections.spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                        let mut args = Vec::new();
                        for _ in 0..count {
                            line.clear();
                            reader.read_line(&mut line).await.unwrap();
                            let len: usize =
                                line.trim().strip_prefix('$').unwrap().parse().unwrap();
                            let mut bytes = vec![0; len + 2];
                            reader.read_exact(&mut bytes).await.unwrap();
                            bytes.truncate(len);
                            args.push(bytes);
                        }
                        if let Some(response) = reply(args) {
                            if writer.write_all(&response).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        });
        let redis = RedisService::single_with_options(
            address.to_string(),
            RedisServiceOptions::default().with_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap();
        (redis, task)
    }
}
