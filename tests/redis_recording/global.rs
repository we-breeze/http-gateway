use super::*;
use brz_http_gateway::AdmissionError;
use brz_redis::{ErrorKind, RedisError};
use sha1::{Digest, Sha1};
use std::sync::{Arc, Mutex};

async fn registry() -> Option<(RedisService, String, String)> {
    let endpoint = std::env::var("BREEZE_REDIS_TEST_ENDPOINT").ok()?;
    let redis = RedisService::single(endpoint).await.unwrap();
    let prefix = prefix();
    let url_key = format!("{prefix}:url");
    Some((redis, prefix, url_key))
}

fn register(
    redis: RedisService,
    url_key: &str,
    max_inflight: u32,
) -> RecorderRegistration<RedisService> {
    RecorderRegistration::new(
        redis,
        url_key,
        "http://127.0.0.1:9001".parse().unwrap(),
        Duration::from_secs(30),
    )
    .unwrap()
    .with_max_inflight(std::num::NonZeroU32::new(max_inflight).unwrap())
    .unwrap()
}

fn acquired(outcome: AcquireOutcome) -> AdmissionTicket {
    let AcquireOutcome::Acquired(ticket) = outcome else {
        panic!("global admission failed: {outcome:?}");
    };
    assert!(ticket.is_global());
    ticket
}

const SCRIPT_CACHE_PREFIX: &str = "script-cache-test";
const SCRIPT_CACHE_URL_KEY: &str = "script-cache-test:url";
const RESERVATIONS_SCRIPT: &[u8] = include_bytes!("../../src/redis_admission/reservations.lua");

#[derive(Default)]
struct ScriptCache {
    commands: Vec<&'static str>,
    address: Option<String>,
    loaded: bool,
    load_denied: bool,
}

impl ScriptCache {
    fn reply(&mut self, args: &[Vec<u8>], digest: &str) -> Vec<u8> {
        let (name, reply) = match args[0].as_slice() {
            b"SCRIPT" if args[1] == b"LOAD" => {
                assert_eq!(args[2], RESERVATIONS_SCRIPT);
                if self.load_denied {
                    ("SCRIPT LOAD", b"-NOPERM script load denied\r\n".to_vec())
                } else {
                    self.loaded = true;
                    ("SCRIPT LOAD", bulk_reply(digest))
                }
            }
            b"SCRIPT" if args[1] == b"EXISTS" => {
                assert_eq!(args[2], digest.as_bytes());
                (
                    "SCRIPT EXISTS",
                    format!("*1\r\n:{}\r\n", u8::from(self.loaded)).into_bytes(),
                )
            }
            b"SET" => {
                assert_eq!(args[1], SCRIPT_CACHE_URL_KEY.as_bytes());
                assert!(self.loaded);
                self.address = Some(String::from_utf8(args[2].clone()).unwrap());
                ("SET", b"+OK\r\n".to_vec())
            }
            b"GET" => {
                assert_eq!(args[1], SCRIPT_CACHE_URL_KEY.as_bytes());
                ("GET", bulk_reply(self.address.as_deref().unwrap()))
            }
            b"EVAL" => {
                assert_eq!(args[3], SCRIPT_CACHE_URL_KEY.as_bytes());
                assert_eq!(args[4], self.address.as_deref().unwrap().as_bytes());
                ("EVAL", b":1\r\n".to_vec())
            }
            b"EVALSHA" => {
                assert_eq!(args[1], digest.as_bytes());
                assert_eq!(args[2], b"1");
                assert_eq!(args[3], b"script-cache-test:inflight");
                let name = match args[4].as_slice() {
                    b"reserve" => "EVALSHA reserve",
                    b"owns" => "EVALSHA owns",
                    b"renew" => "EVALSHA renew",
                    b"complete" => "EVALSHA complete",
                    _ => panic!("unexpected reservation operation"),
                };
                let reply = if self.loaded {
                    b":1\r\n".to_vec()
                } else {
                    b"-NOSCRIPT No matching script. Please use EVAL.\r\n".to_vec()
                };
                (name, reply)
            }
            _ => panic!("unexpected script cache command {args:?}"),
        };
        self.commands.push(name);
        reply
    }
}

fn bulk_reply(value: &str) -> Vec<u8> {
    format!("${}\r\n{value}\r\n", value.len()).into_bytes()
}

async fn script_cache_fixture(
    state: &Arc<Mutex<ScriptCache>>,
) -> (RedisService, tokio::task::JoinHandle<()>) {
    let state = Arc::clone(state);
    let digest = format!("{:x}", Sha1::digest(RESERVATIONS_SCRIPT));
    super::cases::command_fixture(move |args| Some(state.lock().unwrap().reply(&args, &digest)))
        .await
}

#[test]
fn global_script_is_preloaded_and_only_heartbeat_repairs_cache_loss() {
    runtime().block_on(async {
        let state = Arc::new(Mutex::new(ScriptCache::default()));
        let (redis, fixture) = script_cache_fixture(&state).await;
        let registration = register(redis.clone(), SCRIPT_CACHE_URL_KEY, 2);
        let provider =
            RedisAdmissionProvider::new(redis, SCRIPT_CACHE_PREFIX, SCRIPT_CACHE_URL_KEY).unwrap();
        let expect_commands = |expected: &[&str]| {
            assert_eq!(
                std::mem::take(&mut state.lock().unwrap().commands),
                expected
            );
        };

        assert!(registration.register().await.unwrap());
        expect_commands(&["SCRIPT LOAD", "SET"]);
        provider.load_policy().await.unwrap();
        let ticket = acquired(provider.try_acquire_global_request("/one").await.unwrap());
        assert!(provider.owns(&ticket).await.unwrap());
        assert!(provider.renew(&ticket).await.unwrap());
        assert!(provider.complete(&ticket).await.unwrap());
        expect_commands(&[
            "GET",
            "EVALSHA reserve",
            "EVALSHA owns",
            "EVALSHA renew",
            "EVALSHA complete",
        ]);

        state.lock().unwrap().loaded = false;
        let error = provider
            .try_acquire_global_request("/two")
            .await
            .unwrap_err();
        let AdmissionError::Backend(error) = error else {
            panic!("expected Redis script-cache error");
        };
        let redis_error = error.downcast_ref::<RedisError>().unwrap();
        assert_eq!(redis_error.kind(), ErrorKind::NoScript);
        expect_commands(&["GET", "EVALSHA reserve"]);

        assert!(registration.renew().await.unwrap());
        expect_commands(&["EVAL", "SCRIPT EXISTS", "SCRIPT LOAD"]);
        assert!(registration.renew().await.unwrap());
        expect_commands(&["EVAL", "SCRIPT EXISTS"]);
        fixture.abort();
    });
}

#[test]
fn global_address_is_not_published_when_script_load_fails() {
    runtime().block_on(async {
        let state = Arc::new(Mutex::new(ScriptCache {
            load_denied: true,
            ..ScriptCache::default()
        }));
        let (redis, fixture) = script_cache_fixture(&state).await;
        let registration = register(redis, SCRIPT_CACHE_URL_KEY, 2);
        assert!(registration.register().await.is_err());
        assert_eq!(state.lock().unwrap().commands, ["SCRIPT LOAD"]);
        fixture.abort();
    });
}

#[test]
fn global_pool_admits_concurrent_requests_and_recovers_only_expired_tokens() {
    runtime().block_on(async {
        let Some((redis, prefix, url_key)) = registry().await else {
            return;
        };
        let registration = register(redis.clone(), &url_key, 2);
        assert!(registration.register().await.unwrap());
        assert!(registration.renew().await.unwrap());
        let first = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        let second = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        first.load_policy().await.unwrap();
        second.load_policy().await.unwrap();

        // Both requests share the provider and enter discovery at the same time.
        let (one, two) = tokio::join!(
            first.try_acquire_global_request("/one"),
            first.try_acquire_global_request("/two"),
        );
        let one = acquired(one.unwrap());
        let two = acquired(two.unwrap());
        assert_ne!(one.token(), two.token());
        assert!(matches!(
            second.try_acquire_global_request("/three").await.unwrap(),
            AcquireOutcome::Busy
        ));
        assert!(second.owns(&one).await.unwrap());
        assert!(second.renew(&two).await.unwrap());

        let mut expire_one = cmd("ZADD");
        expire_one
            .arg(format!("{prefix}:inflight"))
            .arg(0)
            .arg(one.token());
        assert_eq!(redis.command::<i64>(expire_one).await.unwrap(), 0);
        assert!(!second.renew(&one).await.unwrap());
        assert!(!second.complete(&one).await.unwrap());
        let three = acquired(second.try_acquire_global_request("/three").await.unwrap());
        assert!(first.owns(&two).await.unwrap());
        assert!(first.owns(&three).await.unwrap());
        assert!(first.complete(&three).await.unwrap());
        assert!(second.complete(&two).await.unwrap());
        assert!(!redis.exists(format!("{prefix}:inflight")).await.unwrap());
        assert!(registration.unregister().await.unwrap());
    });
}

#[test]
fn global_admission_requires_explicit_protocol_even_at_capacity_one() {
    runtime().block_on(async {
        let Some((redis, prefix, url_key)) = registry().await else {
            return;
        };
        let legacy = RecorderRegistration::new(
            redis.clone(),
            &url_key,
            "http://127.0.0.1:9001".parse().unwrap(),
            Duration::from_secs(30),
        )
        .unwrap();
        assert!(legacy.register().await.unwrap());
        let old = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        old.load_policy().await.unwrap();
        assert!(matches!(
            old.try_acquire_global_request("/route").await.unwrap(),
            AcquireOutcome::NoRecorder
        ));
        let scoped = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        let scoped_ticket = acquire(&scoped, "existing-scope").await;
        assert!(!scoped_ticket.is_global());
        assert!(scoped.complete(&scoped_ticket).await.unwrap());
        assert!(legacy.unregister().await.unwrap());

        let registration = register(redis.clone(), &url_key, 1);
        assert!(registration.register().await.unwrap());
        assert!(matches!(
            scoped.try_acquire("existing-scope").await.unwrap(),
            AcquireOutcome::NoRecorder
        ));
        let global = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        global.load_policy().await.unwrap();
        let ticket = acquired(global.try_acquire_global_request("/route").await.unwrap());
        assert!(matches!(
            global.try_acquire_global_request("/other").await.unwrap(),
            AcquireOutcome::Busy
        ));
        assert!(redis.exists(format!("{prefix}:inflight")).await.unwrap());
        assert!(
            !redis
                .exists(format!("{prefix}:inflight:existing-scope"))
                .await
                .unwrap()
        );
        assert!(global.complete(&ticket).await.unwrap());
        assert!(registration.unregister().await.unwrap());
    });
}

#[test]
fn concurrent_global_admissions_keep_exact_api_quota_and_release_rejected_slots() {
    runtime().block_on(async {
        let Some((redis, prefix, url_key)) = registry().await else {
            return;
        };
        let registration = register(redis.clone(), &url_key, 2);
        assert!(registration.register().await.unwrap());
        let policy_key = format!("{prefix}:api-policy");
        redis.set(&policy_key, r#"{"case-limit":1}"#).await.unwrap();
        let provider = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        provider.load_policy().await.unwrap();
        let (first, second) = tokio::join!(
            provider.try_acquire_global_request("/limited"),
            provider.try_acquire_global_request("/limited"),
        );
        let ticket = match (first.unwrap(), second.unwrap()) {
            (AcquireOutcome::Acquired(ticket), AcquireOutcome::QuotaExhausted)
            | (AcquireOutcome::QuotaExhausted, AcquireOutcome::Acquired(ticket)) => ticket,
            outcomes => panic!("unexpected API quota outcomes: {outcomes:?}"),
        };
        let other = acquired(provider.try_acquire_global_request("/other").await.unwrap());
        assert!(provider.complete(&ticket).await.unwrap());
        assert!(provider.complete(&other).await.unwrap());
        assert!(registration.unregister().await.unwrap());
        redis.del(&policy_key).await.unwrap();
        redis.del(format!("{prefix}:api-counts")).await.unwrap();
    });
}
