#![cfg(feature = "redis-recording")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brz_http_gateway::redis_admission::{GatewayLeaseConfig, RedisAdmissionProvider};
use brz_http_gateway::{AcquireOutcome, AdmissionProvider, AdmissionTicket};
use brz_redis::{Redis, RedisService, RedisServiceOptions, Value, cmd};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const PREFIX: &str = "policy-test";
const URL_KEY: &str = "policy-test:url";
const COUNTS_KEY: &str = "policy-test:api-counts";

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

macro_rules! policy_test {
    ($($name:ident),* $(,)?) => { $(
        #[test]
        fn $name() { runtime().block_on(cases::$name()); }
    )* };
}

#[derive(Default)]
struct State {
    policy: Mutex<Option<String>>,
    url: Mutex<Option<String>>,
    counts: Mutex<HashMap<String, String>>,
    locks: Mutex<HashMap<String, String>>,
    commands: Mutex<Vec<Vec<String>>>,
    fail: Mutex<Option<String>>,
    override_reply: Mutex<Option<(String, Vec<u8>)>>,
}

impl State {
    fn reply(&self, args: Vec<String>) -> Vec<u8> {
        self.commands.lock().unwrap().push(args.clone());
        if self.fail.lock().unwrap().as_deref() == Some(args[0].as_str()) {
            return b"-ERR fixture failure\r\n".to_vec();
        }
        if let Some((command, reply)) = &*self.override_reply.lock().unwrap() {
            if *command == args[0] {
                return reply.clone();
            }
        }
        match args[0].as_str() {
            "GET" if args[1].ends_with(":api-policy") => {
                bulk(self.policy.lock().unwrap().as_deref())
            }
            "GET" if args[1].ends_with(":max-gateways") => bulk(Some("1")),
            "GET" if args[1] == URL_KEY => bulk(self.url.lock().unwrap().as_deref()),
            "HGET" => {
                assert_eq!(args[1], COUNTS_KEY);
                bulk(
                    self.counts
                        .lock()
                        .unwrap()
                        .get(&args[2])
                        .map(String::as_str),
                )
            }
            "HINCRBY" => {
                assert_eq!(args[1], COUNTS_KEY);
                assert_eq!(args[3], "1");
                let mut counts = self.counts.lock().unwrap();
                let count = counts
                    .get(&args[2])
                    .map_or(0, |value| value.parse::<i64>().unwrap())
                    + 1;
                counts.insert(args[2].clone(), count.to_string());
                format!(":{count}\r\n").into_bytes()
            }
            "SET" => {
                assert_eq!(args[3], "NX");
                assert_eq!(args[4], "PX");
                let mut locks = self.locks.lock().unwrap();
                if locks.contains_key(&args[1]) {
                    bulk(None)
                } else {
                    locks.insert(args[1].clone(), args[2].clone());
                    b"+OK\r\n".to_vec()
                }
            }
            "EVAL" => {
                let mut locks = self.locks.lock().unwrap();
                let owned = locks.get(&args[3]) == Some(&args[4]);
                if owned && args[1].contains("'DEL'") {
                    locks.remove(&args[3]);
                }
                format!(":{}\r\n", i32::from(owned)).into_bytes()
            }
            "GET" => bulk(self.locks.lock().unwrap().get(&args[1]).map(String::as_str)),
            _ => panic!("unexpected command {args:?}"),
        }
    }

    fn take_commands(&self) -> Vec<Vec<String>> {
        std::mem::take(&mut *self.commands.lock().unwrap())
    }
}

fn bulk(value: Option<&str>) -> Vec<u8> {
    value.map_or_else(
        || b"$-1\r\n".to_vec(),
        |text| format!("${}\r\n{text}\r\n", text.len()).into_bytes(),
    )
}

struct Fixture {
    provider: RedisAdmissionProvider,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(policy: Option<&str>) -> Self {
        let state = Arc::new(State::default());
        *state.policy.lock().unwrap() = policy.map(str::to_owned);
        *state.url.lock().unwrap() = Some("http://127.0.0.1:9001".to_owned());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                let state = Arc::clone(&server);
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
                            args.push(String::from_utf8(bytes).unwrap());
                        }
                        if writer.write_all(&state.reply(args)).await.is_err() {
                            break;
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
        Self {
            provider: RedisAdmissionProvider::new(redis, PREFIX, URL_KEY).unwrap(),
            state,
            task,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn ticket(outcome: AcquireOutcome) -> AdmissionTicket {
    match outcome {
        AcquireOutcome::Acquired(ticket) => ticket,
        other => panic!("expected admission: {other:?}"),
    }
}

fn names(commands: &[Vec<String>]) -> Vec<&str> {
    commands.iter().map(|command| command[0].as_str()).collect()
}

policy_test!(
    url_is_first_and_missing_recorder_never_reads_counts,
    success_uses_four_commands_and_patterns_share_counts,
    exhausted_quota_can_resume_after_counts_are_deleted,
    busy_slot_does_not_increment_or_start_cooldown,
    local_policy_rejects_without_redis_and_legacy_calls_still_work,
    clones_share_reload_and_invalid_policy_preserves_last_good_snapshot,
    absent_policy_allows_all_without_counter_commands,
    invalid_counts_and_increment_errors_return_errors,
    policy_errors_do_not_revoke_gateway_leases,
    periodic_policy_refresh_updates_clones_without_resetting_counts,
    global_gateway_shares_path_counts_across_methods,
    redis_admission_faults_preserve_http_requests,
    invalid_policy_loads_preserve_http_behavior,
    invalid_initial_policy_bypasses_recording_and_recovers,
    live_redis_clients_share_counts_and_resume_after_reset,
);

/// A loopback RESP fixture comparison, including completion in both paths.
/// This is not a production Redis latency benchmark.
#[test]
#[ignore]
fn loopback_admission_cost() {
    runtime().block_on(async {
        let fixture = Fixture::new(Some(r#"{"case-limit":1000000}"#)).await;
        fixture.provider.load_policy().await.unwrap();
        let warmup = ticket(fixture.provider.try_acquire("all-apis").await.unwrap());
        fixture.provider.complete(&warmup).await.unwrap();
        for capped in [false, true] {
            let started = std::time::Instant::now();
            for _ in 0..500 {
                let result = if capped {
                    fixture
                        .provider
                        .try_acquire_request("all-apis", "/api")
                        .await
                } else {
                    fixture.provider.try_acquire("all-apis").await
                };
                fixture
                    .provider
                    .complete(&ticket(result.unwrap()))
                    .await
                    .unwrap();
                fixture.state.take_commands();
            }
            eprintln!(
                "loopback RESP fixture, capped={capped}, admission + completion: {:.1} us/case",
                started.elapsed().as_micros() as f64 / 500.0
            );
        }
    });
}

mod cases {
    use super::*;

    pub async fn url_is_first_and_missing_recorder_never_reads_counts() {
        let fixture = Fixture::new(Some(r#"{"case-limit":200}"#)).await;
        fixture.provider.load_policy().await.unwrap();
        fixture.state.take_commands();
        *fixture.state.url.lock().unwrap() = None;
        for _ in 0..3 {
            assert!(matches!(
                fixture
                    .provider
                    .try_acquire_request("all-apis", "/api")
                    .await
                    .unwrap(),
                AcquireOutcome::NoRecorder
            ));
        }
        assert_eq!(fixture.state.take_commands(), vec![vec!["GET", URL_KEY]]);
    }

    pub async fn success_uses_four_commands_and_patterns_share_counts() {
        let fixture = Fixture::new(Some(
            r#"{"case-limit":2,"includes":[{"path":"/"},{"path":"/api/:id/xxx","match":"exact"}]}"#,
        ))
        .await;
        fixture.provider.load_policy().await.unwrap();
        fixture.state.take_commands();
        let first = ticket(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api/123/xxx")
                .await
                .unwrap(),
        );
        let commands = fixture.state.take_commands();
        assert_eq!(names(&commands), ["GET", "HGET", "SET", "HINCRBY"]);
        assert_eq!(commands[0], ["GET", URL_KEY]);
        assert_eq!(commands[1], ["HGET", COUNTS_KEY, "/api/:id/xxx"]);
        assert_eq!(commands[2][1], "policy-test:inflight:all-apis");
        assert_eq!(commands[2][5], "60000");
        assert_eq!(commands[3], ["HINCRBY", COUNTS_KEY, "/api/:id/xxx", "1"]);
        assert!(fixture.provider.complete(&first).await.unwrap());
        let second = ticket(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api/456/xxx")
                .await
                .unwrap(),
        );
        assert!(fixture.provider.complete(&second).await.unwrap());
        assert!(matches!(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api/789/xxx")
                .await
                .unwrap(),
            AcquireOutcome::QuotaExhausted
        ));
        assert_eq!(
            fixture
                .state
                .counts
                .lock()
                .unwrap()
                .get("/api/:id/xxx")
                .unwrap(),
            "2"
        );
    }

    pub async fn exhausted_quota_can_resume_after_counts_are_deleted() {
        let fixture = Fixture::new(Some(r#"{"case-limit":2}"#)).await;
        fixture.provider.load_policy().await.unwrap();
        fixture
            .state
            .counts
            .lock()
            .unwrap()
            .insert("/api".into(), "2".into());
        fixture.state.take_commands();
        for _ in 0..2 {
            assert!(matches!(
                fixture
                    .provider
                    .try_acquire_request("all-apis", "/api")
                    .await
                    .unwrap(),
                AcquireOutcome::QuotaExhausted
            ));
            assert_eq!(names(&fixture.state.take_commands()), ["GET", "HGET"]);
        }
        fixture.state.counts.lock().unwrap().clear();
        let _ = ticket(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api")
                .await
                .unwrap(),
        );
        assert_eq!(
            fixture.state.counts.lock().unwrap().get("/api").unwrap(),
            "1"
        );
    }

    pub async fn busy_slot_does_not_increment_or_start_cooldown() {
        let fixture = Fixture::new(Some(r#"{"case-limit":2}"#)).await;
        fixture.provider.load_policy().await.unwrap();
        fixture
            .state
            .locks
            .lock()
            .unwrap()
            .insert("policy-test:inflight:all-apis".into(), "other".into());
        fixture.state.take_commands();
        assert!(matches!(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api")
                .await
                .unwrap(),
            AcquireOutcome::Busy
        ));
        assert_eq!(
            names(&fixture.state.take_commands()),
            ["GET", "HGET", "SET"]
        );
        assert!(fixture.state.counts.lock().unwrap().is_empty());
        fixture.state.locks.lock().unwrap().clear();
        let _ = ticket(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api")
                .await
                .unwrap(),
        );
    }

    pub async fn local_policy_rejects_without_redis_and_legacy_calls_still_work() {
        let fixture = Fixture::new(Some(
            r#"{"case-limit":2,"includes":[{"path":"/api"}],"excludes":[{"path":"/api/private"}]}"#,
        ))
        .await;
        assert!(matches!(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api")
                .await
                .unwrap(),
            AcquireOutcome::NotEligible
        ));
        assert!(fixture.state.take_commands().is_empty());
        fixture.provider.load_policy().await.unwrap();
        fixture.state.take_commands();
        for path in ["/outside", "/apix", "/api/private/1"] {
            assert!(matches!(
                fixture
                    .provider
                    .try_acquire_request("all-apis", path)
                    .await
                    .unwrap(),
                AcquireOutcome::NotEligible
            ));
        }
        assert!(fixture.state.take_commands().is_empty());
        let _ = ticket(fixture.provider.try_acquire("all-apis").await.unwrap());
        assert_eq!(names(&fixture.state.take_commands()), ["GET", "SET"]);
    }

    pub async fn clones_share_reload_and_invalid_policy_preserves_last_good_snapshot() {
        let fixture = Fixture::new(Some(r#"{"includes":[{"path":"/first"}]}"#)).await;
        let clone = fixture.provider.clone();
        clone.load_policy().await.unwrap();
        assert!(matches!(
            fixture
                .provider
                .try_acquire_request("all-apis", "/second")
                .await
                .unwrap(),
            AcquireOutcome::NotEligible
        ));
        *fixture.state.policy.lock().unwrap() = Some("invalid JSON".into());
        assert!(fixture.provider.load_policy().await.is_err());
        assert!(matches!(
            clone
                .try_acquire_request("all-apis", "/second")
                .await
                .unwrap(),
            AcquireOutcome::NotEligible
        ));
        let retained = ticket(
            clone
                .try_acquire_request("all-apis", "/first/retained")
                .await
                .unwrap(),
        );
        assert!(clone.complete(&retained).await.unwrap());
        *fixture.state.policy.lock().unwrap() = Some(r#"{"case-limit":0}"#.into());
        fixture.provider.load_policy().await.unwrap();
        fixture.state.take_commands();
        assert!(matches!(
            clone
                .try_acquire_request("all-apis", "/first")
                .await
                .unwrap(),
            AcquireOutcome::NotEligible
        ));
        assert!(fixture.state.take_commands().is_empty());
    }

    pub async fn absent_policy_allows_all_without_counter_commands() {
        let fixture = Fixture::new(None).await;
        fixture.provider.load_policy().await.unwrap();
        fixture.state.take_commands();
        let _ = ticket(
            fixture
                .provider
                .try_acquire_request("all-apis", "/unlisted")
                .await
                .unwrap(),
        );
        assert_eq!(names(&fixture.state.take_commands()), ["GET", "SET"]);
        assert!(fixture.state.counts.lock().unwrap().is_empty());
    }

    pub async fn invalid_counts_and_increment_errors_return_errors() {
        for bad_count in ["-1", "wrong", "18446744073709551616"] {
            let fixture = Fixture::new(Some(r#"{"case-limit":2}"#)).await;
            fixture.provider.load_policy().await.unwrap();
            fixture
                .state
                .counts
                .lock()
                .unwrap()
                .insert("/api".into(), bad_count.into());
            fixture.state.take_commands();
            assert!(
                fixture
                    .provider
                    .try_acquire_request("all-apis", "/api")
                    .await
                    .is_err()
            );
            assert_eq!(names(&fixture.state.take_commands()), ["GET", "HGET"]);
        }
        let fixture = Fixture::new(Some(r#"{"case-limit":2}"#)).await;
        fixture.provider.load_policy().await.unwrap();
        *fixture.state.fail.lock().unwrap() = Some("HINCRBY".into());
        assert!(
            fixture
                .provider
                .try_acquire_request("all-apis", "/api")
                .await
                .is_err()
        );
        assert!(fixture.state.counts.lock().unwrap().is_empty());
    }

    pub async fn policy_errors_do_not_revoke_gateway_leases() {
        let fixture = Fixture::new(Some("invalid JSON")).await;
        let provider = fixture
            .provider
            .clone()
            .with_gateway_limit(GatewayLeaseConfig {
                instance_id: "policy-errors".into(),
                lease_ttl: Duration::from_millis(300),
                refresh_interval: Duration::from_millis(60),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !provider.is_participating() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(provider.is_participating());
        let commands = fixture.state.take_commands();
        assert!(
            commands
                .iter()
                .filter(|cmd| cmd[0] == "GET" && cmd[1].ends_with(":api-policy"))
                .count()
                > 2
        );
        assert!(commands.iter().filter(|cmd| cmd[0] == "EVAL").count() > 2);
    }

    pub async fn periodic_policy_refresh_updates_clones_without_resetting_counts() {
        let fixture = Fixture::new(Some(r#"{"case-limit":0}"#)).await;
        fixture
            .state
            .counts
            .lock()
            .unwrap()
            .insert("/api".into(), "1".into());
        let provider = fixture
            .provider
            .clone()
            .with_gateway_limit(GatewayLeaseConfig {
                instance_id: "refresh-policy".into(),
                lease_ttl: Duration::from_millis(300),
                refresh_interval: Duration::from_millis(60),
            })
            .unwrap();
        let observer = provider.clone();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !observer.is_participating() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            observer
                .try_acquire_request("all-apis", "/api")
                .await
                .unwrap(),
            AcquireOutcome::NotEligible
        ));
        *fixture.state.policy.lock().unwrap() = Some(r#"{"case-limit":2}"#.into());
        let admitted = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match observer
                    .try_acquire_request("all-apis", "/api")
                    .await
                    .unwrap()
                {
                    AcquireOutcome::Acquired(ticket) => break ticket,
                    AcquireOutcome::NotEligible | AcquireOutcome::NotParticipant => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    other => panic!("unexpected refresh outcome {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert!(provider.complete(&admitted).await.unwrap());
        assert_eq!(
            fixture.state.counts.lock().unwrap().get("/api").unwrap(),
            "2"
        );
        assert!(matches!(
            provider
                .try_acquire_request("all-apis", "/api")
                .await
                .unwrap(),
            AcquireOutcome::QuotaExhausted
        ));
    }

    pub async fn global_gateway_shares_path_counts_across_methods() {
        super::http_case::exercise().await;
    }

    pub async fn redis_admission_faults_preserve_http_requests() {
        super::http_case::admission_faults().await;
    }

    pub async fn invalid_policy_loads_preserve_http_behavior() {
        super::http_case::policy_faults().await;
    }

    pub async fn invalid_initial_policy_bypasses_recording_and_recovers() {
        super::http_case::invalid_initial_policy().await;
    }

    pub async fn live_redis_clients_share_counts_and_resume_after_reset() {
        let Ok(endpoint) = std::env::var("BREEZE_REDIS_TEST_ENDPOINT") else {
            return;
        };
        let redis = RedisService::single(&endpoint).await.unwrap();
        let second = RedisService::single(&endpoint).await.unwrap();
        let prefix = format!("gateway-policy-live:{:032x}", rand::random::<u128>());
        let url_key = format!("{prefix}:url");
        let policy_key = format!("{prefix}:api-policy");
        let counts_key = format!("{prefix}:api-counts");
        redis.set(&url_key, "http://127.0.0.1:9001").await.unwrap();
        redis
            .set(
                &policy_key,
                r#"{"case-limit":3,"includes":[{"path":"/api/:id","match":"exact"}]}"#,
            )
            .await
            .unwrap();
        let first = RedisAdmissionProvider::new(redis.clone(), &prefix, &url_key).unwrap();
        let second = RedisAdmissionProvider::new(second, &prefix, &url_key).unwrap();
        first.load_policy().await.unwrap();
        second.load_policy().await.unwrap();
        for index in 0..3 {
            let (owner, other) = if index % 2 == 0 {
                (&first, &second)
            } else {
                (&second, &first)
            };
            let path = format!("/api/{index}");
            let admission = ticket(owner.try_acquire_request("all-apis", &path).await.unwrap());
            assert!(matches!(
                other
                    .try_acquire_request("all-apis", "/api/other")
                    .await
                    .unwrap(),
                AcquireOutcome::Busy | AcquireOutcome::QuotaExhausted
            ));
            assert!(other.owns(&admission).await.unwrap());
            assert!(other.complete(&admission).await.unwrap());
        }
        let mut count = cmd("HGET");
        count.arg(&counts_key).arg("/api/:id");
        let count: String = redis.command(count).await.unwrap();
        assert_eq!(count, "3");
        assert!(matches!(
            first
                .try_acquire_request("all-apis", "/api/last")
                .await
                .unwrap(),
            AcquireOutcome::QuotaExhausted
        ));
        redis.del(&counts_key).await.unwrap();
        let admission = ticket(
            first
                .try_acquire_request("all-apis", "/api/new-round")
                .await
                .unwrap(),
        );
        assert!(second.complete(&admission).await.unwrap());
        let mut cleanup = cmd("DEL");
        cleanup
            .arg(&url_key)
            .arg(&policy_key)
            .arg(&counts_key)
            .arg(format!("{prefix}:inflight:all-apis"));
        let _: Value = redis.command(cleanup).await.unwrap();
    }
}

#[path = "redis_policy/http_case.rs"]
mod http_case;
