use std::sync::{Arc, Mutex};
use std::time::Duration;

use brz_redis::{Redis, Value, cmd};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{COMPLETE, reserve};
use crate::AdmissionError;

const RENEW: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"#;

/// Instance-level participation, independent of the per-request recording TTL.
/// Defaults to 30 second leases and 10 second refresh attempts with jitter.
/// The global instance limit is read from `<key_prefix>:max-gateways` in Redis.
#[derive(Clone, Debug)]
pub struct GatewayLeaseConfig {
    /// Gateway label. Empty or whitespace-only values generate a fresh random
    /// ID when the provider starts; explicit values are preserved.
    pub instance_id: String,
    pub lease_ttl: Duration,
    pub refresh_interval: Duration,
}

impl GatewayLeaseConfig {
    #[must_use]
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            lease_ttl: Duration::from_secs(30),
            refresh_interval: Duration::from_secs(10),
        }
    }

    fn validate(&self) -> Result<(), GatewayLeaseConfigError> {
        if self.lease_ttl.as_millis() == 0
            || self.lease_ttl.as_millis() > i64::MAX as u128
            || self.lease_ttl.subsec_nanos() % 1_000_000 != 0
            || self.refresh_interval.as_millis() == 0
            || self.refresh_interval.subsec_nanos() % 1_000_000 != 0
            || self.refresh_interval > self.lease_ttl / 3
        {
            return Err(GatewayLeaseConfigError::InvalidTiming);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayLeaseConfigError {
    #[error(
        "lease TTL and refresh interval must be positive whole milliseconds; refresh must be at most one third of TTL"
    )]
    InvalidTiming,
    #[error("gateway participation is already configured")]
    AlreadyConfigured,
    #[error("recorder URL key conflicts with a gateway participation key")]
    RecorderKeyConflict,
    #[error("gateway participation requires a running Tokio runtime")]
    NoRuntime,
}

pub(super) struct GatewayLease {
    valid_until: Arc<Mutex<Option<Instant>>>,
    task: JoinHandle<()>,
}

impl GatewayLease {
    pub(super) fn start<R: Redis + 'static>(
        redis: Arc<R>,
        prefix: String,
        mut config: GatewayLeaseConfig,
    ) -> Result<Self, GatewayLeaseConfigError> {
        config.validate()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| GatewayLeaseConfigError::NoRuntime)?;
        if config.instance_id.trim().is_empty() {
            config.instance_id = format!("gw-{:032x}", rand::random::<u128>());
        }
        let valid_until = Arc::new(Mutex::new(None));
        let state = Arc::clone(&valid_until);
        // Restarting the same instance ID never reuses the old lease token.
        let token = format!("{}:{:032x}", config.instance_id, rand::random::<u128>());
        let task = runtime.spawn(run(redis, prefix, token, config, state));
        Ok(Self { valid_until, task })
    }

    pub(super) fn is_valid(&self) -> bool {
        is_valid(&self.valid_until)
    }
}

impl Drop for GatewayLease {
    fn drop(&mut self) {
        // No asynchronous destructor: after the last provider clone drops,
        // renewal stops and Redis TTL allows another instance to take over.
        self.task.abort();
    }
}

async fn run<R: Redis + 'static>(
    redis: Arc<R>,
    prefix: String,
    token: String,
    config: GatewayLeaseConfig,
    state: Arc<Mutex<Option<Instant>>>,
) {
    // Avoid a synchronized election when a deployment starts many gateways.
    tokio::time::sleep(config.refresh_interval.mul_f64(rand::random::<f64>())).await;
    let mut owned_slot = None;
    loop {
        let result = tokio::time::timeout(
            config.refresh_interval,
            refresh(
                redis.as_ref(),
                &prefix,
                &token,
                &config,
                &state,
                &mut owned_slot,
            ),
        )
        .await
        .unwrap_or(Err(AdmissionError::Timeout));
        if let Err(error) = result {
            clear(&state);
            // Keep a possibly owned slot for reconciliation. An unknown
            // SET/renew/release result must not cause another slot acquisition.
            tracing::warn!(instance = %config.instance_id, %error,
                "gateway participation refresh failed");
        }
        // Standbys wait at least one full interval before competing again.
        // Renewals may run slightly earlier to protect an existing lease.
        let jitter = if is_valid(&state) {
            0.8 + rand::random::<f64>() * 0.4
        } else {
            1.0 + rand::random::<f64>() * 0.2
        };
        tokio::time::sleep(config.refresh_interval.mul_f64(jitter)).await;
    }
}

fn is_valid(state: &Mutex<Option<Instant>>) -> bool {
    state
        .lock()
        .expect("participation state lock poisoned")
        .is_some_and(|deadline| Instant::now() < deadline)
}

async fn refresh<R: Redis>(
    redis: &R,
    prefix: &str,
    token: &str,
    config: &GatewayLeaseConfig,
    state: &Mutex<Option<Instant>>,
    owned_slot: &mut Option<usize>,
) -> Result<(), AdmissionError> {
    // The limit is read on the writer once per background cycle, never for
    // each incoming HTTP request. Changes propagate on the next refresh.
    let limit = read_limit(redis, &format!("{prefix}:max-gateways")).await?;
    if let Some(slot) = *owned_slot {
        if slot >= limit {
            clear(state);
            let key = format!("{prefix}:gateway:{slot}");
            transition(redis, COMPLETE, &key, &[token]).await?;
            *owned_slot = None;
        }
    }
    if limit == 0 {
        clear(state);
        return Ok(());
    }
    if let Some(slot) = *owned_slot {
        let key = format!("{prefix}:gateway:{slot}");
        let started = Instant::now();
        if renew(redis, &key, token, config.lease_ttl.as_millis() as u64).await? {
            publish(state, started, config.lease_ttl);
        } else {
            clear(state);
            *owned_slot = None;
        }
        return Ok(());
    }
    let first = rand::random::<usize>() % limit;
    for slot in (first..limit).chain(0..first) {
        let key = format!("{prefix}:gateway:{slot}");
        let started = Instant::now();
        // Set before awaiting so cancellation/timeouts retain the possible
        // owner, even when Redis accepted SET but its response was lost.
        *owned_slot = Some(slot);
        if reserve(redis, &key, token, config.lease_ttl.as_millis() as u64).await? {
            publish(state, started, config.lease_ttl);
            break;
        }
        *owned_slot = None;
    }
    Ok(())
}

async fn read_limit<R: Redis>(redis: &R, key: &str) -> Result<usize, AdmissionError> {
    let mut lookup = cmd("GET");
    lookup.arg(key); // Writer-side lookup; replicas may lag configuration changes.
    let result: Value = redis
        .command(lookup)
        .await
        .map_err(AdmissionError::backend)?;
    match result {
        Value::Nil => Ok(0),
        Value::BulkString(bytes) if !bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit) => {
            std::str::from_utf8(&bytes)
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or_else(invalid_limit)
        }
        _ => Err(invalid_limit()),
    }
}

fn invalid_limit() -> AdmissionError {
    AdmissionError::backend(std::io::Error::other(
        "Redis max-gateways must be a nonnegative decimal integer that fits usize",
    ))
}

fn publish(state: &Mutex<Option<Instant>>, started: Instant, ttl: Duration) {
    // Start the local validity window before sending to Redis. A delayed
    // response cannot extend eligibility past the Redis lease's earliest TTL.
    *state.lock().expect("participation state lock poisoned") = Some(started + ttl);
}

fn clear(state: &Mutex<Option<Instant>>) {
    *state.lock().expect("participation state lock poisoned") = None;
}

async fn renew<R: Redis>(
    redis: &R,
    key: &str,
    token: &str,
    ttl_ms: u64,
) -> Result<bool, AdmissionError> {
    let ttl = ttl_ms.to_string();
    transition(redis, RENEW, key, &[token, ttl.as_str()]).await
}

async fn transition<R: Redis>(
    redis: &R,
    script: &str,
    key: &str,
    args: &[&str],
) -> Result<bool, AdmissionError> {
    let result: Value = redis
        .eval(script, &[key], args)
        .await
        .map_err(AdmissionError::backend)?;
    match result {
        Value::Int(0) => Ok(false),
        Value::Int(1) => Ok(true),
        _ => Err(AdmissionError::backend(std::io::Error::other(
            "unexpected Redis participation transition response",
        ))),
    }
}
