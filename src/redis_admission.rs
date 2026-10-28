//! Redis-backed, single-slot recording admission across gateway processes.
//!
//! A successful acquisition expires after 60 seconds by default. Recording
//! requires no start/claim handshake. Completion can release the same token
//! earlier, without deleting a newer acquisition after TTL expiry. Receivers
//! can explicitly renew a live reservation while its case is still running.

use std::sync::Arc;
use std::time::Duration;

use brz_redis::{Redis, RedisService, Value, cmd};

use crate::{AcquireOutcome, AdmissionError, AdmissionFuture, AdmissionProvider, AdmissionTicket};

mod discovery;
mod lease;
mod policy;
mod registration;
use discovery::{LookupUnavailable, RecorderDiscovery};
use lease::GatewayLease;
pub use lease::{GatewayLeaseConfig, GatewayLeaseConfigError};
pub use registration::RecorderRegistration;

const COMPLETE: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"#;

const RENEW: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"#;

pub const DEFAULT_RECORDING_TTL: Duration = Duration::from_secs(60);

/// One shared slot per `(key_prefix, scope)` on the configured Redis writer.
/// Gateway and recorder must use the same prefix, scope and Redis topology.
pub struct RedisAdmissionProvider<R = RedisService> {
    redis: Arc<R>,
    key_prefix: String,
    recorder_url_key: String,
    ttl_ms: u64,
    participation: Option<Arc<GatewayLease>>,
    discovery: Arc<RecorderDiscovery>,
    policy: Arc<policy::PolicyStore>,
}

impl<R> Clone for RedisAdmissionProvider<R> {
    fn clone(&self) -> Self {
        Self {
            redis: Arc::clone(&self.redis),
            key_prefix: self.key_prefix.clone(),
            recorder_url_key: self.recorder_url_key.clone(),
            ttl_ms: self.ttl_ms,
            participation: self.participation.clone(),
            discovery: Arc::clone(&self.discovery),
            policy: Arc::clone(&self.policy),
        }
    }
}

impl<R: Redis + 'static> RedisAdmissionProvider<R> {
    /// Uses the application's existing client with a 60 second TTL.
    pub fn new(
        redis: R,
        key_prefix: impl Into<String>,
        recorder_url_key: impl Into<String>,
    ) -> Result<Self, RedisAdmissionConfigError> {
        Self::with_ttl(redis, key_prefix, recorder_url_key, DEFAULT_RECORDING_TTL)
    }

    /// Overrides the TTL. When it expires a new request may be admitted even
    /// if the old case is still executing. No automatic renewal is performed.
    pub fn with_ttl(
        redis: R,
        key_prefix: impl Into<String>,
        recorder_url_key: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self, RedisAdmissionConfigError> {
        let key_prefix = key_prefix.into();
        let recorder_url_key = recorder_url_key.into();
        if key_prefix.is_empty() {
            return Err(RedisAdmissionConfigError::EmptyPrefix);
        }
        if recorder_url_key.is_empty() {
            return Err(RedisAdmissionConfigError::EmptyRecorderKey);
        }
        if recorder_url_key == format!("{key_prefix}:api-policy")
            || recorder_url_key == format!("{key_prefix}:api-counts")
        {
            return Err(RedisAdmissionConfigError::RecorderKeyConflict);
        }
        let ms = ttl.as_millis();
        if ms == 0 || ms > i64::MAX as u128 || ttl.subsec_nanos() % 1_000_000 != 0 {
            return Err(RedisAdmissionConfigError::InvalidTtl);
        }
        let policy = Arc::new(policy::PolicyStore::new(&key_prefix));
        Ok(Self {
            redis: Arc::new(redis),
            key_prefix,
            recorder_url_key,
            ttl_ms: ms as u64,
            participation: None,
            discovery: Arc::new(RecorderDiscovery::default()),
            policy,
        })
    }

    /// Loads `<key_prefix>:api-policy` once and atomically publishes a compiled
    /// snapshot to all clones. Missing policy means all paths with no case cap.
    /// Invalid/failed loads preserve the last valid snapshot. Calls are bounded
    /// to 200 ms, including waiting for another load. Counts are never reset.
    pub async fn load_policy(&self) -> Result<(), AdmissionError> {
        self.policy.load(self.redis.as_ref()).await
    }

    /// Returns the configured request-reservation TTL. Receivers can schedule
    /// renewal before expiry, for example every `provider.reservation_ttl() / 3`.
    #[must_use]
    pub fn reservation_ttl(&self) -> Duration {
        Duration::from_millis(self.ttl_ms)
    }

    /// Enables background election using `<key_prefix>:max-gateways` in Redis.
    /// Configure before cloning/registering the provider. Gateways sharing the
    /// prefix read the same limit. Requires a running Tokio runtime.
    /// The last clone's drop stops renewal; the slot then expires by TTL.
    pub fn with_gateway_limit(
        mut self,
        config: GatewayLeaseConfig,
    ) -> Result<Self, GatewayLeaseConfigError> {
        if self.participation.is_some() {
            return Err(GatewayLeaseConfigError::AlreadyConfigured);
        }
        if self.recorder_url_key == format!("{}:max-gateways", self.key_prefix)
            || self
                .recorder_url_key
                .strip_prefix(&format!("{}:gateway:", self.key_prefix))
                .and_then(|slot| slot.parse::<usize>().ok())
                .is_some()
        {
            return Err(GatewayLeaseConfigError::RecorderKeyConflict);
        }
        self.participation = Some(Arc::new(GatewayLease::start(
            Arc::clone(&self.redis),
            self.key_prefix.clone(),
            config,
            Arc::clone(&self.policy),
        )?));
        Ok(self)
    }

    /// Local-only eligibility check. Without `with_gateway_limit`, this
    /// low-level provider permits participation unconditionally.
    #[must_use]
    pub fn is_participating(&self) -> bool {
        self.participation
            .as_ref()
            .is_none_or(|lease| lease.is_valid())
    }

    /// Recorder-side release after the case and all dependencies finish. An old
    /// ticket cannot release another reservation. Safe to retry with this token.
    pub async fn complete(&self, ticket: &AdmissionTicket) -> Result<bool, AdmissionError> {
        self.transition(COMPLETE, ticket, &[ticket.token()]).await
    }

    /// Extends a live reservation by this provider's configured TTL. The
    /// receiver owns renewal scheduling until the case and dependencies finish.
    /// Returns false if the reservation expired or its token was replaced;
    /// renewal never recreates a missing slot or extends a different owner.
    pub async fn renew(&self, ticket: &AdmissionTicket) -> Result<bool, AdmissionError> {
        let ttl = self.ttl_ms.to_string();
        self.transition(RENEW, ticket, &[ticket.token(), &ttl])
            .await
    }

    /// Receiver-side validation before dispatching a forwarded reservation.
    /// Uses the writer so an expired or replaced token cannot start a case.
    pub async fn owns(&self, ticket: &AdmissionTicket) -> Result<bool, AdmissionError> {
        let mut command = cmd("GET");
        command.arg(self.key(ticket.scope()));
        let result: Value = self
            .redis
            .command(command)
            .await
            .map_err(AdmissionError::backend)?;
        match result {
            Value::Nil => Ok(false),
            Value::BulkString(value) => Ok(value == ticket.token().as_bytes()),
            _ => Err(AdmissionError::backend(std::io::Error::other(
                "unexpected Redis reservation response",
            ))),
        }
    }

    async fn transition(
        &self,
        script: &str,
        ticket: &AdmissionTicket,
        args: &[&str],
    ) -> Result<bool, AdmissionError> {
        let key = self.key(ticket.scope());
        let result: Value = self
            .redis
            .eval(script, &[key.as_str()], args)
            .await
            .map_err(AdmissionError::backend)?;
        match result {
            Value::Int(0) => Ok(false),
            Value::Int(1) => Ok(true),
            _ => Err(AdmissionError::backend(std::io::Error::other(
                "unexpected Redis admission transition response",
            ))),
        }
    }

    fn key(&self, scope: &str) -> String {
        format!("{}:inflight:{scope}", self.key_prefix)
    }
}

impl<R: Redis + 'static> RedisAdmissionProvider<R> {
    async fn acquire(
        &self,
        scope: &str,
        quota: Option<(&str, u64)>,
    ) -> Result<AcquireOutcome, AdmissionError> {
        if !self.is_participating() {
            return Ok(AcquireOutcome::NotParticipant);
        }
        if self.key(scope) == self.recorder_url_key {
            return Err(AdmissionError::backend(std::io::Error::other(
                "recorder URL key conflicts with slot key",
            )));
        }
        let lookup_guard = match self.discovery.try_lookup() {
            Ok(guard) => guard,
            Err(LookupUnavailable::CoolingDown) => return Ok(AcquireOutcome::NoRecorder),
            Err(LookupUnavailable::InProgress) => return Ok(AcquireOutcome::Busy),
        };
        let mut lookup = cmd("GET");
        lookup.arg(&self.recorder_url_key);
        // Route discovery to the writer, avoiding stale replica reads.
        let result: Value = self
            .redis
            .command(lookup)
            .await
            .map_err(AdmissionError::backend)?;
        let origin = match result {
            Value::Nil => return Ok(AcquireOutcome::NoRecorder),
            Value::BulkString(bytes) => registration::origin(&bytes)?,
            _ => return Err(AdmissionError::InvalidRecorderUrl),
        };
        if let Some((api, limit)) = quota {
            let mut command = cmd("HGET");
            command.arg(&self.policy.counts_key).arg(api);
            let value: Value = self
                .redis
                .command(command)
                .await
                .map_err(AdmissionError::backend)?;
            let count = match value {
                Value::Nil => 0,
                Value::BulkString(bytes)
                    if !bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit) =>
                {
                    std::str::from_utf8(&bytes)
                        .ok()
                        .and_then(|text| text.parse::<u64>().ok())
                        .ok_or_else(|| policy::invalid("API count must be a nonnegative integer"))?
                }
                _ => return Err(policy::invalid("API count must be a nonnegative integer")),
            };
            if count >= limit {
                lookup_guard.available();
                return Ok(AcquireOutcome::QuotaExhausted);
            }
        }
        let token = format!(
            "{:032x}{:032x}",
            rand::random::<u128>(),
            rand::random::<u128>()
        );
        let ticket = AdmissionTicket::new(scope, &token)?.with_origin(origin)?;
        if !reserve(
            self.redis.as_ref(),
            &self.key(scope),
            ticket.token(),
            self.ttl_ms,
        )
        .await?
        {
            lookup_guard.available();
            return Ok(AcquireOutcome::Busy);
        }
        if let Some((api, _)) = quota {
            let mut command = cmd("HINCRBY");
            command.arg(&self.policy.counts_key).arg(api).arg(1);
            let value: Value = self
                .redis
                .command(command)
                .await
                .map_err(AdmissionError::backend)?;
            if !matches!(value, Value::Int(count) if count > 0) {
                return Err(policy::invalid("unexpected API count increment response"));
            }
        }
        // Check again after all Redis operations, including count updates.
        // An unused reservation remains until completion or TTL recovery.
        if !self.is_participating() {
            return Err(AdmissionError::ParticipationLost);
        }
        lookup_guard.available();
        Ok(AcquireOutcome::Acquired(ticket))
    }
}

impl<R: Redis + 'static> AdmissionProvider for RedisAdmissionProvider<R> {
    fn try_acquire<'a>(&'a self, scope: &'a str) -> AdmissionFuture<'a> {
        // Recorder-side and legacy route admission retain the lock-only path.
        Box::pin(self.acquire(scope, None))
    }

    fn try_acquire_request<'a>(&'a self, scope: &'a str, path: &'a str) -> AdmissionFuture<'a> {
        Box::pin(async move {
            if !self.is_participating() {
                return Ok(AcquireOutcome::NotParticipant);
            }
            let snapshot = self.policy.snapshot();
            let Some(policy) = &snapshot.policy else {
                return Ok(AcquireOutcome::NotEligible);
            };
            let Some(api) = policy.api(path) else {
                return Ok(AcquireOutcome::NotEligible);
            };
            self.acquire(scope, policy.case_limit.map(|limit| (api, limit)))
                .await
        })
    }
}

async fn reserve<R: Redis>(
    redis: &R,
    key: &str,
    token: &str,
    ttl_ms: u64,
) -> Result<bool, AdmissionError> {
    let mut command = cmd("SET");
    command.arg(key).arg(token).arg("NX").arg("PX").arg(ttl_ms);
    let result: Value = redis
        .command(command)
        .await
        .map_err(AdmissionError::backend)?;
    // Only Redis' explicit NX rejection is Busy. Unknown replies are errors.
    match result {
        Value::Okay => Ok(true),
        Value::SimpleString(value) if value == "OK" => Ok(true),
        Value::Nil => Ok(false),
        _ => Err(AdmissionError::backend(std::io::Error::other(
            "unexpected Redis admission acquisition response",
        ))),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RedisAdmissionConfigError {
    #[error("recorder URL key conflicts with API policy or counts key")]
    RecorderKeyConflict,
    #[error("Redis admission key prefix must not be empty")]
    EmptyPrefix,
    #[error("Redis recorder URL key must not be empty")]
    EmptyRecorderKey,
    #[error("Redis admission reservation TTL must be positive whole milliseconds up to i64::MAX")]
    InvalidTtl,
}
