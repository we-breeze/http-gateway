//! Redis-backed, single-slot recording admission across gateway processes.
//!
//! A successful acquisition expires after 60 seconds by default. Recording
//! requires no start/claim handshake. Completion can release the same token
//! earlier, without deleting a newer acquisition after TTL expiry.

use std::sync::Arc;
use std::time::Duration;

use brz_redis::{Redis, RedisService, Value, cmd};

use crate::{AcquireOutcome, AdmissionError, AdmissionFuture, AdmissionProvider, AdmissionTicket};

mod discovery;
mod lease;
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
        let ms = ttl.as_millis();
        if ms == 0 || ms > i64::MAX as u128 || ttl.subsec_nanos() % 1_000_000 != 0 {
            return Err(RedisAdmissionConfigError::InvalidTtl);
        }
        Ok(Self {
            redis: Arc::new(redis),
            key_prefix,
            recorder_url_key,
            ttl_ms: ms as u64,
            participation: None,
            discovery: Arc::new(RecorderDiscovery::default()),
        })
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
        self.transition(COMPLETE, ticket).await
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
    ) -> Result<bool, AdmissionError> {
        let key = self.key(ticket.scope());
        let result: Value = self
            .redis
            .eval(script, &[key.as_str()], &[ticket.token()])
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

impl<R: Redis + 'static> AdmissionProvider for RedisAdmissionProvider<R> {
    fn try_acquire<'a>(&'a self, scope: &'a str) -> AdmissionFuture<'a> {
        Box::pin(async move {
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
            // The gateway falls back before dispatch on participation loss.
            // An unused reservation remains until completion or TTL recovery.
            if !self.is_participating() {
                return Err(AdmissionError::ParticipationLost);
            }
            lookup_guard.available();
            Ok(AcquireOutcome::Acquired(ticket))
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
    #[error("Redis admission key prefix must not be empty")]
    EmptyPrefix,
    #[error("Redis recorder URL key must not be empty")]
    EmptyRecorderKey,
    #[error("Redis admission reservation TTL must be positive whole milliseconds up to i64::MAX")]
    InvalidTtl,
}
