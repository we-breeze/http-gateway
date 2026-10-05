//! Recorder address leases; the caller owns readiness and heartbeat scheduling.

use crate::{AdmissionError, AdmissionTicket};
use brz_redis::{Redis, Value};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Address {
    url: String,
    owner: String,
}

/// One owner per discovery key, with token-checked renewal and removal.
/// Publishing never replaces a live recorder. Expiry permits replacement.
pub struct RecorderRegistration<R> {
    redis: R,
    key: String,
    value: String,
    ttl_ms: u64,
}

impl<R: Redis> RecorderRegistration<R> {
    pub fn new(
        redis: R,
        key: impl Into<String>,
        url: http::Uri,
        ttl: Duration,
    ) -> Result<Self, AdmissionError> {
        AdmissionTicket::new("registration", "validation")?.with_origin(url.clone())?;
        let key = key.into();
        let ms = ttl.as_millis();
        if key.is_empty() || ms == 0 || ms > i64::MAX as u128 || ttl.subsec_nanos() % 1_000_000 != 0
        {
            return Err(AdmissionError::backend(std::io::Error::other(
                "registration key and whole-millisecond TTL must be valid",
            )));
        }
        let address = Address {
            url: url.to_string(),
            owner: format!("{:032x}", rand::random::<u128>()),
        };
        let value = serde_json::to_string(&address).map_err(AdmissionError::backend)?;
        Ok(Self {
            redis,
            key,
            value,
            ttl_ms: ms as u64,
        })
    }

    pub async fn register(&self) -> Result<bool, AdmissionError> {
        super::reserve(&self.redis, &self.key, &self.value, self.ttl_ms).await
    }

    pub async fn renew(&self) -> Result<bool, AdmissionError> {
        self.transition("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end return 0").await
    }

    pub async fn unregister(&self) -> Result<bool, AdmissionError> {
        self.transition(super::COMPLETE).await
    }

    async fn transition(&self, script: &str) -> Result<bool, AdmissionError> {
        let ttl = self.ttl_ms.to_string();
        let result: Value = self
            .redis
            .eval(
                script,
                &[self.key.as_str()],
                &[self.value.as_str(), ttl.as_str()],
            )
            .await
            .map_err(AdmissionError::backend)?;
        match result {
            Value::Int(0) => Ok(false),
            Value::Int(1) => Ok(true),
            _ => Err(AdmissionError::backend(std::io::Error::other(
                "unexpected Redis registration response",
            ))),
        }
    }
}

pub(super) fn origin(bytes: &[u8]) -> Result<http::Uri, AdmissionError> {
    let text = std::str::from_utf8(bytes).map_err(|_| AdmissionError::InvalidRecorderUrl)?;
    let url = if text.trim_start().starts_with('{') {
        let address: Address =
            serde_json::from_str(text).map_err(|_| AdmissionError::InvalidRecorderUrl)?;
        if address.owner.trim().is_empty() {
            return Err(AdmissionError::InvalidRecorderUrl);
        }
        address.url
    } else {
        text.to_owned()
    };
    url.parse().map_err(|_| AdmissionError::InvalidRecorderUrl)
}
