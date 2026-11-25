use std::{
    io,
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};

use brz_redis::{Redis, Value, cmd};
use sha1::{Digest, Sha1};

use crate::AdmissionError;

const SCRIPT: &str = include_str!("reservations.lua");
static SCRIPT_SHA: LazyLock<String> = LazyLock::new(|| format!("{:x}", Sha1::digest(SCRIPT)));

pub(super) async fn load<R: Redis>(redis: &R) -> Result<(), AdmissionError> {
    let mut command = cmd("SCRIPT");
    command.arg("LOAD").arg(SCRIPT);
    let digest: String = redis
        .command(command)
        .await
        .map_err(AdmissionError::backend)?;
    if digest == SCRIPT_SHA.as_str() {
        Ok(())
    } else {
        Err(AdmissionError::backend(io::Error::other(
            "unexpected Redis reservation script digest",
        )))
    }
}

pub(super) async fn ensure_loaded<R: Redis>(redis: &R) -> Result<(), AdmissionError> {
    let mut command = cmd("SCRIPT");
    command.arg("EXISTS").arg(SCRIPT_SHA.as_str());
    let exists: Vec<bool> = redis
        .command(command)
        .await
        .map_err(AdmissionError::backend)?;
    match exists.as_slice() {
        [true] => Ok(()),
        [false] => load(redis).await,
        _ => Err(AdmissionError::backend(io::Error::other(
            "unexpected Redis reservation script cache response",
        ))),
    }
}

pub(super) async fn transition<R: Redis>(
    redis: &R,
    key: &str,
    operation: &str,
    token: &str,
    ttl_ms: u64,
    max_inflight: u32,
) -> Result<bool, AdmissionError> {
    // Pass the time as an argument so script-replicating Redis can replay
    // the same decisions without executing a nondeterministic TIME command.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(AdmissionError::backend)?
        .as_millis()
        .to_string();
    let ttl = ttl_ms.to_string();
    let limit = max_inflight.to_string();
    let result: Value = redis
        .evalsha(
            SCRIPT_SHA.as_str(),
            &[key],
            &[operation, token, ttl.as_str(), limit.as_str(), now.as_str()],
        )
        .await
        .map_err(AdmissionError::backend)?;
    super::transition_result(result)
}
