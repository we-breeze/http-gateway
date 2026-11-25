use brz_redis::{Redis, Value};

use crate::AdmissionError;

pub(super) async fn transition<R: Redis>(
    redis: &R,
    key: &str,
    operation: &str,
    token: &str,
    ttl_ms: u64,
    max_inflight: u32,
) -> Result<bool, AdmissionError> {
    let ttl = ttl_ms.to_string();
    let limit = max_inflight.to_string();
    let result: Value = redis
        .eval(
            include_str!("reservations.lua"),
            &[key],
            &[operation, token, ttl.as_str(), limit.as_str()],
        )
        .await
        .map_err(AdmissionError::backend)?;
    super::transition_result(result)
}
