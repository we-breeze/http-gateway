//! Start a recording gateway; publish its policy, Recorder URL and participation
//! limit in Redis separately. For hybrid applications, use this on the fallback
//! branch after selecting locally owned business routes.

use brz_http_gateway::redis_admission::{GatewayLeaseConfig, RedisAdmissionProvider};
use brz_http_gateway::{
    AdmissionRegistry, AdmissionRule, Gateway, OriginService, RouteTable, bind, serve,
};
use brz_redis::RedisService;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisService::single("127.0.0.1:6379").await?;
    let provider = RedisAdmissionProvider::new(redis, "recording:wegent", "recording:wegent:url")?
        .with_gateway_limit(GatewayLeaseConfig::new(""))?;
    let mut registry = AdmissionRegistry::new();
    registry.register("recorder", provider)?;
    let gateway = Gateway::with_admission(
        RouteTable::empty(),
        OriginService::from_admission(),
        &"http://127.0.0.1:8000".parse()?,
        registry,
    )?
    .with_recording_admission(AdmissionRule {
        provider: "recorder".into(),
        scope: "all-apis".into(),
        acquire_timeout_ms: 50,
    })?;
    let listener = bind("127.0.0.1:3000".parse()?).await?;
    serve(
        listener,
        gateway,
        std::future::pending(),
        Duration::from_secs(30),
    )
    .await?;
    Ok(())
}
