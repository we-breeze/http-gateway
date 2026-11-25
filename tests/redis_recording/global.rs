use super::*;

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
