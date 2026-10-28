use super::*;

fn policy(json: &str) -> CompiledPolicy {
    CompiledPolicy::compile(serde_json::from_str(json).unwrap()).unwrap()
}

#[test]
fn omitted_and_empty_includes_allow_all_paths() {
    for json in ["{}", r#"{"includes":[],"excludes":[]}"#] {
        let policy = policy(json);
        assert_eq!(policy.api("/"), Some("/"));
        assert_eq!(policy.api("/unlisted/path"), Some("/unlisted/path"));
        assert_eq!(policy.case_limit, None);
    }
    assert_eq!(policy(r#"{"case-limit":0}"#).api("/api"), None);
}

#[test]
fn prefix_matches_whole_segments_and_exact_patterns_define_count_identity() {
    let policy = policy(
        r#"{
        "case-limit":200,
        "includes":[{"path":"/api/"},{"path":"/api/:id/xxx","match":"exact"}]
    }"#,
    );
    assert_eq!(policy.api("/api"), Some("/api"));
    assert_eq!(policy.api("/api/"), Some("/api/"));
    assert_eq!(policy.api("/api/other"), Some("/api/other"));
    assert_eq!(policy.api("/apix"), None);
    assert_eq!(policy.api("/api/123/xxx"), Some("/api/:id/xxx"));
    assert_eq!(policy.api("/api/456/xxx"), Some("/api/:id/xxx"));
    assert_eq!(policy.api("/api/123/xxx/tail"), Some("/api/123/xxx/tail"));
    let exact = policy_for_exact();
    assert_eq!(exact.api("/api/123/xxx/tail"), None);
}

fn policy_for_exact() -> CompiledPolicy {
    policy(r#"{"includes":[{"path":"/api/:id/xxx","match":"exact"}]}"#)
}

#[test]
fn excludes_override_all_include_shapes() {
    let policy = policy(
        r#"{
        "includes":[{"path":"/"},{"path":"/private/:id","match":"exact"}],
        "excludes":[{"path":"/socket.io"},{"path":"/private/:id","match":"exact"}]
    }"#,
    );
    for path in [
        "/socket.io",
        "/socket.io/",
        "/socket.io/session",
        "/private/12",
    ] {
        assert_eq!(policy.api(path), None, "{path}");
    }
    for path in ["/", "/socket.io-other", "/private/12/tail", "/other"] {
        assert_eq!(policy.api(path), Some(path));
    }
}

#[test]
fn invalid_policies_are_rejected() {
    for json in [
        r#"{"case-limit":-1}"#,
        r#"{"case-limit":9223372036854775808}"#,
        r#"{"case-limit":"200"}"#,
        r#"{"includes":[{"path":"relative"}]}"#,
        r#"{"includes":[{"path":"/api?query=1"}]}"#,
        r#"{"includes":[{"path":"/api#fragment","match":"exact"}]}"#,
        r#"{"includes":[{"path":"/api/:id"}]}"#,
        r#"{"includes":[{"path":"/api","match":"all"}]}"#,
        r#"{"includes":[{"path":"/api","method":"POST"}]}"#,
        r#"{"unknown":1}"#,
    ] {
        let result = serde_json::from_str::<PolicyConfig>(json)
            .map_err(AdmissionError::backend)
            .and_then(CompiledPolicy::compile);
        assert!(result.is_err(), "{json}");
    }
}

/// Run explicitly with `cargo test --release --all-features policy_match_cost
/// -- --ignored --nocapture`. This measures CPU only, not Redis latency.
#[test]
#[ignore]
fn policy_match_cost() {
    use std::{hint::black_box, time::Instant};
    let rules: Vec<_> = (0..100)
        .map(|index| serde_json::json!({"path":format!("/api/{index}/:id"),"match":"exact"}))
        .collect();
    let policy = policy(
        &serde_json::json!({"case-limit":200,"includes":rules,
        "excludes":[{"path":"/internal"}]})
        .to_string(),
    );
    let paths = ["/api/0/123", "/api/99/456", "/unlisted", "/internal/test"];
    let baseline = compile(
        (0..100)
            .map(|index| indexed(format!("/api/{index}/:id")))
            .collect(),
    )
    .unwrap();
    let iterations = 1_000_000;
    let started = Instant::now();
    for index in 0..iterations {
        black_box(baseline.resolve("GET", black_box(paths[index % paths.len()]), PathMode::Raw));
    }
    let baseline_ns = started.elapsed().as_nanos() as f64 / iterations as f64;
    let store = PolicyStore::new("benchmark");
    store.snapshot.store(Arc::new(Snapshot {
        raw: None,
        policy: Some(policy),
    }));
    let started = Instant::now();
    for index in 0..iterations {
        let snapshot = store.snapshot();
        black_box(
            snapshot
                .policy
                .as_ref()
                .unwrap()
                .api(black_box(paths[index % paths.len()])),
        );
    }
    let policy_ns = started.elapsed().as_nanos() as f64 / iterations as f64;
    eprintln!(
        "100 rules, mixed hits/misses: router {baseline_ns:.1} ns; policy + shared snapshot {policy_ns:.1} ns; delta {:.1} ns",
        policy_ns - baseline_ns
    );
}
