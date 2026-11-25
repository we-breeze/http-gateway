//! Compiled path selection shared by provider clones; no request-time parsing.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use brz_http_router::{IndexedRouteRule, PathMode, RouteIndex};
use brz_redis::{Redis, Value, cmd};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::AdmissionError;

const LOAD_TIMEOUT: Duration = Duration::from_millis(200);
const TRY_INCREMENT: &str = r#"
local count = redis.call('HGET', KEYS[1], ARGV[1]) or '0'
if not string.match(count, '^%d+$') then
    return redis.error_reply('API count must be a nonnegative integer')
end
local limit = ARGV[2]
-- Compare decimal strings to keep the full Redis integer range.
if #count > #limit or (#count == #limit and count >= limit) then return 0 end
redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
return 1
"#;

pub(super) struct PolicyStore {
    pub(super) key: String,
    counts_key: String,
    snapshot: ArcSwap<Snapshot>,
    loading: Mutex<()>,
}

pub(super) struct Snapshot {
    raw: Option<bytes::Bytes>,
    pub(super) policy: Option<CompiledPolicy>,
}

impl PolicyStore {
    pub(super) fn new(prefix: &str) -> Self {
        Self {
            key: format!("{prefix}:api-policy"),
            counts_key: format!("{prefix}:api-counts"),
            snapshot: ArcSwap::from_pointee(Snapshot {
                raw: None,
                policy: None,
            }),
            loading: Mutex::new(()),
        }
    }

    pub(super) fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.load_full()
    }

    pub(super) async fn case_count<R: Redis>(
        &self,
        redis: &R,
        api: &str,
    ) -> Result<u64, AdmissionError> {
        let mut command = cmd("HGET");
        command.arg(&self.counts_key).arg(api);
        let value: Value = redis
            .command(command)
            .await
            .map_err(AdmissionError::backend)?;
        match value {
            Value::Nil => Ok(0),
            Value::BulkString(bytes)
                if !bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit) =>
            {
                std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| text.parse::<u64>().ok())
                    .ok_or_else(|| invalid("API count must be a nonnegative integer"))
            }
            _ => Err(invalid("API count must be a nonnegative integer")),
        }
    }

    pub(super) async fn increment<R: Redis>(
        &self,
        redis: &R,
        api: &str,
    ) -> Result<(), AdmissionError> {
        let mut command = cmd("HINCRBY");
        command.arg(&self.counts_key).arg(api).arg(1);
        let value: Value = redis
            .command(command)
            .await
            .map_err(AdmissionError::backend)?;
        if !matches!(value, Value::Int(count) if count > 0) {
            return Err(invalid("unexpected API count increment response"));
        }
        Ok(())
    }

    pub(super) async fn try_increment<R: Redis>(
        &self,
        redis: &R,
        api: &str,
        limit: u64,
    ) -> Result<bool, AdmissionError> {
        let limit = limit.to_string();
        let result: Value = redis
            .eval(
                TRY_INCREMENT,
                &[self.counts_key.as_str()],
                &[api, limit.as_str()],
            )
            .await
            .map_err(AdmissionError::backend)?;
        match result {
            Value::Int(0) => Ok(false),
            Value::Int(1) => Ok(true),
            _ => Err(invalid("unexpected API count increment response")),
        }
    }

    pub(super) async fn load<R: Redis>(&self, redis: &R) -> Result<(), AdmissionError> {
        tokio::time::timeout(LOAD_TIMEOUT, async {
            // Serialize manual and periodic loads so an older reply cannot win.
            let _loading = self.loading.lock().await;
            let mut command = cmd("GET");
            command.arg(&self.key);
            let value: Value = redis
                .command(command)
                .await
                .map_err(AdmissionError::backend)?;
            let raw = match value {
                Value::Nil => None,
                Value::BulkString(bytes) => Some(bytes),
                _ => return Err(invalid("API policy must be a JSON string")),
            };
            let current = self.snapshot.load();
            if current.policy.is_some() && current.raw == raw {
                return Ok(());
            }
            let config: PolicyConfig = match &raw {
                Some(bytes) => serde_json::from_slice(bytes).map_err(AdmissionError::backend)?,
                None => PolicyConfig::default(),
            };
            let policy = CompiledPolicy::compile(config)?;
            self.snapshot.store(Arc::new(Snapshot {
                raw,
                policy: Some(policy),
            }));
            Ok(())
        })
        .await
        .map_err(|_| AdmissionError::Timeout)?
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct PolicyConfig {
    #[serde(default)]
    case_limit: Option<u64>,
    #[serde(default)]
    includes: Vec<PathRule>,
    #[serde(default)]
    excludes: Vec<PathRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PathRule {
    path: String,
    #[serde(default, rename = "match")]
    kind: MatchKind,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum MatchKind {
    #[default]
    Prefix,
    Exact,
}

pub(super) struct CompiledPolicy {
    pub(super) case_limit: Option<u64>,
    includes_all: bool,
    exact: RouteIndex,
    exact_paths: Vec<String>,
    prefixes: RouteIndex,
    excludes: RouteIndex,
}

impl CompiledPolicy {
    fn compile(config: PolicyConfig) -> Result<Self, AdmissionError> {
        if config
            .case_limit
            .is_some_and(|limit| limit > i64::MAX as u64)
        {
            return Err(invalid("case-limit must fit a nonnegative Redis integer"));
        }
        let includes_all = config.includes.is_empty();
        let mut exact = Vec::new();
        let mut exact_paths = Vec::new();
        let mut prefixes = Vec::new();
        for rule in config.includes {
            validate_path(&rule.path)?;
            match rule.kind {
                MatchKind::Exact => {
                    exact_paths.push(rule.path.clone());
                    exact.push(indexed(rule.path));
                }
                MatchKind::Prefix => add_prefix(&mut prefixes, &rule.path)?,
            }
        }
        let mut excludes = Vec::new();
        for rule in config.excludes {
            validate_path(&rule.path)?;
            match rule.kind {
                MatchKind::Exact => excludes.push(indexed(rule.path)),
                MatchKind::Prefix => add_prefix(&mut excludes, &rule.path)?,
            }
        }
        Ok(Self {
            case_limit: config.case_limit,
            includes_all,
            exact: compile(exact)?,
            exact_paths,
            prefixes: compile(prefixes)?,
            excludes: compile(excludes)?,
        })
    }

    /// Borrows either a canonical pattern or the caller's path, without allocation.
    pub(super) fn api<'a>(&'a self, path: &'a str) -> Option<&'a str> {
        if self.case_limit == Some(0) || self.excludes.matches_str("GET", path) {
            return None;
        }
        if let Some(route) = self.exact.resolve("GET", path, PathMode::Raw).selected() {
            return Some(&self.exact_paths[route.id()]);
        }
        (self.includes_all || self.prefixes.matches_str("GET", path)).then_some(path)
    }
}

fn indexed(path: String) -> IndexedRouteRule {
    IndexedRouteRule::new(path, Vec::new(), 0)
}

fn validate_path(path: &str) -> Result<(), AdmissionError> {
    let uri = path.parse::<http::Uri>().map_err(AdmissionError::backend)?;
    if !path.starts_with('/') || uri.path() != path || uri.query().is_some() || path.contains('#') {
        return Err(invalid(
            "policy rules require an absolute path without query or fragment",
        ));
    }
    Ok(())
}

fn compile(rules: Vec<IndexedRouteRule>) -> Result<RouteIndex, AdmissionError> {
    RouteIndex::compile(rules).map_err(AdmissionError::backend)
}

fn add_prefix(rules: &mut Vec<IndexedRouteRule>, path: &str) -> Result<(), AdmissionError> {
    if !path.starts_with('/')
        || path
            .split('/')
            .any(|segment| segment.starts_with([':', '*']))
    {
        return Err(invalid("prefix paths must be absolute literal paths"));
    }
    let base = path.trim_end_matches('/');
    rules.push(indexed(if base.is_empty() { "/" } else { base }.to_owned()));
    rules.push(indexed(format!("{base}/*__breeze_policy_path")));
    Ok(())
}

pub(super) fn invalid(message: &str) -> AdmissionError {
    AdmissionError::backend(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

#[cfg(test)]
mod tests;
