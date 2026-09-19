use std::path::{Path, PathBuf};

use http::Method;
use serde::Deserialize;

/// On-disk route selection configuration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutesConfig {
    /// Requests matching any rule are dispatched to the selected service.
    #[serde(default)]
    pub routes: Vec<RouteRule>,
}

impl RoutesConfig {
    /// Loads TOML route configuration from `path`.
    ///
    /// # Errors
    /// Returns an error when the file cannot be read or decoded.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&contents).map_err(|source| ConfigError::Decode {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// One route dispatched to the selected service instead of the fallback origin.
/// A path containing `:name` or a terminal `*name` is a template; all other
/// paths are exact.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRule {
    /// Empty means every HTTP method.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Absolute request path. Query parameters are not used for matching.
    pub path: String,
    /// Whether the gateway access log should include this route.
    #[serde(default = "default_log")]
    pub log: bool,
}

const fn default_log() -> bool {
    true
}

/// Validated route table shared by gateway connections.
#[derive(Clone, Debug, Default)]
pub struct RouteTable {
    matcher: brz_http_router::RouteTable,
    silent: brz_http_router::RouteTable,
}

impl RouteTable {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates and compiles route configuration.
    ///
    /// # Errors
    /// Returns an error for an invalid method or malformed route template.
    pub fn compile(config: RoutesConfig) -> Result<Self, ConfigError> {
        let mut routes = Vec::new();
        let mut silent = Vec::new();
        config
            .routes
            .into_iter()
            .enumerate()
            .map(|(index, rule)| {
                let methods = rule
                    .methods
                    .into_iter()
                    .map(|method| {
                        Method::from_bytes(method.as_bytes())
                            .map_err(|_| ConfigError::InvalidMethod { index, method })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let route = brz_http_router::RouteRule::new(rule.path, methods.clone());
                if !rule.log {
                    silent.push(route.clone());
                }
                routes.push(route);
                Ok(())
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        let matcher = brz_http_router::RouteTable::compile(routes);
        let silent = brz_http_router::RouteTable::compile(silent);
        matcher
            .and_then(|matcher| silent.map(|silent| Self { matcher, silent }))
            .map_err(ConfigError::Route)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.matcher.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.matcher.len()
    }

    #[must_use]
    pub fn matches(&self, method: &Method, path: &str) -> bool {
        self.matcher.matches(method, path)
    }

    /// Returns whether a request should be written to gateway.log.
    #[must_use]
    pub fn logs(&self, method: &Method, path: &str) -> bool {
        !self.silent.matches(method, path)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read route config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode route config {path}: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("route {index} has invalid HTTP method: {method}")]
    InvalidMethod { index: usize, method: String },
    #[error(transparent)]
    Route(#[from] brz_http_router::RouteError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(methods: &[&str], path: &str) -> RouteRule {
        RouteRule {
            methods: methods.iter().map(ToString::to_string).collect(),
            path: path.to_owned(),
            log: true,
        }
    }

    #[test]
    fn empty_table_falls_back_every_request() {
        let table = RouteTable::empty();
        assert!(!table.matches(&Method::GET, "/api/health"));
    }

    #[test]
    fn exact_rules_match_method() {
        let table = RouteTable::compile(RoutesConfig {
            routes: vec![rule(&["GET"], "/api/health")],
        })
        .unwrap();
        assert!(table.matches(&Method::GET, "/api/health"));
        assert!(!table.matches(&Method::POST, "/api/health"));
        assert!(!table.matches(&Method::GET, "/api/health/ready"));
    }

    #[test]
    fn templates_select_parameter_and_catch_all_paths() {
        let table = RouteTable::compile(RoutesConfig {
            routes: vec![
                rule(&["GET"], "/api/tasks/:task_id"),
                rule(&["GET"], "/api/quota/*path"),
            ],
        })
        .unwrap();
        assert!(table.matches(&Method::GET, "/api/tasks/123"));
        assert!(table.matches(&Method::GET, "/api/quota/claude/quota"));
        assert!(!table.matches(&Method::GET, "/api/quota"));
    }

    #[test]
    fn route_log_flag_is_independent_from_matching() {
        let table = RouteTable::compile(RoutesConfig {
            routes: vec![RouteRule {
                methods: vec!["GET".to_owned()],
                path: "/".to_owned(),
                log: false,
            }],
        })
        .unwrap();
        assert!(table.matches(&Method::GET, "/"));
        assert!(!table.logs(&Method::GET, "/"));
        assert!(table.logs(&Method::GET, "/other"));
    }
}
