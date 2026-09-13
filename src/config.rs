use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRule {
    /// Empty means every HTTP method.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Absolute request path. Query parameters are not used for matching.
    pub path: String,
    /// Exact by default; prefix matches on path-segment boundaries.
    #[serde(default)]
    pub match_kind: PathMatch,
}

/// Path matching behavior for a route rule.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PathMatch {
    #[default]
    Exact,
    Prefix,
}

/// Validated route table shared by gateway connections.
#[derive(Clone, Debug, Default)]
pub struct RouteTable {
    routes: Arc<[CompiledRoute]>,
}

impl RouteTable {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates and compiles route configuration.
    ///
    /// # Errors
    /// Returns an error for an invalid method or non-absolute path.
    pub fn compile(config: RoutesConfig) -> Result<Self, ConfigError> {
        let routes = config
            .routes
            .into_iter()
            .enumerate()
            .map(|(index, rule)| CompiledRoute::compile(index, rule))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            routes: routes.into(),
        })
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    #[must_use]
    pub fn matches(&self, method: &Method, path: &str) -> bool {
        self.routes.iter().any(|route| route.matches(method, path))
    }
}

#[derive(Clone, Debug)]
struct CompiledRoute {
    methods: HashSet<Method>,
    path: Box<str>,
    match_kind: PathMatch,
}

impl CompiledRoute {
    fn compile(index: usize, rule: RouteRule) -> Result<Self, ConfigError> {
        if !rule.path.starts_with('/') {
            return Err(ConfigError::InvalidPath {
                index,
                path: rule.path,
            });
        }
        let methods = rule
            .methods
            .into_iter()
            .map(|method| {
                Method::from_bytes(method.as_bytes())
                    .map_err(|_| ConfigError::InvalidMethod { index, method })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            methods,
            path: rule.path.into_boxed_str(),
            match_kind: rule.match_kind,
        })
    }

    fn matches(&self, method: &Method, path: &str) -> bool {
        (self.methods.is_empty() || self.methods.contains(method))
            && match self.match_kind {
                PathMatch::Exact => path == self.path.as_ref(),
                PathMatch::Prefix => prefix_matches(&self.path, path),
            }
    }
}

fn prefix_matches(prefix: &str, path: &str) -> bool {
    if prefix == "/" || path == prefix {
        return true;
    }
    let Some(remainder) = path.strip_prefix(prefix) else {
        return false;
    };
    prefix.ends_with('/') || remainder.starts_with('/')
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
    #[error("route {index} path must start with '/': {path}")]
    InvalidPath { index: usize, path: String },
    #[error("route {index} has invalid HTTP method: {method}")]
    InvalidMethod { index: usize, method: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(methods: &[&str], path: &str, match_kind: PathMatch) -> RouteRule {
        RouteRule {
            methods: methods.iter().map(ToString::to_string).collect(),
            path: path.to_owned(),
            match_kind,
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
            routes: vec![rule(&["GET"], "/api/health", PathMatch::Exact)],
        })
        .unwrap();
        assert!(table.matches(&Method::GET, "/api/health"));
        assert!(!table.matches(&Method::POST, "/api/health"));
        assert!(!table.matches(&Method::GET, "/api/health/ready"));
    }

    #[test]
    fn prefix_rules_stop_at_segment_boundaries() {
        let table = RouteTable::compile(RoutesConfig {
            routes: vec![rule(&[], "/api/tasks", PathMatch::Prefix)],
        })
        .unwrap();
        assert!(table.matches(&Method::PATCH, "/api/tasks"));
        assert!(table.matches(&Method::GET, "/api/tasks/42"));
        assert!(!table.matches(&Method::GET, "/api/taskstream"));
    }
}
