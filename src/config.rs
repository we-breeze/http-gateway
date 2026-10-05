use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::{HeaderValue, Method};
use serde::Deserialize;

/// On-disk route selection configuration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutesConfig {
    /// Requests matching any rule are dispatched to the selected service.
    #[serde(default)]
    pub routes: Vec<RouteRule>,
    /// Requests matching any exclusion always use the fallback origin.
    /// Exclusions take precedence over every selected route.
    #[serde(default)]
    pub exclude: Vec<ExclusionRule>,
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
    /// Optional non-waiting admission before entering the selected service.
    #[serde(default)]
    pub admission: Option<AdmissionRule>,
}

/// A method and path filter that bypasses selected services and admission.
/// Paths use the same exact and template matching as [`RouteRule`].
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExclusionRule {
    /// Empty means every HTTP method.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Absolute request path. Query parameters are not used for matching.
    pub path: String,
}

/// A named provider and a scope shared across gateway instances.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRule {
    pub provider: String,
    pub scope: String,
    /// Maximum time to acquire admission, before any request is dispatched.
    /// Acquisition timeout or failure uses the fallback origin.
    #[serde(default = "default_admission_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,
}

fn default_admission_acquire_timeout_ms() -> u64 {
    50
}

/// Validated route table shared by gateway connections.
#[derive(Clone, Debug, Default)]
pub struct RouteTable {
    index: brz_http_router::RouteIndex,
    exclusions: brz_http_router::RouteIndex,
    admissions: Arc<[Option<AdmissionRule>]>,
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
        let exclusions = config
            .exclude
            .into_iter()
            .enumerate()
            .map(|(index, rule)| {
                let methods = rule
                    .methods
                    .into_iter()
                    .map(|method| {
                        Method::from_bytes(method.as_bytes())
                            .map_err(|_| ConfigError::InvalidExclusionMethod { index, method })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(brz_http_router::IndexedRouteRule::new(
                    rule.path, methods, 0,
                ))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        let exclusions = brz_http_router::RouteIndex::compile(exclusions)
            .map_err(ConfigError::ExclusionRoute)?;
        let mut admissions = Vec::with_capacity(config.routes.len());
        let routes = config
            .routes
            .into_iter()
            .enumerate()
            .map(|(index, rule)| {
                if let Some(admission) = &rule.admission {
                    if admission.provider.trim().is_empty()
                        || admission.scope.trim().is_empty()
                        || !admission.scope.is_ascii()
                        || HeaderValue::from_str(&admission.scope).is_err()
                        || admission.acquire_timeout_ms == 0
                    {
                        return Err(ConfigError::InvalidAdmission { index });
                    }
                }
                admissions.push(rule.admission);
                let methods = rule
                    .methods
                    .into_iter()
                    .map(|method| {
                        Method::from_bytes(method.as_bytes())
                            .map_err(|_| ConfigError::InvalidMethod { index, method })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(brz_http_router::IndexedRouteRule::new(
                    rule.path, methods, 0,
                ))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        brz_http_router::RouteIndex::compile(routes)
            .map(|index| Self {
                index,
                exclusions,
                admissions: admissions.into(),
            })
            .map_err(ConfigError::Route)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[must_use]
    pub fn matches(&self, method: &Method, path: &str) -> bool {
        self.select(method, path).is_some()
    }

    /// Configured admission policies, including policies on overlapping rules.
    pub fn admission_rules(&self) -> impl Iterator<Item = &AdmissionRule> {
        self.admissions.iter().flatten()
    }

    /// Returns no rule for excluded requests, otherwise the first match.
    /// Overlapping routes must
    /// declare the desired admission policy on the first matching entry.
    pub(crate) fn select(&self, method: &Method, path: &str) -> Option<usize> {
        if self.exclusions.matches(method, path) {
            return None;
        }
        self.index
            .resolve(method.as_str(), path, brz_http_router::PathMode::Raw)
            .selected()
            .map(|route| route.id())
    }

    pub(crate) fn admissions(&self) -> &[Option<AdmissionRule>] {
        &self.admissions
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
    #[error("exclusion {index} has invalid HTTP method: {method}")]
    InvalidExclusionMethod { index: usize, method: String },
    #[error("invalid exclusion route: {0}")]
    ExclusionRoute(#[source] brz_http_router::RouteError),
    #[error(
        "route {index} admission requires a provider, a header-safe nonempty scope and a positive acquire_timeout_ms"
    )]
    InvalidAdmission { index: usize },
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
            admission: None,
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
            ..RoutesConfig::default()
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
            ..RoutesConfig::default()
        })
        .unwrap();
        assert!(table.matches(&Method::GET, "/api/tasks/123"));
        assert!(table.matches(&Method::GET, "/api/quota/claude/quota"));
        assert!(!table.matches(&Method::GET, "/api/quota"));
    }

    #[test]
    fn admission_uses_first_matching_route_and_validates_configuration() {
        let config: RoutesConfig = toml::from_str(
            r#"
            [[routes]]
            path = "/api/:id"
            admission = { provider = "recording", scope = "all-writes" }
            [[routes]]
            path = "/api/new"
        "#,
        )
        .unwrap();
        let table = RouteTable::compile(config).unwrap();
        assert_eq!(table.select(&Method::POST, "/api/new"), Some(0));
        assert_eq!(
            table.admissions()[0].as_ref().unwrap().acquire_timeout_ms,
            50
        );
        for (provider, scope, acquire_timeout_ms) in [
            ("", "scope", 10),
            ("recording", "", 10),
            ("recording", "scope", 0),
            ("recording", "非ASCII", 10),
        ] {
            let mut route = rule(&["POST"], "/api/write");
            route.admission = Some(AdmissionRule {
                provider: provider.into(),
                scope: scope.into(),
                acquire_timeout_ms,
            });
            assert!(matches!(
                RouteTable::compile(RoutesConfig {
                    routes: vec![route],
                    ..RoutesConfig::default()
                }),
                Err(ConfigError::InvalidAdmission { index: 0 })
            ));
        }
    }

    #[test]
    fn exclusions_override_exact_and_wildcard_routes_for_only_the_given_method() {
        let config: RoutesConfig = toml::from_str(
            r#"
            [[routes]]
            path = "/api/*path"
            admission = { provider = "recording", scope = "all-apis" }
            [[routes]]
            path = "/api/events"
            [[exclude]]
            methods = ["GET"]
            path = "/api/events"
            "#,
        )
        .unwrap();
        let table = RouteTable::compile(config).unwrap();
        assert!(!table.matches(&Method::GET, "/api/events"));
        assert_eq!(table.select(&Method::GET, "/api/events"), None);
        assert_eq!(table.select(&Method::POST, "/api/events"), Some(0));
        assert!(table.matches(&Method::HEAD, "/api/events"));
        assert!(table.matches(&Method::GET, "/api/events/child"));
        assert!(table.matches(&Method::GET, "/api/other"));
    }

    #[test]
    fn exclusions_support_templates_and_all_methods() {
        let config: RoutesConfig = toml::from_str(
            r#"
            [[exclude]]
            path = "/api/private/:id"
            [[exclude]]
            methods = ["GET"]
            path = "/api/streams/*path"
            [[routes]]
            path = "/api/*path"
            "#,
        )
        .unwrap();
        let table = RouteTable::compile(config).unwrap();
        for method in [
            Method::GET,
            Method::POST,
            Method::from_bytes(b"CUSTOM").unwrap(),
        ] {
            assert!(!table.matches(&method, "/api/private/42"));
            assert!(table.matches(&method, "/api/private/42/child"));
        }
        assert!(!table.matches(&Method::GET, "/api/streams/a/b"));
        assert!(table.matches(&Method::POST, "/api/streams/a/b"));
    }

    #[test]
    fn invalid_exclusions_fail_configuration_instead_of_being_ignored() {
        let invalid_method: RoutesConfig =
            toml::from_str("[[exclude]]\nmethods = [\"BAD METHOD\"]\npath = \"/api/events\"")
                .unwrap();
        assert!(matches!(
            RouteTable::compile(invalid_method),
            Err(ConfigError::InvalidExclusionMethod { index: 0, .. })
        ));
        let invalid_path: RoutesConfig =
            toml::from_str("[[exclude]]\npath = \"/api/*path/child\"").unwrap();
        assert!(matches!(
            RouteTable::compile(invalid_path),
            Err(ConfigError::ExclusionRoute(_))
        ));
        assert!(
            toml::from_str::<RoutesConfig>("[[exclude]]\npath = \"/api/events\"\nadmission = {}")
                .is_err()
        );
    }
}
