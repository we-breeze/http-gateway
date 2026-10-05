use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::{HeaderMap, HeaderValue, Request, Uri};

use crate::BoxError;

/// Reserved transport headers. The gateway removes client-supplied values.
pub const ADMISSION_SCOPE_HEADER: &str = "x-breeze-admission-scope";
pub const ADMISSION_TOKEN_HEADER: &str = "x-breeze-admission-token";

/// Transferable reservation, explicitly completed by the receiving service.
/// Dropping a ticket does not release its reservation.
#[derive(Clone)]
pub struct AdmissionTicket {
    scope: HeaderValue,
    token: HeaderValue,
    origin: Option<Uri>,
}

impl AdmissionTicket {
    /// Creates a ticket whose values can safely travel in HTTP headers.
    pub fn new(scope: &str, token: &str) -> Result<Self, AdmissionError> {
        if scope.trim().is_empty()
            || token.trim().is_empty()
            || !scope.is_ascii()
            || !token.is_ascii()
        {
            return Err(AdmissionError::InvalidTicket);
        }
        let scope = HeaderValue::from_str(scope).map_err(|_| AdmissionError::InvalidTicket)?;
        let mut token = HeaderValue::from_str(token).map_err(|_| AdmissionError::InvalidTicket)?;
        token.set_sensitive(true);
        Ok(Self {
            scope,
            token,
            origin: None,
        })
    }

    #[must_use]
    pub fn scope(&self) -> &str {
        self.scope
            .to_str()
            .expect("ticket contains a string header")
    }

    #[must_use]
    pub fn token(&self) -> &str {
        self.token
            .to_str()
            .expect("ticket contains a string header")
    }

    /// Carries a discovered HTTP origin for `OriginService` to select.
    pub fn with_origin(mut self, origin: Uri) -> Result<Self, AdmissionError> {
        if origin.scheme_str() != Some("http")
            || origin.authority().is_none()
            || origin.path() != "/"
            || origin.query().is_some()
        {
            return Err(AdmissionError::InvalidRecorderUrl);
        }
        self.origin = Some(origin);
        Ok(self)
    }

    #[must_use]
    pub fn origin(&self) -> Option<&Uri> {
        self.origin.as_ref()
    }

    /// Reads a ticket from forwarded headers for a completion adapter.
    pub fn from_headers(headers: &HeaderMap) -> Result<Option<Self>, AdmissionError> {
        match (
            headers.get(ADMISSION_SCOPE_HEADER),
            headers.get(ADMISSION_TOKEN_HEADER),
        ) {
            (None, None) => Ok(None),
            (Some(scope), Some(token)) => {
                if headers.get_all(ADMISSION_SCOPE_HEADER).iter().count() != 1
                    || headers.get_all(ADMISSION_TOKEN_HEADER).iter().count() != 1
                {
                    return Err(AdmissionError::InvalidTicket);
                }
                Self::new(
                    scope.to_str().map_err(|_| AdmissionError::InvalidTicket)?,
                    token.to_str().map_err(|_| AdmissionError::InvalidTicket)?,
                )
                .map(Some)
            }
            _ => Err(AdmissionError::InvalidTicket),
        }
    }

    pub(crate) fn attach<B>(&self, request: &mut Request<B>) {
        request
            .headers_mut()
            .insert(ADMISSION_SCOPE_HEADER, self.scope.clone());
        request
            .headers_mut()
            .insert(ADMISSION_TOKEN_HEADER, self.token.clone());
        request.extensions_mut().insert(self.clone());
    }
}

impl std::fmt::Debug for AdmissionTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionTicket")
            .field("scope", &self.scope())
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// Explicit decisions that permit dispatch or forwarding to the fallback.
#[derive(Debug)]
pub enum AcquireOutcome {
    Acquired(AdmissionTicket),
    Busy,
    /// Discovery found no usable recorder, including a cached unavailable result.
    NoRecorder,
    /// This gateway has no current participation lease. No request was sent.
    NotParticipant,
}

pub type AdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AcquireOutcome, AdmissionError>> + Send + 'a>>;

/// Non-waiting admission shared across gateway instances. Implementations
/// must atomically reserve a scope or report that it is already occupied.
/// They must not dispatch the application request: acquisition errors and
/// timeouts allow the gateway to forward that request to its fallback.
pub trait AdmissionProvider: Send + Sync + 'static {
    fn try_acquire<'a>(&'a self, scope: &'a str) -> AdmissionFuture<'a>;
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("admission backend failed: {0}")]
    Backend(#[source] BoxError),
    #[error("admission request timed out; reservation outcome is unknown")]
    Timeout,
    #[error("admission ticket is invalid or belongs to another scope")]
    InvalidTicket,
    #[error("recorder URL must be an origin-only http:// URL")]
    InvalidRecorderUrl,
    #[error("gateway participation expired during request admission")]
    ParticipationLost,
}

impl AdmissionError {
    pub fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend(Box::new(error))
    }
}

/// Application-owned registration of named providers used by route config.
#[derive(Clone, Default)]
pub struct AdmissionRegistry {
    providers: BTreeMap<String, Arc<dyn AdmissionProvider>>,
}

impl std::fmt::Debug for AdmissionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl AdmissionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a provider. Duplicate names are errors rather than replacements.
    pub fn register<P: AdmissionProvider>(
        &mut self,
        name: impl Into<String>,
        provider: P,
    ) -> Result<(), RegistrationError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(RegistrationError::EmptyName);
        }
        match self.providers.entry(name) {
            std::collections::btree_map::Entry::Occupied(entry) => {
                Err(RegistrationError::Duplicate(entry.key().clone()))
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Arc::new(provider));
                Ok(())
            }
        }
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Arc<dyn AdmissionProvider>> {
        self.providers.get(name)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("admission provider name must not be empty")]
    EmptyName,
    #[error("admission provider is already registered: {0}")]
    Duplicate(String),
}
