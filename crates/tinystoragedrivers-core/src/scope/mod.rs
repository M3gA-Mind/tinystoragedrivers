//! The tenant boundary every stored record carries.
//!
//! A [`Scope`] partitions one backend between independent owners: on a
//! multi-tenant MongoDB deployment it is the user (agent) a record belongs to,
//! and on a single-operator desktop it is [`Scope::local`]. Port handles are
//! opened *for* a scope (see
//! [`StorageBackend::for_scope`](crate::StorageBackend::for_scope)), so no port
//! method takes one and no call site can forget it.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Result, StorageError};

mod segment;
mod types;

pub use types::{ParsedScope, Principal, Tenant};

/// The longest scope a driver must be able to store, in bytes.
pub const MAX_SCOPE_LEN: usize = 256;

/// The prefix of a user tenant: `u:<segment>`.
pub const USER_PREFIX: &str = "u:";

/// Separates a tenant from an agent principal: `<tenant>/agent:<segment>`.
pub const AGENT_INFIX: &str = "/agent:";

/// Separates a tenant from a module principal: `<tenant>/module:<segment>`.
pub const MODULE_INFIX: &str = "/module:";

/// The one scope with no tenant.
pub const CLUSTER: &str = "cluster";

/// The longest segment kept as written, in bytes; longer ones are hashed.
pub const MAX_SEGMENT_LEN: usize = 96;

/// A validated tenant key.
///
/// A scope is 1 to [`MAX_SCOPE_LEN`] bytes of printable text without
/// whitespace, so it can be stored as a plain column or field and appear in a
/// log line without escaping.
///
/// ```
/// use tinystoragedrivers_core::Scope;
///
/// let scope = Scope::new("agent:alice")?;
/// assert_eq!(scope.as_str(), "agent:alice");
/// assert!(Scope::new("").is_err());
/// assert!(Scope::local().is_local());
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
///
/// New writes should build scopes with [`Scope::local`], [`Scope::tenant`],
/// [`Scope::cluster`], [`Scope::agent`] and [`Scope::module`], which follow the
/// v2 grammar and hash unsafe ids. [`Scope::new`] stays permissive so that v1
/// data remains readable during migration.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scope(Arc<str>);

impl Scope {
    /// The value of [`Scope::local`].
    pub const LOCAL: &'static str = "local";

    /// Validate `value` as a scope.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// value is empty, longer than [`MAX_SCOPE_LEN`] bytes, or contains
    /// whitespace or control characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(StorageError::invalid_input("scope must not be empty"));
        }
        if value.len() > MAX_SCOPE_LEN {
            return Err(StorageError::invalid_input(format!(
                "scope is {} bytes, longer than the {MAX_SCOPE_LEN} byte limit",
                value.len()
            )));
        }
        if value.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(StorageError::invalid_input(
                "scope must not contain whitespace or control characters",
            ));
        }
        Ok(Self(Arc::from(value)))
    }

    /// The scope a single-operator installation stores everything under.
    #[must_use]
    pub fn local() -> Self {
        Self(Arc::from(Self::LOCAL))
    }

    /// Whether this is [`Scope::local`].
    #[must_use]
    pub fn is_local(&self) -> bool {
        &*self.0 == Self::LOCAL
    }

    /// The tenant scope `u:<segment>` for the raw user or profile `id`.
    ///
    /// An `id` that is longer than [`MAX_SEGMENT_LEN`] bytes or holds
    /// characters outside `A-Za-z0-9-_.@` is replaced by its hashed form.
    ///
    /// ```
    /// use tinystoragedrivers_core::Scope;
    ///
    /// assert_eq!(Scope::tenant("alice")?.as_str(), "u:alice");
    /// assert!(Scope::tenant("not legal")?.as_str().starts_with("u:~"));
    /// # Ok::<(), tinystoragedrivers_core::StorageError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when `id`
    /// is empty.
    pub fn tenant(id: impl AsRef<str>) -> Result<Self> {
        let segment = segment::encode(id.as_ref())?;
        Self::new(format!("{USER_PREFIX}{segment}"))
    }

    /// The tenant-less [`CLUSTER`] scope.
    #[must_use]
    pub fn cluster() -> Self {
        Self(Arc::from(CLUSTER))
    }

    /// Whether this is [`Scope::cluster`].
    #[must_use]
    pub fn is_cluster(&self) -> bool {
        &*self.0 == CLUSTER
    }

    /// This tenant scope narrowed to the agent `id` (`<tenant>/agent:<segment>`).
    ///
    /// The id is hashed like [`Scope::tenant`] ids are.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when `id`
    /// is empty or this is not a tenant scope without a principal.
    pub fn agent(&self, id: impl AsRef<str>) -> Result<Self> {
        self.with_principal(AGENT_INFIX, id.as_ref())
    }

    /// This tenant scope narrowed to the module `id` (`<tenant>/module:<segment>`).
    ///
    /// The id is hashed like [`Scope::tenant`] ids are.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when `id`
    /// is empty or this is not a tenant scope without a principal.
    pub fn module(&self, id: impl AsRef<str>) -> Result<Self> {
        self.with_principal(MODULE_INFIX, id.as_ref())
    }

    fn with_principal(&self, infix: &str, id: &str) -> Result<Self> {
        let parsed = self.parsed()?;
        if parsed.tenant == Tenant::Cluster || parsed.principal.is_some() {
            return Err(StorageError::invalid_input(format!(
                "a principal can only be added to a tenant scope without one, not {self}"
            )));
        }
        let segment = segment::encode(id)?;
        Self::new(format!("{self}{infix}{segment}"))
    }

    /// The tenant scope this scope belongs to: itself without any principal.
    ///
    /// `None` for [`Scope::cluster`], which has no tenant, and for a v1 string
    /// outside the grammar.
    #[must_use]
    pub fn tenant_scope(&self) -> Option<Self> {
        let mut parsed = self.parsed().ok()?;
        if parsed.tenant == Tenant::Cluster {
            return None;
        }
        parsed.principal = None;
        parsed.to_scope().ok()
    }

    /// Split `text` into the parts of the v2 grammar.
    ///
    /// ```
    /// use tinystoragedrivers_core::{Scope, Tenant};
    ///
    /// assert_eq!(Scope::parse("local")?.tenant, Tenant::Local);
    /// assert!(Scope::parse("profile:x").is_err());
    /// # Ok::<(), tinystoragedrivers_core::StorageError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when `text`
    /// does not follow the grammar, which includes every v1 scope that
    /// [`Scope::new`] still accepts.
    pub fn parse(text: &str) -> Result<ParsedScope> {
        let bad = || StorageError::invalid_input(format!("{text:?} is not a v2 scope"));
        if text == CLUSTER {
            return Ok(ParsedScope {
                tenant: Tenant::Cluster,
                principal: None,
            });
        }
        let (tenant_text, principal_text) = match text.split_once('/') {
            Some((tenant, principal)) => (tenant, Some(principal)),
            None => (text, None),
        };
        let tenant = if tenant_text == Self::LOCAL {
            Tenant::Local
        } else {
            let id = tenant_text.strip_prefix(USER_PREFIX).ok_or_else(bad)?;
            if !segment::is_valid(id) {
                return Err(bad());
            }
            Tenant::User(id.to_owned())
        };
        let principal = match principal_text {
            None => None,
            Some(text) => {
                let (make, id): (fn(String) -> Principal, &str) =
                    if let Some(id) = text.strip_prefix(&AGENT_INFIX[1..]) {
                        (Principal::Agent, id)
                    } else if let Some(id) = text.strip_prefix(&MODULE_INFIX[1..]) {
                        (Principal::Module, id)
                    } else {
                        return Err(bad());
                    };
                if !segment::is_valid(id) {
                    return Err(bad());
                }
                Some(make(id.to_owned()))
            }
        };
        Ok(ParsedScope { tenant, principal })
    }

    /// This scope split into the parts of the v2 grammar; see [`Scope::parse`].
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when this
    /// is a v1 scope outside the grammar.
    pub fn parsed(&self) -> Result<ParsedScope> {
        Self::parse(&self.0)
    }

    /// The scope as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scope({:?})", &*self.0)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Scope {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Serialize for Scope {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
