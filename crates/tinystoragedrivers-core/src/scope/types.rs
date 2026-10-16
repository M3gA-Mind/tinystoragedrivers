//! The typed view of a scope that follows the v2 grammar.

use std::fmt;

use super::{AGENT_INFIX, CLUSTER, MODULE_INFIX, Scope, USER_PREFIX};
use crate::error::Result;

/// The isolation boundary a scope belongs to.
///
/// The `String` of [`Tenant::User`] is the stored segment, so it is already in
/// its hashed form when the original id was too long or held characters
/// outside the segment set.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Tenant {
    /// The single desktop operator (`local`).
    Local,
    /// A user or profile (`u:<segment>`).
    User(String),
    /// The shared, tenant-less scope (`cluster`).
    Cluster,
}

/// The agent or module a record is narrowed to inside its tenant.
///
/// The `String` is the stored segment, hashed when the original id required it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Principal {
    /// One agent (`/agent:<segment>`).
    Agent(String),
    /// One module (`/module:<segment>`).
    Module(String),
}

/// A scope string split into the parts of the v2 grammar.
///
/// Produced by [`Scope::parse`]. [`Tenant::Cluster`] never carries a
/// principal.
///
/// ```
/// use tinystoragedrivers_core::{Principal, Scope, Tenant};
///
/// let parsed = Scope::parse("u:alice/agent:bot")?;
/// assert_eq!(parsed.tenant, Tenant::User("alice".into()));
/// assert_eq!(parsed.principal, Some(Principal::Agent("bot".into())));
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ParsedScope {
    /// The tenant the scope belongs to.
    pub tenant: Tenant,
    /// The agent or module the scope narrows to, when there is one.
    pub principal: Option<Principal>,
}

impl ParsedScope {
    /// The [`Scope`] this view describes.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// view was built by hand with a principal on [`Tenant::Cluster`], or with
    /// a segment outside the grammar (empty, over the length limit, or with
    /// characters a stored segment cannot hold). Only what [`Scope::parse`]
    /// accepts converts.
    pub fn to_scope(&self) -> Result<Scope> {
        let text = self.to_string();
        Scope::parse(&text)?;
        Scope::new(text)
    }
}

impl fmt::Display for ParsedScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.tenant {
            Tenant::Local => f.write_str(Scope::LOCAL)?,
            Tenant::User(segment) => write!(f, "{USER_PREFIX}{segment}")?,
            Tenant::Cluster => f.write_str(CLUSTER)?,
        }
        match &self.principal {
            None => Ok(()),
            Some(Principal::Agent(segment)) => write!(f, "{AGENT_INFIX}{segment}"),
            Some(Principal::Module(segment)) => write!(f, "{MODULE_INFIX}{segment}"),
        }
    }
}
