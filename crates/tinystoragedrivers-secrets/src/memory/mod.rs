//! [`MemorySecrets`]: secrets in process memory, for tests.

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::SystemTime;

use async_trait::async_trait;
use tinystoragedrivers_types::{Result, SecretPrecondition, SecretVersion, StorageError};
use zeroize::Zeroizing;

use crate::store::SecretStore;
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);
static PROCESS_STORE_PREFIX: OnceLock<u64> = OnceLock::new();

fn process_store_prefix() -> u64 {
    *PROCESS_STORE_PREFIX.get_or_init(|| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        std::process::id().hash(&mut hasher);
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .hash(&mut hasher);
        hasher.finish()
    })
}

struct SecretEntry {
    value: Zeroizing<Vec<u8>>,
    version: SecretVersion,
}
use tinystoragedrivers_types::{
    validate_secret_name as validate_name, validate_secret_prefix as validate_prefix,
};

/// Secrets in a process-local map. Nothing survives the process, and values
/// are wiped from memory when replaced, deleted, or dropped with the store.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};
///
/// let secrets = MemorySecrets::new();
/// secrets.set("token", b"abc").await?;
/// assert_eq!(secrets.get("token").await?.unwrap().as_slice(), b"abc");
/// assert!(secrets.delete("token").await?);
/// # Ok::<(), tinystoragedrivers_types::StorageError>(())
/// # }).unwrap();
/// ```
pub struct MemorySecrets {
    entries: Mutex<BTreeMap<String, SecretEntry>>,
    store_id: u64,
    next_version: AtomicU64,
}

impl MemorySecrets {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            store_id: process_store_prefix() ^ NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            next_version: AtomicU64::new(1),
        }
    }

    /// The map. A panic in another holder cannot leave it half-written (every
    /// mutation is one map call), so a poisoned lock is still safe to use.
    fn entries(&self) -> MutexGuard<'_, BTreeMap<String, SecretEntry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn new_version(&self) -> Result<SecretVersion> {
        let mut revision = self.next_version.load(Ordering::Relaxed);
        loop {
            let next = revision
                .checked_add(1)
                .ok_or_else(|| StorageError::unavailable("memory secret revision exhausted"))?;
            match self.next_version.compare_exchange_weak(
                revision,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => revision = current,
            }
        }
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&self.store_id.to_be_bytes());
        bytes[8..16].copy_from_slice(&revision.to_be_bytes());
        Ok(SecretVersion::from_bytes(bytes))
    }
}

impl Default for MemorySecrets {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemorySecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySecrets")
            .field("len", &self.entries().len())
            .finish()
    }
}

#[async_trait]
impl SecretStore for MemorySecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        Ok(self.entries().get(name).map(|entry| entry.value.clone()))
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        validate_name(name)?;
        Ok(self
            .entries()
            .get(name)
            .map(|entry| (entry.value.clone(), entry.version)))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        let version = self.new_version()?;
        self.entries().insert(
            name.to_string(),
            SecretEntry {
                value: Zeroizing::new(value.to_vec()),
                version,
            },
        );
        Ok(())
    }

    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: SecretPrecondition,
    ) -> Result<SecretVersion> {
        validate_name(name)?;
        let mut entries = self.entries();
        let matches = match (condition, entries.get(name)) {
            (SecretPrecondition::Absent, None) => true,
            (SecretPrecondition::Version(expected), Some(entry)) => entry.version == expected,
            _ => false,
        };
        if !matches {
            return Err(StorageError::conflict("secret precondition did not match"));
        }
        let version = self.new_version()?;
        entries.insert(
            name.to_string(),
            SecretEntry {
                value: Zeroizing::new(value.to_vec()),
                version,
            },
        );
        Ok(version)
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        Ok(self.entries().remove(name).is_some())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        Ok(self
            .entries()
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn enumerable(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
