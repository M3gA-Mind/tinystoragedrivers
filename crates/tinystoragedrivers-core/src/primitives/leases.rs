//! [`Leases`]: time-limited ownership with a fencing epoch.
//!
//! # Why expiry is a body field
//!
//! A lease could carry its expiry as record metadata
//! ([`WriteOptions::expires_ms`](crate::WriteOptions)), which would hide it
//! once it ran out. It deliberately does not. A takeover must read the expired
//! record to learn its `epoch` and write `epoch + 1`; if the record vanished,
//! the next holder would restart at epoch 1 and a stale holder's
//! [`Fence`] over epoch 1 would hold again. So the record stays, its
//! `expires_ms` field says when the lease lapses, and `epoch` only ever moves
//! forward.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::LEASES;
use super::repo::{Repo, RetryPolicy};
use crate::document::DocumentStore;
use crate::error::{Result, StorageError};
use crate::fence::Fence;
use crate::key::Key;
use crate::memory::{Clock, system_clock};
use crate::scope::Scope;

/// The stored body: `{holder, epoch, expires_ms}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Body {
    holder: String,
    epoch: u64,
    expires_ms: i64,
}

/// A lease as its holder sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    id: String,
    /// Who holds the lease.
    pub holder: String,
    /// Moves up each time a different holder takes the lease over, and stays
    /// put while one holder renews. This is the write-fence token.
    pub epoch: u64,
    /// Unix epoch milliseconds at which the lease lapses.
    pub expires_ms: i64,
}

impl Lease {
    /// A [`Fence`] that holds while this lease's epoch is still the stored
    /// one. Bind it with
    /// [`StorageBackend::for_scope_fenced`](crate::StorageBackend::for_scope_fenced)
    /// so a holder that lost the lease cannot land a late write.
    ///
    /// `scope` is the scope the lease record lives in, the one the
    /// [`Leases`] handle was opened for.
    ///
    /// The fence checks the epoch only: it refuses writes once another holder
    /// took over, not merely because the clock passed `expires_ms`. A holder
    /// that wants the second guarantee renews before expiry.
    #[must_use]
    pub fn fence(&self, scope: &Scope) -> Fence {
        Fence::epoch(scope.clone(), LEASES, self.id.clone(), "epoch", self.epoch)
    }

    fn from_body(id: String, body: Body) -> Self {
        Self {
            id,
            holder: body.holder,
            epoch: body.epoch,
            expires_ms: body.expires_ms,
        }
    }
}

/// Leases over [`LEASES`], one record per `(owner, key)`.
///
/// Time comes from an injectable [`Clock`] (Unix epoch milliseconds), so
/// tests drive expiry without sleeping. A lease is live while
/// `now < expires_ms`.
#[derive(Clone)]
pub struct Leases {
    repo: Repo<Body>,
    clock: Clock,
}

impl std::fmt::Debug for Leases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leases")
            .field("repo", &self.repo)
            .finish_non_exhaustive()
    }
}

impl Leases {
    /// Leases on `docs`, which must be bound to the scope that holds them
    /// (typically `cluster` for process placement), timed by the system clock.
    ///
    /// # Panics
    ///
    /// Never: the collection name is a valid constant.
    #[must_use]
    #[allow(clippy::expect_used, reason = "LEASES is a valid constant name")]
    pub fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            repo: Repo::new(docs, LEASES).expect("tsd.leases is a valid collection"),
            clock: Arc::new(system_clock),
        }
    }

    /// Time leases with `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Replace the retry policy of the underlying compare-and-swap loop.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.repo = self.repo.with_retry(retry);
        self
    }

    fn now(&self) -> Result<i64> {
        i64::try_from((self.clock)()).map_err(|_| StorageError::invalid_input("clock out of range"))
    }

    fn deadline(&self, ttl_ms: i64) -> Result<(i64, i64)> {
        if ttl_ms <= 0 {
            return Err(StorageError::invalid_input("a lease ttl must be positive"));
        }
        let now = self.now()?;
        let expires = now
            .checked_add(ttl_ms)
            .ok_or_else(|| StorageError::invalid_input("lease ttl out of range"))?;
        Ok((now, expires))
    }

    /// Take the lease `(owner, key)` for `holder` for `ttl_ms` milliseconds.
    ///
    /// Succeeds when the lease is absent (epoch 1), expired or released
    /// (epoch + 1 when the holder differs), or already held by `holder` (the
    /// epoch stays, the expiry is extended). Returns `None` while another
    /// holder's lease is live. Racing acquirers get exactly one winner.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a non-positive ttl, or a storage error.
    pub async fn acquire(
        &self,
        owner: &str,
        key: &str,
        holder: &str,
        ttl_ms: i64,
    ) -> Result<Option<Lease>> {
        let id = id(owner, key)?;
        let stored = self
            .repo
            .update(&id, |current| {
                let (now, expires_ms) = self.deadline(ttl_ms)?;
                let epoch = match current {
                    None => 1,
                    Some(lease) if lease.holder == holder => lease.epoch,
                    Some(lease) if lease.expires_ms > now => return Ok(None),
                    Some(lease) => lease
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| StorageError::backend("lease epoch overflow"))?,
                };
                Ok(Some(Body {
                    holder: holder.to_owned(),
                    epoch,
                    expires_ms,
                }))
            })
            .await?;
        Ok(stored.map(|(body, _)| Lease::from_body(id, body)))
    }

    /// Extend a lease `holder` still holds by `ttl_ms` from now, keeping its
    /// epoch. Returns `None` when the lease is gone, lapsed or held by someone
    /// else: the caller has lost it and must stop acting on it (renewing never
    /// resurrects a lapsed lease; call [`acquire`](Self::acquire)).
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a non-positive ttl, or a storage error.
    pub async fn renew(
        &self,
        owner: &str,
        key: &str,
        holder: &str,
        ttl_ms: i64,
    ) -> Result<Option<Lease>> {
        let id = id(owner, key)?;
        let stored = self
            .repo
            .update(&id, |current| {
                let (now, expires_ms) = self.deadline(ttl_ms)?;
                Ok(current
                    .filter(|lease| lease.holder == holder && lease.expires_ms > now)
                    .map(|lease| Body {
                        expires_ms,
                        ..lease
                    }))
            })
            .await?;
        Ok(stored.map(|(body, _)| Lease::from_body(id, body)))
    }

    /// Give the lease up: only the holder of a live lease can, and it is
    /// marked expired at once (the record and its epoch stay, so the next
    /// holder continues the epoch). Returns whether a lease was released.
    ///
    /// # Errors
    ///
    /// A storage error.
    pub async fn release(&self, owner: &str, key: &str, holder: &str) -> Result<bool> {
        let id = id(owner, key)?;
        let now = self.now()?;
        let stored = self
            .repo
            .update(&id, |current| {
                Ok(current
                    .filter(|lease| lease.holder == holder && lease.expires_ms > now)
                    .map(|lease| Body {
                        expires_ms: now,
                        ..lease
                    }))
            })
            .await?;
        Ok(stored.is_some())
    }

    /// The live lease `(owner, key)`, or `None` when absent or lapsed.
    ///
    /// # Errors
    ///
    /// A storage error.
    pub async fn current(&self, owner: &str, key: &str) -> Result<Option<Lease>> {
        let id = id(owner, key)?;
        let now = self.now()?;
        Ok(self
            .repo
            .get(&id)
            .await?
            .filter(|found| found.doc.expires_ms > now)
            .map(|found| Lease::from_body(id, found.doc)))
    }
}

fn id(owner: &str, key: &str) -> Result<String> {
    Ok(Key::of([owner, key])?.as_str().to_owned())
}

#[cfg(test)]
#[path = "leases_tests.rs"]
mod tests;
