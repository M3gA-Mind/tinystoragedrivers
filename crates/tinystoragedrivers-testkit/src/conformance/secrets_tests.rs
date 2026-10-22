use super::*;
use async_trait::async_trait;
use std::sync::Arc;
use tinystoragedrivers_api::SecretStore;
use tinystoragedrivers_types::{Capability, Result, SecretVersion, StorageError};
use zeroize::Zeroizing;

#[derive(Debug)]
struct UnsupportedConditionalSecrets {
    inner: Arc<dyn SecretStore>,
    missing_versioned: bool,
}

#[async_trait]
impl SecretStore for UnsupportedConditionalSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.inner.get(name).await
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        let _ = name;
        if self.missing_versioned {
            return Ok(None);
        }
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "versioned reads disabled by test adapter",
        ))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        self.inner.set(name, value).await
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        self.inner.delete(name).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }

    fn enumerable(&self) -> bool {
        self.inner.enumerable()
    }

    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }
}

#[tokio::test]
async fn suite_accepts_stores_without_conditional_secret_reads() -> Result<()> {
    let scope = tinystoragedrivers_types::Scope::new("secret_without_versions")?;
    let inner = crate::memory_storage(&scope).secrets().clone();
    run(&UnsupportedConditionalSecrets {
        inner,
        missing_versioned: false,
    })
    .await;
    Ok(())
}

#[tokio::test]
async fn suite_reports_a_disappearing_versioned_secret() -> Result<()> {
    let scope = tinystoragedrivers_types::Scope::new("missing_versioned_secret")?;
    let inner = crate::memory_storage(&scope).secrets().clone();
    let adapter = UnsupportedConditionalSecrets {
        inner,
        missing_versioned: true,
    };
    let result = tokio::spawn(async move {
        run(&adapter).await;
    })
    .await;
    assert!(
        result.is_err(),
        "the suite should fail on a missing written secret"
    );
    Ok(())
}
