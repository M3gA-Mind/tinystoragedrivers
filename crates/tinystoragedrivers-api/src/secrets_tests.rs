use super::*;

#[derive(Debug)]
struct CustomSecretStore;

#[async_trait]
impl SecretStore for CustomSecretStore {
    async fn get(&self, _: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        Ok(None)
    }

    async fn set(&self, _: &str, _: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _: &str) -> Result<bool> {
        Ok(false)
    }

    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn backend_name(&self) -> &'static str {
        "custom"
    }
}

#[test]
fn custom_secret_store_uses_the_historical_enumerable_default() {
    assert!(CustomSecretStore.enumerable());
}
