use super::*;

#[derive(Debug)]
struct EmptyKv;

#[async_trait]
impl KvStore for EmptyKv {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        tinystoragedrivers_types::Capabilities::all()
    }
    async fn get(&self, _: &str) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }
    async fn put(&self, _: &str, _: Value) -> Result<Version> {
        Ok(Version(1))
    }
    async fn cas(&self, _: &str, _: Value, _: Precondition) -> Result<Version> {
        Ok(Version(1))
    }
    async fn delete(&self, _: &str, _: Precondition) -> Result<bool> {
        Ok(false)
    }
    async fn list(&self, _: &str, _: Option<Cursor>, _: usize) -> Result<Page<Versioned<Value>>> {
        Ok(Page {
            items: Vec::new(),
            next: None,
        })
    }
}

#[tokio::test]
async fn decorator_hides_ttl_capability_and_rejects_ttl_writes() {
    let store = NoNativeTtl::new(Arc::new(EmptyKv));
    assert!(!store.capabilities().contains(Capability::Ttl));
    let error = store
        .put_with_ttl("k", Value::Null, Duration::from_secs(1))
        .await
        .expect_err("decorator disables native TTL");
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(Capability::Ttl)
    );
    assert!(store.get("k").await.expect("read delegates").is_none());
}

#[tokio::test]
async fn no_native_ttl_forwards_the_other_kv_operations() -> tinystoragedrivers_types::Result<()> {
    let scope = tinystoragedrivers_types::Scope::new("no_native_ttl_suite")?;
    let storage = crate::memory_storage(&scope);
    let store = NoNativeTtl::new(Arc::clone(storage.kv()));
    crate::conformance::kv::run(&store).await;
    Ok(())
}
