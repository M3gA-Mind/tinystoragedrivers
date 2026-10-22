use super::*;

use std::future::Future;
use std::task::{Context, Poll, Waker};

use tinystoragedrivers_types::{ErrorKind, Page, StorageError, Versioned};

#[derive(Debug)]
struct BareKv;

#[async_trait]
impl KvStore for BareKv {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    async fn get(&self, _: &str) -> Result<Option<Versioned<Value>>> {
        Err(StorageError::backend("unused"))
    }
    async fn put(&self, _: &str, _: Value) -> Result<Version> {
        Err(StorageError::backend("unused"))
    }
    async fn cas(&self, _: &str, _: Value, _: Precondition) -> Result<Version> {
        Err(StorageError::backend("unused"))
    }
    async fn delete(&self, _: &str, _: Precondition) -> Result<bool> {
        Err(StorageError::backend("unused"))
    }
    async fn list(&self, _: &str, _: Option<Cursor>, _: usize) -> Result<Page<Versioned<Value>>> {
        Err(StorageError::backend("unused"))
    }
}

#[test]
fn ttl_write_defaults_to_typed_unsupported() {
    let result =
        run_ready(BareKv.put_with_ttl("key", serde_json::json!(1), Duration::from_secs(1)));
    assert_eq!(
        result.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Ttl)
    );
}

fn run_ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("default KV TTL write unexpectedly pended"),
    }
}
