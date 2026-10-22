use super::*;

use std::future::Future;
use std::task::{Context, Poll, Waker};

use serde_json::json;
use tinystoragedrivers_types::{Capability, ErrorKind, StreamEntry};

#[derive(Debug)]
struct BasicStreams;

#[async_trait]
impl StreamStore for BasicStreams {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    async fn append(&self, _: &str, _: serde_json::Value) -> Result<u64> {
        Ok(1)
    }

    async fn append_batch(&self, _: &str, values: Vec<serde_json::Value>) -> Result<u64> {
        Ok(values.len() as u64)
    }

    async fn read_window(&self, _: &str, _: u64, _: usize) -> Result<Vec<StreamEntry>> {
        Ok(vec![])
    }

    async fn len(&self, _: &str) -> Result<u64> {
        Ok(0)
    }

    async fn truncate_before(&self, _: &str, _: u64) -> Result<u64> {
        Ok(0)
    }

    async fn delete_stream(&self, _: &str) -> Result<bool> {
        Ok(false)
    }

    async fn streams(&self, _: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
}

#[test]
fn optional_conditional_append_and_seal_operations_return_precise_unsupported_errors() {
    let store = BasicStreams;

    assert_eq!(
        run_ready(store.append_batch_if_offset("s", 0, vec![json!(1)]))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ConditionalStreamAppend)
    );
    assert_eq!(
        run_ready(store.seal("s")).unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::StreamSeal)
    );
    assert_eq!(
        run_ready(store.delete_sealed_stream("s", 123))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::StreamSeal)
    );
    assert_eq!(
        run_ready(store.info("s")).unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::StreamSeal)
    );
}

#[test]
fn required_stream_methods_remain_available_on_a_minimal_implementation() {
    let store = BasicStreams;

    assert_eq!(run_ready(store.append("s", json!(1))).unwrap(), 1);
    assert_eq!(
        run_ready(store.append_batch("s", vec![json!(1), json!(2)])).unwrap(),
        2
    );
    assert_eq!(
        run_ready(store.read_window("s", 0, 10)).unwrap(),
        Vec::<StreamEntry>::new()
    );
    assert_eq!(run_ready(store.len("s")).unwrap(), 0);
    assert_eq!(run_ready(store.truncate_before("s", 10)).unwrap(), 0);
    assert!(!run_ready(store.delete_stream("s")).unwrap());
    assert_eq!(
        run_ready(store.streams("prefix")).unwrap(),
        Vec::<String>::new()
    );
}

fn run_ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly pended"),
    }
}
