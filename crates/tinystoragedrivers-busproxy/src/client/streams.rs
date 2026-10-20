//! `StreamStore` over the `Streams` object.
use serde_json::Value;
use tinystoragedrivers_api::{StreamStore, async_trait};
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::MAX_BATCH_OPS;
use tinystoragedrivers_types::{
    Capability, Result, SealOutcome, StorageError, StreamEntry, StreamInfo,
};

use super::Wire;
use crate::transport::Object;

/// The stream port over a bus.
#[derive(Clone, Debug)]
pub struct BusStreams {
    wire: Wire,
    capabilities: tinystoragedrivers_types::Capabilities,
}

impl BusStreams {
    pub(super) fn new(wire: Wire, capabilities: tinystoragedrivers_types::Capabilities) -> Self {
        Self { wire, capabilities }
    }

    async fn call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        selector: &str,
        request: &Req,
        unsupported_as: Option<Capability>,
    ) -> Result<Resp> {
        self.wire
            .call(Object::Streams, selector, request, unsupported_as)
            .await
    }
}

fn records(values: Vec<Value>) -> Result<Vec<wire::StreamRecord>> {
    if values.len() > MAX_BATCH_OPS {
        return Err(StorageError::invalid_input(
            "a stream append batch is limited to 1000 records",
        ));
    }
    Ok(values.into_iter().map(wire::StreamRecord).collect())
}

#[async_trait]
impl StreamStore for BusStreams {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.capabilities
    }
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        let request = wire::AppendRequest {
            stream: stream.to_owned(),
            record: value,
        };
        self.call("Append", &request, None).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        let request = wire::AppendBatchRequest {
            stream: stream.to_owned(),
            records: records(values)?,
        };
        self.call("AppendBatch", &request, None).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        let request = wire::AppendBatchIfOffsetRequest {
            stream: stream.to_owned(),
            expected_next_offset,
            records: records(values)?,
        };
        self.call(
            "AppendBatchIfOffset",
            &request,
            Some(Capability::ConditionalStreamAppend),
        )
        .await
    }

    /// Windows above the page cap are assembled from several calls. A window
    /// the server cut short at its byte cap is shorter than `limit`; callers
    /// continue from the last offset plus one.
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        let mut out: Vec<StreamEntry> = Vec::new();
        let mut next = from;
        while out.len() < limit {
            let request = wire::ReadWindowRequest {
                stream: stream.to_owned(),
                from: next,
                limit: limit - out.len(),
            };
            let window: wire::ReadWindowResponse = self.call("ReadWindow", &request, None).await?;
            // Only an empty window ends the stream early: the server also cuts
            // a page short at its byte cap, so a short page is not the end.
            let Some(last) = window.last() else { break };
            next = last.offset.saturating_add(1);
            out.extend(window.into_iter().map(|item| StreamEntry {
                offset: item.offset,
                value: item.record,
            }));
        }
        Ok(out)
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        let request = wire::StreamRequest {
            stream: stream.to_owned(),
        };
        self.call("Len", &request, None).await
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        let request = wire::TruncateBeforeRequest {
            stream: stream.to_owned(),
            offset,
        };
        self.call("TruncateBefore", &request, None).await
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        let request = wire::StreamRequest {
            stream: stream.to_owned(),
        };
        self.call("DeleteStream", &request, None).await
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        let request = wire::StreamRequest {
            stream: stream.to_owned(),
        };
        let sealed: wire::SealResponse = self
            .call("Seal", &request, Some(Capability::StreamSeal))
            .await?;
        Ok(SealOutcome::new(sealed.base, sealed.len, sealed.blob_bytes))
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        let request = wire::StreamRequest {
            stream: stream.to_owned(),
        };
        let info: Option<wire::StreamInfoResponse> = self
            .call("Info", &request, Some(Capability::StreamSeal))
            .await?;
        Ok(info
            .map(|info| StreamInfo::new(info.base, info.len, info.sealed_ms, info.last_append_ms)))
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let request = wire::ListStreamsRequest {
            prefix: prefix.to_owned(),
        };
        self.call("Streams", &request, None).await
    }
}

#[cfg(test)]
#[path = "streams_tests.rs"]
mod tests;
