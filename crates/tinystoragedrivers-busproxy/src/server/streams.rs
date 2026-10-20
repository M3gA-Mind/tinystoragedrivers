//! `Streams` selectors.
use serde_json::Value;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{MAX_BATCH_OPS, MAX_INLINE_BYTES, MAX_PAGE_ITEMS};

use super::documents::bounded_list;
use super::{Reply, StorageServer, decode, encode, invalid, native};
use crate::convert::capability_names;

/// Framing a `ReadWindow` page adds around each record (offset, separators).
const RECORD_FRAMING: usize = 64;

/// Whether `record` can be read back: alone in a `ReadWindow` page it must stay
/// inside the inline byte cap, or the first-item exception would push the reply
/// past the frame limit. Checked before any native write.
fn fits(record: &Value) -> bool {
    record.to_string().len() + RECORD_FRAMING <= MAX_INLINE_BYTES
}

fn records(items: Vec<wire::StreamRecord>) -> Result<Vec<Value>, wire::StorageFailure> {
    if items.len() > MAX_BATCH_OPS || items.iter().any(|record| !fits(&record.0)) {
        return Err(invalid());
    }
    Ok(items.into_iter().map(|record| record.0).collect())
}

impl StorageServer {
    pub(super) async fn streams(&self, selector: &str, request: Value) -> Reply {
        let streams = self.storage.streams();
        match selector {
            "Append" => {
                let r: wire::AppendRequest = decode(request)?;
                if !fits(&r.record) {
                    return Err(invalid());
                }
                encode(&native(streams.append(&r.stream, r.record).await)?)
            }
            "AppendBatch" => {
                let r: wire::AppendBatchRequest = decode(request)?;
                let values = records(r.records)?;
                encode(&native(streams.append_batch(&r.stream, values).await)?)
            }
            "AppendBatchIfOffset" => {
                let r: wire::AppendBatchIfOffsetRequest = decode(request)?;
                let values = records(r.records)?;
                let offset = native(
                    streams
                        .append_batch_if_offset(&r.stream, r.expected_next_offset, values)
                        .await,
                )?;
                encode(&offset)
            }
            "ReadWindow" => {
                let r: wire::ReadWindowRequest = decode(request)?;
                let limit = r.limit.min(MAX_PAGE_ITEMS);
                let window = native(streams.read_window(&r.stream, r.from, limit).await)?;
                let mut size = 0usize;
                let mut items = Vec::with_capacity(window.len());
                for entry in window {
                    size = size.saturating_add(entry.value.to_string().len() + RECORD_FRAMING);
                    if !items.is_empty() && size > MAX_INLINE_BYTES {
                        break;
                    }
                    items.push(wire::StreamItem {
                        offset: entry.offset,
                        record: entry.value,
                    });
                }
                encode(&items)
            }
            "Len" => {
                let r: wire::StreamRequest = decode(request)?;
                encode(&native(streams.len(&r.stream).await)?)
            }
            "TruncateBefore" => {
                let r: wire::TruncateBeforeRequest = decode(request)?;
                encode(&native(streams.truncate_before(&r.stream, r.offset).await)?)
            }
            "DeleteStream" => {
                let r: wire::StreamRequest = decode(request)?;
                encode(&native(streams.delete_stream(&r.stream).await)?)
            }
            "Seal" => {
                let r: wire::StreamRequest = decode(request)?;
                let sealed = native(streams.seal(&r.stream).await)?;
                encode(&wire::SealResponse {
                    base: sealed.base,
                    len: sealed.len,
                    blob_bytes: sealed.blob_bytes,
                })
            }
            "Info" => {
                let r: wire::StreamRequest = decode(request)?;
                let info = native(streams.info(&r.stream).await)?;
                encode(&info.map(|info| wire::StreamInfoResponse {
                    base: info.base,
                    len: info.len,
                    sealed_ms: info.sealed_ms,
                    last_append_ms: info.last_append_ms,
                }))
            }
            "Streams" => {
                let r: wire::ListStreamsRequest = decode(request)?;
                bounded_list(&native(streams.streams(&r.prefix).await)?)
            }
            "Capabilities" => {
                if !request.is_null() {
                    return Err(invalid());
                }
                encode(&wire::CapabilitiesReply {
                    capabilities: capability_names(streams.capabilities()),
                })
            }
            _ => Err(invalid()),
        }
    }
}
