//! `Kv` selectors, served directly by the bound key/value port.
use std::time::Duration;

use serde_json::Value;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_types::{Meta, Version, Versioned};

use super::{Reply, StorageServer, decode, encode, invalid, native};
use crate::convert::capability_names;

impl StorageServer {
    pub(super) async fn kv(&self, selector: &str, request: Value) -> Reply {
        let kv = self.storage.kv();
        match selector {
            "Get" => {
                let r: wire::KvKeyRequest = decode(request)?;
                let found = native(kv.get(&r.key).await)?;
                if let Some(value) = &found {
                    let encoded = serde_json::to_vec(value).map_err(|_| invalid())?;
                    if encoded.len() > wire::MAX_INLINE_BYTES {
                        return Err(invalid());
                    }
                }
                encode(&found)
            }
            "Put" => {
                let r: wire::KvPutRequest = decode(request)?;
                ensure_inline_record(&r.key, &r.value)?;
                encode(&native(kv.put(&r.key, r.value).await)?)
            }
            "Cas" => {
                let r: wire::KvCasRequest = decode(request)?;
                ensure_inline_record(&r.key, &r.value)?;
                encode(&native(kv.cas(&r.key, r.value, r.expected).await)?)
            }
            "Delete" => {
                let r: wire::KvDeleteRequest = decode(request)?;
                encode(&native(kv.delete(&r.key, r.expected).await)?)
            }
            "List" => {
                let r: wire::KvListRequest = decode(request)?;
                let page = native(
                    kv.list(&r.prefix, r.cursor, r.limit.min(wire::MAX_PAGE_ITEMS))
                        .await,
                )?;
                if page.items.len() > wire::MAX_PAGE_ITEMS {
                    return Err(invalid());
                }
                let value = encode(&page)?;
                if value.to_string().len() > wire::MAX_INLINE_BYTES {
                    return Err(invalid());
                }
                Ok(value)
            }
            "PutWithTtl" => {
                let r: wire::KvPutWithTtlRequest = decode(request)?;
                ensure_inline_record(&r.key, &r.value)?;
                encode(&native(
                    kv.put_with_ttl(&r.key, r.value, Duration::from_millis(r.ttl_ms))
                        .await,
                )?)
            }
            "Capabilities" => {
                if !request.is_null() {
                    return Err(invalid());
                }
                encode(&wire::KvCapabilitiesReply {
                    capabilities: capability_names(kv.capabilities()),
                })
            }
            _ => Err(invalid()),
        }
    }
}

fn ensure_inline_record(key: &str, value: &Value) -> Result<(), wire::StorageFailure> {
    // Account for the actual public read envelope, including its key, version,
    // and metadata fields, before allowing a write to make it unreadable.
    let record = Versioned {
        id: key.to_owned(),
        version: Version(u64::MAX),
        doc: value,
        meta: Meta {
            schema: u32::MAX,
            created_ms: i64::MIN,
            updated_ms: i64::MIN,
            expires_ms: Some(i64::MIN),
        },
    };
    let encoded = serde_json::to_vec(&record).map_err(|_| invalid())?;
    if encoded.len() > wire::MAX_INLINE_BYTES {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;
