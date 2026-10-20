//! Native key/value operations over the `Kv` storage object.
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Page, Precondition, Result, Version, Versioned,
};

use super::Wire;
use crate::transport::Object;

/// A key/value port bound by the host to this transport's caller scope.
#[derive(Clone, Debug)]
pub struct BusKv {
    wire: Wire,
    capabilities: Capabilities,
}

impl BusKv {
    pub(super) fn new(wire: Wire, capabilities: Capabilities) -> Self {
        Self { wire, capabilities }
    }
}

#[async_trait::async_trait]
impl KvStore for BusKv {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        self.wire
            .call(
                Object::Kv,
                "Get",
                &wire::KvKeyRequest { key: key.into() },
                None,
            )
            .await
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.wire
            .call(
                Object::Kv,
                "Put",
                &wire::KvPutRequest {
                    key: key.into(),
                    value,
                },
                None,
            )
            .await
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.wire
            .call(
                Object::Kv,
                "Cas",
                &wire::KvCasRequest {
                    key: key.into(),
                    value,
                    expected,
                },
                None,
            )
            .await
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        self.wire
            .call(
                Object::Kv,
                "Delete",
                &wire::KvDeleteRequest {
                    key: key.into(),
                    expected,
                },
                None,
            )
            .await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        self.wire
            .call(
                Object::Kv,
                "List",
                &wire::KvListRequest {
                    prefix: prefix.into(),
                    cursor,
                    limit,
                },
                None,
            )
            .await
    }

    async fn put_with_ttl(
        &self,
        key: &str,
        value: Value,
        ttl: std::time::Duration,
    ) -> Result<Version> {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        self.wire
            .call(
                Object::Kv,
                "PutWithTtl",
                &wire::KvPutWithTtlRequest {
                    key: key.into(),
                    value,
                    ttl_ms,
                },
                Some(Capability::Ttl),
            )
            .await
    }
}
