//! Confidential typed calls.
use crate::{DeleteRequest, GetRequest, Outcome, PutRequest, Version, Versioned};
use serde_json::Value;
/// Module-side client; names alone never grant confidential admission.
#[derive(Clone, Debug)]
pub struct DocumentClient(tinybus::Proxy);
impl DocumentClient {
    /// Wrap an exact-interface proxy.
    ///
    /// # Errors
    /// Refuses proxies addressing another interface.
    pub fn new(proxy: tinybus::Proxy) -> tinybus::Result<Self> {
        if proxy.interface().as_str() != crate::INTERFACE {
            return Err(tinybus::Error::failed(
                "document callback interface mismatch",
            ));
        }
        Ok(Self(proxy))
    }
    /// Read one document; absent documents return None.
    ///
    /// # Errors
    /// Transport/admission failures are outer errors; storage failures are inner.
    pub async fn get(
        &self,
        request: GetRequest,
    ) -> tinybus::Result<Outcome<Option<Versioned<Value>>>> {
        self.0.call_confidential("Get", (request,)).await
    }
    /// Atomically write under the supplied condition.
    ///
    /// # Errors
    /// Transport/admission failures are outer errors; storage failures are inner.
    pub async fn put(&self, request: PutRequest) -> tinybus::Result<Outcome<Version>> {
        self.0.call_confidential("Put", (request,)).await
    }
    /// Atomically delete under the supplied condition.
    ///
    /// # Errors
    /// Transport/admission failures are outer errors; storage failures are inner.
    pub async fn delete(&self, request: DeleteRequest) -> tinybus::Result<Outcome<bool>> {
        self.0.call_confidential("Delete", (request,)).await
    }
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
