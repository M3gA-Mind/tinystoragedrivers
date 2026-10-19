//! Record metadata: what the driver keeps about a document outside its body.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Metadata recorded alongside a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Meta {
    /// Declared document schema version, or zero when undeclared.
    #[serde(default)]
    pub schema: u32,
    /// Creation time in Unix epoch milliseconds.
    #[serde(default)]
    pub created_ms: i64,
    /// Most recent update time in Unix epoch milliseconds.
    #[serde(default)]
    pub updated_ms: i64,
    /// Expiry time in Unix epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
}
impl Meta {
    /// Metadata for an engine that does not persist metadata.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            schema: 0,
            created_ms: 0,
            updated_ms: 0,
            expires_ms: None,
        }
    }
    /// Tests whether the value has expired at `now_ms`.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_ms.is_some_and(|expiry| expiry <= now_ms)
    }
    /// Resolves one reserved metadata field to a JSON value.
    #[must_use]
    pub fn field(&self, path: &str) -> Option<Value> {
        match path {
            "_schema" => Some(Value::from(self.schema)),
            "_created_ms" => Some(Value::from(self.created_ms)),
            "_updated_ms" => Some(Value::from(self.updated_ms)),
            "_expires_ms" => self.expires_ms.map(Value::from),
            _ => None,
        }
    }
}

/// What a write declares about the document beyond its body.
///
/// A write replaces these values: a plain document put
/// declares [`WriteOptions::default`], so an upsert without options clears an
/// earlier expiry. The creation and update times are never part of a write;
/// the driver sets them from its own clock.
///
/// ```
/// use tinystoragedrivers_types::WriteOptions;
///
/// let options = WriteOptions::new().schema(3).expires_ms(1_700_000_000_000);
/// assert_eq!(options.schema, 3);
/// assert!(WriteOptions::new().is_default());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WriteOptions {
    /// The body version the writer declares; `0` means undeclared.
    #[serde(default)]
    pub schema: u32,
    /// Unix epoch milliseconds from which the document reads as absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
    /// Unix epoch milliseconds to record as the document's creation time
    /// instead of the driver's clock. **Migration only**: a step that rewrites
    /// a record uses it so history keeps its original times. Honoured only by
    /// drivers with the `RecordMetadata` capability;
    /// the others refuse a write that sets it. The update time still comes from
    /// the driver clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_ms: Option<i64>,
}

impl WriteOptions {
    /// Undeclared schema, no expiry, driver-clock creation time.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            schema: 0,
            expires_ms: None,
            created_ms: None,
        }
    }

    /// Record `created_ms` as the creation time (migration only; see the
    /// field).
    #[must_use]
    pub const fn created_ms(mut self, created_ms: i64) -> Self {
        self.created_ms = Some(created_ms);
        self
    }

    /// Declare the body version.
    #[must_use]
    pub const fn schema(mut self, schema: u32) -> Self {
        self.schema = schema;
        self
    }

    /// Expire the document at `expires_ms` (Unix epoch milliseconds).
    #[must_use]
    pub const fn expires_ms(mut self, expires_ms: i64) -> Self {
        self.expires_ms = Some(expires_ms);
        self
    }

    /// Whether these are the options of a plain write.
    #[must_use]
    pub const fn is_default(&self) -> bool {
        self.schema == 0 && self.expires_ms.is_none() && self.created_ms.is_none()
    }
}

#[cfg(test)]
#[path = "meta_tests.rs"]
mod tests;
