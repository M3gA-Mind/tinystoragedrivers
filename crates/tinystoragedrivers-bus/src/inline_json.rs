//! Inline document data is JSON text, never a transport stream capability.
//!
//! `TinyBus` deliberately refuses recursively stream-shaped objects in private
//! calls. Encoding only document data as text preserves arbitrary business JSON
//! while keeping native stream handles out of this contract.
use serde::{Deserialize, Deserializer, Serializer};
use serde_json::Value;

pub(crate) fn serialize<S: Serializer>(document: &Value, serializer: S) -> Result<S::Ok, S::Error> {
    let text = serde_json::to_string(document)
        .map_err(|_| serde::ser::Error::custom("inline document serialization failed"))?;
    serializer.serialize_str(&text)
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
    let text = String::deserialize(deserializer)?;
    serde_json::from_str(&text)
        .map_err(|_| serde::de::Error::custom("invalid inline document json"))
}

#[cfg(test)]
#[path = "inline_json_tests.rs"]
mod tests;
