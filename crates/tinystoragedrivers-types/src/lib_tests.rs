//! Crate-level wire and value contract tests.

use super::*;

#[test]
fn scope_child_escapes_utf8_and_preserves_legacy_root() {
    let root = Scope::new("agent:legacy-雪").unwrap();
    let child = root.child("agent/a%b 雪").unwrap();
    assert_eq!(root.as_str(), "agent:legacy-雪");
    assert_eq!(child.as_str(), "agent:legacy-雪/agent%2Fa%25b%20%E9%9B%AA");
    assert_eq!(serde_json::to_string(&root).unwrap(), "\"agent:legacy-雪\"");
}

#[test]
fn scope_deserialization_reuses_validation() {
    assert!(serde_json::from_str::<Scope>("\"\"").is_err());
    assert!(serde_json::from_str::<Scope>("\"bad scope\"").is_err());
    assert!(serde_json::from_str::<Scope>(&format!("\"{}\"", "x".repeat(257))).is_err());
    let valid = serde_json::from_str::<Scope>("\"old:scope/value\"").unwrap();
    assert_eq!(valid.as_str(), "old:scope/value");
}

#[test]
fn child_rejects_empty_segments_and_overlong_composition() {
    let root = Scope::new("x").unwrap();
    assert!(root.child("").is_err());
    assert!(Scope::new(&"x".repeat(255)).unwrap().child("a").is_err());
}

#[test]
fn capability_names_are_stable_snake_case() {
    assert_eq!(
        serde_json::to_string(&Capability::ConditionalStreamAppend).unwrap(),
        "\"conditional_stream_append\""
    );
    assert_eq!(
        Capabilities::none()
            .with(Capability::Streams)
            .iter()
            .collect::<Vec<_>>(),
        vec![Capability::Streams]
    );
}
