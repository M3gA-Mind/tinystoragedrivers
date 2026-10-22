use super::*;
use serde_json::json;

#[test]
fn secret_messages_roundtrip_and_reject_unknown_fields() {
    let set = SetSecretRequest {
        name: "mcp/env".into(),
        value: vec![1, 2, 3],
    };
    let wire = serde_json::to_value(&set).unwrap();
    assert_eq!(
        serde_json::from_value::<SetSecretRequest>(wire).unwrap(),
        set
    );
    assert!(
        serde_json::from_value::<SecretNameRequest>(json!({"name":"n","scope":"bob"})).is_err()
    );
    assert!(serde_json::from_value::<ListSecretsRequest>(json!({"prefix":"p","x":1})).is_err());
}

#[test]
fn secret_debug_output_is_redacted() {
    let set = SetSecretRequest {
        name: "very-secret-name".into(),
        value: b"hunter2".to_vec(),
    };
    let text = format!(
        "{set:?} {:?}",
        SecretValue {
            value: b"hunter2".to_vec()
        }
    );
    assert!(!text.contains("hunter2") && !text.contains("very-secret-name"));
}

#[test]
fn secret_selectors_are_fixed() {
    assert_eq!(SECRETS_OBJECT, "Secrets");
    assert_eq!(
        SECRETS_SELECTORS,
        [
            "Capabilities",
            "Get",
            "GetVersioned",
            "Set",
            "SetIf",
            "Delete",
            "List"
        ]
    );
    assert!(!is_secrets_selector("Dump"));
}

#[test]
fn secret_capabilities_report_only_name_enumeration() {
    let available = SecretsCapabilitiesReply { enumerable: true };
    assert_eq!(
        serde_json::from_value::<SecretsCapabilitiesReply>(
            serde_json::to_value(available).unwrap()
        )
        .unwrap(),
        available
    );
    assert!(
        serde_json::from_value::<SecretsCapabilitiesReply>(json!({
            "enumerable": true,
            "scope": "forged"
        }))
        .is_err()
    );
}

#[test]
fn conditional_secret_messages_roundtrip_without_exposing_values_in_debug() {
    let version = tinystoragedrivers_types::SecretVersion::from_bytes([7; 32]);
    let read = VersionedSecretValue {
        value: b"hidden".to_vec(),
        version,
    };
    assert_eq!(
        serde_json::from_value::<VersionedSecretValue>(serde_json::to_value(&read).unwrap())
            .unwrap(),
        read
    );
    let set = SetSecretIfRequest {
        name: "secret-name".into(),
        value: b"hidden".to_vec(),
        condition: SecretPrecondition::Version(version),
    };
    let wire = serde_json::to_value(&set).unwrap();
    assert_eq!(
        serde_json::from_value::<SetSecretIfRequest>(wire).unwrap(),
        set
    );
    assert!(!format!("{set:?} {read:?}").contains("hidden"));
}
