//! The routing representation contains no authority or extension fields.
use super::*;
use serde_json::json;
#[test]
fn routing_wire_shape_is_versioned_and_rejects_authority_fields() {
    let route = ScopedStorageRoute {
        version: 1,
        destination: ":1.2".into(),
        object_root: "/storage".into(),
        profile: ScopedStorageProfile::DocumentsStreamsSecretsV1,
    };
    let value = json!({"version":1,"destination":":1.2","object_root":"/storage","profile":"DocumentsStreamsSecretsV1"});
    assert_eq!(serde_json::to_value(&route).unwrap(), value);
    assert_eq!(
        serde_json::from_value::<ScopedStorageRoute>(value.clone()).unwrap(),
        route
    );
    for field in ["scope", "generation", "backend", "fence", "grant"] {
        let mut forged = value.clone();
        forged[field] = json!("private");
        assert!(serde_json::from_value::<ScopedStorageRoute>(forged).is_err());
    }
    let mut forged = value;
    forged["profile"] = json!("Blobs");
    assert!(serde_json::from_value::<ScopedStorageRoute>(forged).is_err());
}
