use super::*;

#[test]
fn read_dependencies_round_trip_and_reject_invalid_addresses() {
    for (expected, payload) in [
        (
            ExpectedDocument::Absent,
            serde_json::json!({"kind":"absent"}),
        ),
        (
            ExpectedDocument::Version(Version(3)),
            serde_json::json!({"kind":"version","version":3}),
        ),
    ] {
        let check = ReadCheck {
            collection: "dependencies".into(),
            id: "dep".into(),
            expected,
        };
        let encoded = serde_json::to_string(&check).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
            serde_json::json!({"collection":"dependencies","id":"dep","expected":payload}),
            "public read-check field names and tags are a stable wire contract",
        );
        assert_eq!(serde_json::from_str::<ReadCheck>(&encoded).unwrap(), check);
    }
    let invalid = ReadCheck {
        collection: "dependencies".into(),
        id: String::new(),
        expected: ExpectedDocument::Absent,
    };
    assert!(invalid.validate().is_err());
    assert!(invalid.verify(None).is_err());
}
