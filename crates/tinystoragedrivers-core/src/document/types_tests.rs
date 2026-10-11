use super::*;

#[test]
fn read_dependencies_round_trip_and_reject_invalid_addresses() {
    for expected in [
        ExpectedDocument::Absent,
        ExpectedDocument::Version(Version(3)),
    ] {
        let check = ReadCheck {
            collection: "dependencies".into(),
            id: "dep".into(),
            expected,
        };
        let encoded = serde_json::to_string(&check).unwrap();
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
