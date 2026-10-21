use super::*;

#[test]
fn condition_requires_a_valid_specific_document_predicate() {
    let scope = Scope::new("host:lease").unwrap();
    assert!(WriteCondition::new(scope.clone(), "leases", "one", Filter::eq("epoch", 4)).is_ok());
    assert!(WriteCondition::new(scope.clone(), "bad/name", "one", Filter::eq("epoch", 4)).is_err());
    assert!(
        WriteCondition::new(scope.clone(), "leases", "one", Filter::eq("bad.path.", 4)).is_err()
    );
    assert!(WriteCondition::new(scope, "leases", "one", Filter::All).is_ok());
}

#[test]
fn condition_deadline_is_a_validated_fail_closed_body_predicate() {
    let scope = Scope::new("host:lease").unwrap();
    let condition = WriteCondition::new(scope.clone(), "leases", "one", Filter::eq("epoch", 4))
        .unwrap()
        .with_valid_until_field("lease.expires_ms")
        .unwrap();
    assert_eq!(condition.valid_until_field(), Some("lease.expires_ms"));
    assert!(
        condition
            .clone()
            .with_valid_until_field("lease.bad.path.")
            .is_err()
    );

    let meta = crate::Meta::unknown();
    assert!(condition.matches_at(
        "one",
        &serde_json::json!({"epoch": 4, "lease": {"expires_ms": 101}}),
        &meta,
        100,
    ));
    for body in [
        serde_json::json!({"epoch": 4, "lease": {"expires_ms": 100}}),
        serde_json::json!({"epoch": 4, "lease": {"expires_ms": "101"}}),
        serde_json::json!({"epoch": 4, "lease": {}}),
        serde_json::json!({"epoch": 3, "lease": {"expires_ms": 101}}),
    ] {
        assert!(!condition.matches_at("one", &body, &meta, 100));
    }
}
