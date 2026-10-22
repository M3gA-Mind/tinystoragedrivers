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
    assert_eq!(
        condition.clauses()[0].valid_until_field(),
        Some("lease.expires_ms")
    );
    assert!(
        condition
            .clone()
            .with_valid_until_field("lease.bad.path.")
            .is_err()
    );

    let meta = crate::Meta::unknown();
    assert!(condition.clauses()[0].matches_at(
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
        assert!(!condition.clauses()[0].matches_at("one", &body, &meta, 100));
    }
}

#[test]
fn composed_conditions_keep_each_guard_and_deadline_independent() {
    let profile = WriteCondition::new(
        Scope::new("host:profile").unwrap(),
        "leases",
        "profile",
        Filter::eq("epoch", 7),
    )
    .unwrap()
    .with_valid_until_field("expires_ms")
    .unwrap();
    let import = WriteCondition::new(
        Scope::new("host:import").unwrap(),
        "claims",
        "source-a",
        Filter::eq("state", "importing"),
    )
    .unwrap()
    .with_valid_until_field("claim.until_ms")
    .unwrap();
    let combined = profile.clone().and(import.clone()).unwrap();
    assert_eq!(combined.clauses().len(), 2);
    assert_eq!(combined.clauses()[0].scope().as_str(), "host:profile");
    assert_eq!(combined.clauses()[1].scope().as_str(), "host:import");
    assert_eq!(
        combined.clauses()[0].valid_until_field(),
        Some("expires_ms")
    );
    assert_eq!(
        combined.clauses()[1].valid_until_field(),
        Some("claim.until_ms")
    );
    assert_eq!(
        combined
            .clone()
            .and(profile.clone())
            .unwrap()
            .clauses()
            .len(),
        3
    );
    assert!(
        combined
            .clone()
            .with_valid_until_field("another_deadline")
            .is_err()
    );

    let nine = (0..MAX_WRITE_CONDITION_CLAUSES)
        .map(|index| {
            WriteCondition::new(
                Scope::new(&format!("guard-{index}")).unwrap(),
                "guards",
                "one",
                Filter::eq("active", true),
            )
            .unwrap()
        })
        .reduce(|left, right| left.and(right).unwrap())
        .unwrap();
    assert_eq!(nine.clauses().len(), MAX_WRITE_CONDITION_CLAUSES);
    let extra = WriteCondition::new(
        Scope::new("guard-extra").unwrap(),
        "guards",
        "one",
        Filter::eq("active", true),
    )
    .unwrap();
    assert!(nine.and(extra).is_err());
}
