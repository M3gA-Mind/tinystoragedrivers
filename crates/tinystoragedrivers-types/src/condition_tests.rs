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
