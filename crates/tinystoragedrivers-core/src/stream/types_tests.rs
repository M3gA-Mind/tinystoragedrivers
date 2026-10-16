//! Wire forms of the sealing types.

use serde_json::json;

use super::*;

#[test]
fn seal_policy_serializes_in_its_pinned_form() {
    let policy = SealPolicy::new(SealWhen::Idle(60_000)).retaining_ms(86_400_000);
    assert_eq!(
        serde_json::to_value(policy).unwrap(),
        json!({"when": {"idle": 60_000}, "retain_ms": 86_400_000})
    );
    assert_eq!(
        serde_json::to_value(SealPolicy::new(SealWhen::OnFinish)).unwrap(),
        json!({"when": "on_finish"})
    );
    assert_eq!(
        serde_json::to_value(SealPolicy::default()).unwrap(),
        json!({"when": "never"})
    );
}

#[test]
fn seal_policy_round_trips_and_defaults_retention_to_none() {
    for policy in [
        SealPolicy::new(SealWhen::OnFinish),
        SealPolicy::new(SealWhen::Never).retaining_ms(5),
        SealPolicy::new(SealWhen::Idle(1)).retaining_ms(0),
    ] {
        let wire = serde_json::to_string(&policy).unwrap();
        assert_eq!(serde_json::from_str::<SealPolicy>(&wire).unwrap(), policy);
    }
    let parsed: SealPolicy = serde_json::from_value(json!({"when": "never"})).unwrap();
    assert_eq!(parsed.retain_ms, None);
    assert!(serde_json::from_value::<SealPolicy>(json!({"when": "sometimes"})).is_err());
}

#[test]
fn seal_outcome_serializes_in_its_pinned_form() {
    let outcome = SealOutcome::new(3, 10, 120);
    assert_eq!(
        serde_json::to_value(outcome).unwrap(),
        json!({"base": 3, "len": 10, "blob_bytes": 120})
    );
    assert_eq!(outcome.retained(), 7);
}
