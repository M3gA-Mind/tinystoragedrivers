//! Stream value wire behavior and retained-offset semantics.
use super::*;

#[test]
fn stream_entries_and_info_round_trip() {
    let entry = StreamEntry {
        offset: 42,
        value: serde_json::json!({"event":"done"}),
    };
    assert_eq!(
        serde_json::from_str::<StreamEntry>(&serde_json::to_string(&entry).unwrap()).unwrap(),
        entry
    );
    let info = StreamInfo::new(40, 43, Some(100), Some(90));
    assert!(info.is_sealed());
    assert_eq!(info.base, 40);
    assert_eq!(info.len, 43);
    assert_eq!(
        serde_json::from_value::<StreamInfo>(serde_json::to_value(info).unwrap()).unwrap(),
        info
    );
    assert!(!StreamInfo::new(0, 0, None, None).is_sealed());
}

#[test]
fn seal_outcome_reports_retained_range() {
    let outcome = SealOutcome::new(10, 15, 32);
    assert_eq!(outcome.retained(), 5);
    assert_eq!(SealOutcome::new(15, 10, 0).retained(), 0);
}
