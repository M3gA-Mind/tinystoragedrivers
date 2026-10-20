use super::*;

#[test]
fn test_clock_is_cloneable_and_manual() {
    let clock = TestClock::new(50);
    let clone = clock.clone();
    assert_eq!(clone.advance(20), 70);
    assert_eq!(clock.now_ms(), 70);
    clock.set(4);
    assert_eq!(clone.now_ms(), 4);
}

#[test]
fn test_clock_saturates_at_the_timestamp_limit() {
    let clock = TestClock::new(u64::MAX - 1);
    assert_eq!(clock.advance(5), u64::MAX);
    assert_eq!(clock.advance(1), u64::MAX);
}
