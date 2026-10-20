use super::*;

#[test]
fn fault_plan_fails_the_nth_matching_call_once() {
    let plan = FaultPlan::new();
    plan.fail_nth(2, tinystoragedrivers_types::ErrorKind::Unavailable);
    assert!(plan.check("get").is_ok());
    let error = plan
        .check("put")
        .expect_err("second call is scheduled to fail");
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
    assert!(plan.check("get").is_ok());
}

#[test]
fn fault_plan_can_target_an_operation_and_reset() {
    let plan = FaultPlan::new();
    plan.fail_operation_nth("delete", 1, tinystoragedrivers_types::ErrorKind::NotFound);
    assert!(plan.check("get").is_ok());
    assert_eq!(
        plan.check("delete").expect_err("delete is targeted").kind(),
        tinystoragedrivers_types::ErrorKind::NotFound
    );
    plan.clear();
    assert!(plan.check("delete").is_ok());
}

#[test]
fn failing_shares_its_plan_across_clones() {
    let wrapper = Failing::new("store");
    let clone = wrapper.clone();
    wrapper
        .faults
        .fail_nth(1, tinystoragedrivers_types::ErrorKind::Backend);
    assert_eq!(
        clone.faults.check("read").expect_err("shared plan").kind(),
        tinystoragedrivers_types::ErrorKind::Backend
    );
}

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
