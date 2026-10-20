use super::*;

#[test]
fn assertion_helpers_panic_when_a_driver_violates_the_expected_result() {
    let unexpected_error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ok::<()>(
            Err(tinystoragedrivers_types::StorageError::backend(
                "unexpected",
            )),
            "expected success",
        );
    }));
    assert!(unexpected_error.is_err());

    let unexpected_value = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fails(Ok(5), ErrorKind::Conflict, "expected failure");
    }));
    assert!(unexpected_value.is_err());
}
