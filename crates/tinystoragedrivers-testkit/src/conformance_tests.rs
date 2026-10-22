use super::*;
use tinystoragedrivers_types::{Capabilities, StorageError};

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

#[test]
fn conformance_helpers_report_success_and_failure_cases() {
    assert_eq!(ok(Ok::<_, StorageError>(7), "value"), 7);
    fails(
        Err::<(), _>(StorageError::backend("offline")),
        ErrorKind::Backend,
        "backend failure",
    );
    empty::<u8>(&[], "empty result");
    assert!(has(
        Capabilities::none().with(Capability::Documents),
        Capability::Documents
    ));
    assert!(!has(Capabilities::none(), Capability::Documents));
    assert_ne!(unique("case"), unique("case"));
}

#[test]
fn conformance_helpers_fail_loudly_on_unexpected_results() {
    assert!(
        std::panic::catch_unwind(|| {
            ok::<()>(Err(StorageError::backend("offline")), "value");
        })
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| {
            fails(
                Ok::<_, StorageError>("unexpected"),
                ErrorKind::Backend,
                "failure",
            );
        })
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| {
            fails(
                Err::<(), _>(StorageError::invalid_input("bad")),
                ErrorKind::Backend,
                "failure kind",
            );
        })
        .is_err()
    );
    assert!(std::panic::catch_unwind(|| empty(&[1], "non-empty result")).is_err());
}
