use super::*;
use tinystoragedrivers_types::Query;

#[test]
fn storage_failure_categories_roundtrip_without_backend_details() {
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::Conflict,
        ErrorKind::AlreadyExists,
        ErrorKind::InvalidInput,
        ErrorKind::Serialization,
        ErrorKind::Crypto,
        ErrorKind::Unavailable,
        ErrorKind::Backend,
    ] {
        let error = StorageError::new(kind, "private backend detail");
        let failure = failure_of(&error);
        let rebuilt = error_of(failure, None);
        assert_eq!(rebuilt.kind(), kind);
        assert!(!rebuilt.to_string().contains("private backend detail"));
    }
    assert_eq!(
        error_of(
            StorageFailure {
                kind: StorageKind::Fenced
            },
            None
        )
        .kind(),
        ErrorKind::Backend
    );
    let unsupported = StorageError::unsupported(Capability::Watch, "backend detail");
    let failure = failure_of(&unsupported);
    assert_eq!(failure.kind, StorageKind::Unsupported);
    assert_eq!(
        error_of(failure, Some(Capability::Watch)).kind(),
        ErrorKind::Unsupported(Capability::Watch)
    );
    assert_eq!(error_of(failure, None).kind(), ErrorKind::Backend);
}

#[test]
fn capability_names_roundtrip_while_host_only_and_unknown_names_are_dropped() {
    let names = capability_names(Capabilities::all());
    assert!(!names.contains(&Capability::SharedAcrossProcesses.to_string()));
    assert!(!names.contains(&Capability::CrossProcessCas.to_string()));
    assert!(names.contains(&Capability::ConditionalStreamAppend.to_string()));
    let parsed = capabilities_of(&names);
    assert!(parsed.contains(Capability::ConditionalStreamAppend));
    assert!(!parsed.contains(Capability::SharedAcrossProcesses));
    assert_eq!(
        capabilities_of(&["shared_across_processes".into(), "future_capability".into()]),
        Capabilities::none()
    );
}

#[test]
fn shared_types_serialize_through_the_explicit_conversion_boundary() {
    let query = Query::all();
    let converted: Query = via(&query).unwrap();
    assert_eq!(converted, query);
}
