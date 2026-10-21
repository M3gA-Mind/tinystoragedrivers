use super::*;

#[test]
fn address_requires_unique_host_and_exactly_one_invocation_segment() {
    let host = BusName::new(":1.7").unwrap();
    assert!(
        StorageAddress::try_new(
            BusName::new("ai.example.Host").unwrap(),
            ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/call_1")).unwrap(),
        )
        .is_err()
    );
    assert!(
        StorageAddress::try_new(
            host.clone(),
            ObjectPath::new("/ai/tinyhumans/tinystorage/Documents").unwrap(),
        )
        .is_err()
    );
    assert!(
        StorageAddress::try_new(
            host.clone(),
            ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/call_1/nested")).unwrap(),
        )
        .is_err()
    );

    let address = StorageAddress::try_new(
        host,
        ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/call_1")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        address.port_path(Object::Documents).as_str(),
        "/ai/tinyhumans/tinystorage/Invocation/call_1/Documents"
    );
    assert_eq!(
        address.watch_path().as_str(),
        "/ai/tinyhumans/tinystorage/Invocation/call_1/Watch"
    );
}
