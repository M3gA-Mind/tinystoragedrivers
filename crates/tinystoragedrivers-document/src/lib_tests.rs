use super::*;

#[test]
fn stream_names_are_validated_and_sealed_keys_are_safe() {
    assert!(validate_stream("events").is_ok());
    assert!(validate_stream("").is_err());
    assert!(validate_stream(&"x".repeat(513)).is_err());
    assert!(validate_stream("bad\0name").is_err());

    assert_eq!(sealed_blob_key("events").unwrap(), "_tsd/sealed/events");
    assert_eq!(sealed_blob_key("a/b").unwrap(), "_tsd/sealed/a%2Fb");
    assert_eq!(sealed_blob_key("a%b").unwrap(), "_tsd/sealed/a%25b");
    assert_eq!(sealed_blob_key("~events").unwrap(), "_tsd/sealed/%7Eevents");
    assert_eq!(sealed_blob_key("..").unwrap(), "_tsd/sealed/%2E.");
    assert!(sealed_blob_key(".").unwrap().starts_with("_tsd/sealed/"));
    let long_name = sealed_blob_key(&"%".repeat(200)).unwrap();
    assert!(long_name.starts_with("_tsd/sealed/~"));
    assert_eq!(long_name.len(), "_tsd/sealed/".len() + 53);
    assert!(sealed_blob_key(&"x".repeat(513)).is_err());
}
