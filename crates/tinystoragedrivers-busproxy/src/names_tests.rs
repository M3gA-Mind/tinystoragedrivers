use super::*;

#[test]
fn the_fixed_names_parse() {
    assert_eq!(host_name().as_str(), HOST_NAME);
    assert_eq!(interface().as_str(), STORAGE_INTERFACE);
    assert_eq!(member("Get").as_str(), "Get");
    assert_eq!(
        object_path(Object::Blobs).as_str(),
        "/ai/tinyhumans/tinystorage/Blobs"
    );
}

#[test]
fn callback_watch_uses_a_fixed_path_and_watch_subscribers_must_be_unique() {
    assert_eq!(
        callback_watch_path().as_str(),
        "/ai/tinyhumans/tinystorage/Documents"
    );
    assert!(watch_path(&host_name()).is_err());
    assert_eq!(
        watch_path(&BusName::new(":1.42").unwrap())
            .unwrap()
            .as_str(),
        "/ai/tinyhumans/tinystorage/Watch/_1_42"
    );
}

#[test]
#[should_panic(expected = "a name this crate fixes is valid")]
fn a_name_that_is_not_fixed_is_a_bug_not_an_error() {
    let _ = member("not a member");
}

#[test]
fn wire_values_are_the_plain_json_of_the_message() {
    assert_eq!(wire_value(&[1, 2]), serde_json::json!([1, 2]));
}
