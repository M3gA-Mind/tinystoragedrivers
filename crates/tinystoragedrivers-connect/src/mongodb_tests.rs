use super::validate_database;

#[test]
fn mongo_database_names_match_server_constraints() {
    for name in ["openhuman", "db-name"] {
        assert!(validate_database(name).is_ok(), "{name:?}");
    }
    assert!(validate_database(&"a".repeat(63)).is_ok());
    for name in [
        "",
        "bad/name",
        "bad\\name",
        "bad.name",
        "bad name",
        "bad\0name",
        "bad$name",
        "bad*name",
        "bad<name",
        "bad>name",
        "bad:name",
        "bad|name",
        "bad?name",
    ] {
        assert!(validate_database(name).is_err(), "{name:?}");
    }
    assert!(validate_database(&"a".repeat(64)).is_err());
}
