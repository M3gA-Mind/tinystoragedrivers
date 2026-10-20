use super::*;

#[test]
fn query_cursor_binding_includes_sort_order() {
    let scope = Scope::new("scope-a").unwrap();
    let ascending = Query::all().sort(Sort::asc("name"));
    let descending = Query::all().sort(Sort::desc("name"));
    assert_ne!(
        fingerprint("items", &scope, &ascending),
        fingerprint("items", &scope, &descending)
    );
}

#[test]
fn mongo_json_conversion_preserves_json_values_and_rejects_unrepresentable_values() {
    let value = serde_json::json!({
        "null": null,
        "bool": true,
        "string": "text",
        "integer": -7,
        "float": 1.25,
        "array": [false, "nested"],
        "object": {"answer": 42}
    });
    let bson = json_to_bson(&value).unwrap();
    assert_eq!(bson_to_json(&bson).unwrap(), value);
    assert!(json_to_bson(&serde_json::json!(u64::MAX)).is_err());
    assert!(bson_to_json(&Bson::Double(f64::NAN)).is_err());
    assert!(bson_to_json(&Bson::ObjectId(mongodb::bson::oid::ObjectId::new())).is_err());
}

#[test]
fn mongo_decode_rejects_malformed_records_and_treats_tombstones_as_absent() {
    assert_eq!(decode(&doc! {DELETED: true}).unwrap(), None);
    assert!(decode(&Document::new()).is_err());
    assert!(decode(&doc! {KEY: "id", VERSION: 0_i64, BODY: {}}).is_err());
    assert!(decode(&doc! {KEY: "id", VERSION: 1_i64}).is_err());

    let row = decode(&doc! {KEY: "id", VERSION: 1_i64, BODY: {"value": 1}})
        .unwrap()
        .unwrap();
    assert_eq!(row.meta.schema, 0);
    assert_eq!(row.meta.created_ms, 0);
    assert_eq!(row.meta.updated_ms, 0);
    assert_eq!(row.meta.expires_ms, None);
}

#[test]
fn mongo_index_builders_validate_paths_and_skip_unsupported_nonunique_indexes() {
    use tinystoragedrivers_types::{IndexSpec, SearchSpec};

    let unsupported = CollectionSpec::new("items").index(IndexSpec {
        name: "bad".into(),
        fields: vec!["$invalid".into()],
        unique: false,
    });
    assert!(spec_indexes(&unsupported).unwrap().is_empty());

    let invalid_unique = CollectionSpec::new("items").index(IndexSpec {
        name: "bad".into(),
        fields: vec!["nested.$invalid".into()],
        unique: true,
    });
    assert!(spec_indexes(&invalid_unique).is_err());

    let valid_unique = CollectionSpec::new("items").index(IndexSpec {
        name: "email".into(),
        fields: vec!["email".into()],
        unique: true,
    });
    let indexes = spec_indexes(&valid_unique).unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].options.as_ref().unwrap().unique, Some(true));

    assert!(
        text_index(&SearchSpec { fields: vec![] })
            .unwrap()
            .is_none()
    );
    assert!(
        text_index(&SearchSpec {
            fields: vec!["$bad".into()]
        })
        .is_err()
    );
    assert!(
        text_index(&SearchSpec {
            fields: vec!["nested.$bad".into()]
        })
        .is_err()
    );
    assert!(
        text_index(&SearchSpec {
            fields: vec!["body".into()]
        })
        .unwrap()
        .is_some()
    );
}
