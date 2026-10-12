//! The Mongo aggregation seek expression uses flat predicates and literal data.

use super::*;

fn has_literal(value: &Bson, expected: &str) -> bool {
    match value {
        Bson::Document(document) => document.iter().any(|(key, value)| {
            (key == "$literal" && value.as_str() == Some(expected)) || has_literal(value, expected)
        }),
        Bson::Array(values) => values.iter().any(|value| has_literal(value, expected)),
        _ => false,
    }
}

#[test]
fn seek_expression_flattens_ties_and_treats_cursor_values_as_literals() {
    let position = CursorPosition {
        id: "$$ROOT".into(),
        values: vec![serde_json::json!("$field"), serde_json::json!(3)],
    };
    let expression =
        MongoDocuments::seek_expression(&[Sort::asc("first"), Sort::desc("second")], &position)
            .unwrap();
    let Bson::Document(root) = expression else {
        panic!("seek expression must be a document");
    };
    let Bson::Array(alternatives) = root.get("$or").unwrap() else {
        panic!("seek expression must be a disjunction");
    };
    let Bson::Document(tie) = alternatives.last().unwrap() else {
        panic!("tie alternative must be a document");
    };
    let Bson::Array(tie_terms) = tie.get("$and").unwrap() else {
        panic!("tie alternative must use conjunction terms");
    };
    assert_eq!(tie_terms.len(), 5);
    assert!(tie_terms.iter().all(|term| term.as_document().is_some()));
    let encoded = Bson::Document(root);
    assert!(has_literal(&encoded, "$field"));
    assert!(has_literal(&encoded, "$$ROOT"));
}

#[test]
fn deleted_complex_cursor_positions_keep_the_reference_sort_path() {
    for value in [
        serde_json::json!([1]),
        serde_json::json!({"key": 1}),
        serde_json::json!(u64::MAX),
    ] {
        assert!(!cursor_is_scalar_bson_position(&CursorPosition {
            id: "last".into(),
            values: vec![value],
        }));
    }
    assert!(cursor_is_scalar_bson_position(&CursorPosition {
        id: "last".into(),
        values: vec![serde_json::json!("scalar")],
    }));
}
