//! Resume loss classification and payload-free watch projection.

use super::*;

#[test]
fn pipeline_scopes_document_keys_and_projects_no_values() {
    let pipeline = pipeline("scope");
    assert_eq!(
        pipeline[0],
        doc! {"$match": {"$or": [
            {"documentKey._id.s": "scope"},
            {"operationType": {"$in": ["drop", "rename", "dropDatabase", "invalidate"]}}
        ]}}
    );
    assert_eq!(
        pipeline[1],
        doc! {"$project": {"_id": 1, "operationType": 1}}
    );
}

#[test]
fn resume_loss_codes_are_distinguished_from_other_driver_errors() {
    for code in [136, 260, 280, 286, 91] {
        let command: mongodb::error::CommandError =
            serde_json::from_value(serde_json::json!({"code": code})).unwrap();
        let error: mongodb::error::Error = mongodb::error::ErrorKind::Command(command).into();
        assert_eq!(resume_lost(&error), code != 91);
    }
    assert!(!resume_lost(&mongodb::error::Error::custom("unrelated")));
}

#[test]
fn malformed_and_misbound_cursors_fail_without_disclosing_contents() {
    let invalid = WatchCursor::new(b"secret-invalid-token".to_vec()).unwrap();
    let error = decode(&invalid, [0; 32]).unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert!(!error.to_string().contains("secret"));

    let oversized = WatchCursor::new(vec![
        1;
        tinystoragedrivers_types::MAX_WATCH_CURSOR_BYTES + 1
    ])
    .unwrap_err();
    assert_eq!(
        oversized.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}
