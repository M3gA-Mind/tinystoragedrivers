//! Wire compatibility and authority-selector rejection tests.
use super::*;
use serde_json::json;

#[test]
fn query_roundtrips_and_rejects_authority_selectors() {
    let query = Query {
        filter: Filter::Eq {
            field: "state".into(),
            value: json!("ready"),
        },
        sort: vec![Sort {
            field: "created_at".into(),
            direction: Direction::Desc,
        }],
        limit: Some(25),
        cursor: Some("opaque-cursor".into()),
    };
    let encoded = serde_json::to_value(&query).unwrap();
    assert_eq!(
        serde_json::from_value::<Query>(encoded.clone()).unwrap(),
        query
    );
    let mut forged = encoded;
    forged["scope"] = json!("bob");
    assert!(serde_json::from_value::<Query>(forged).is_err());
    assert_eq!(DOCUMENTS_SELECTORS.len(), 16);
    assert!(!DOCUMENTS_SELECTORS.contains(&"Arbitrary"));
    assert!(is_documents_selector("Query"));
    assert!(!is_documents_selector("Arbitrary"));
    assert_eq!(STORAGE_INTERFACE, "ai.tinyhumans.tinystorage.Storage");
    assert_eq!(DOCUMENTS_OBJECT, "Documents");
}

#[test]
fn document_requests_route_only_within_an_explicit_host_grant() {
    let get = GetDocumentRequest {
        collection: "notes".into(),
        id: "n1".into(),
    };
    let wire = serde_json::to_value(&get).unwrap();
    assert_eq!(wire["collection"], "notes");
    assert_eq!(wire["document_id"], "n1");
    assert!(
        serde_json::from_value::<GetDocumentRequest>(json!({
            "collection":"notes", "document_id":"n1", "scope":"bob"
        }))
        .is_err()
    );

    let watch = WatchNotice::Invalidated {
        cursor: Some(vec![0, 1, 255]),
    };
    let wire = serde_json::to_value(&watch).unwrap();
    assert_eq!(wire, json!({"kind":"invalidated", "cursor":[0, 1, 255]}));
    assert!(wire.get("document").is_none());
}

#[allow(clippy::needless_pass_by_value)]
fn roundtrip<T>(value: T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_value(&value).unwrap();
    assert_eq!(serde_json::from_value::<T>(encoded).unwrap(), value);
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_documents_message_shape_roundtrips() {
    let spec = CollectionSpec {
        name: "notes".into(),
        indexes: vec![IndexSpec {
            name: "by_state".into(),
            fields: vec!["state".into()],
            unique: false,
        }],
        ttl_field: None,
        search: Some(SearchSpec {
            fields: vec!["title".into()],
        }),
    };
    let query = Query::default();
    for filter in [
        Filter::All,
        Filter::Ne {
            field: "state".into(),
            value: json!("ready"),
        },
        Filter::In {
            field: "state".into(),
            values: vec![json!("ready"), json!("done")],
        },
        Filter::Range {
            field: "n".into(),
            gt: Some(json!(1)),
            gte: None,
            lt: Some(json!(10)),
            lte: None,
        },
        Filter::Exists {
            field: "state".into(),
            exists: true,
        },
        Filter::And {
            filters: vec![Filter::All],
        },
        Filter::Or {
            filters: vec![Filter::All],
        },
        Filter::Not {
            filter: Box::new(Filter::All),
        },
    ] {
        roundtrip(filter);
    }
    roundtrip(spec.clone());
    let get = GetDocumentRequest {
        collection: "notes".into(),
        id: "n".into(),
    };
    roundtrip(get);
    let put = PutDocumentRequest {
        collection: "notes".into(),
        id: "n".into(),
        doc: json!({"nested":{"id":"business-data","array":[1,true,null]}}),
        precondition: Precondition::Absent,
    };
    let put_wire = serde_json::to_value(&put).unwrap();
    assert!(put_wire["document_json"].is_string());
    assert_eq!(
        serde_json::from_value::<PutDocumentRequest>(put_wire).unwrap(),
        put
    );
    roundtrip(DeleteDocumentRequest {
        collection: "notes".into(),
        id: "n".into(),
        precondition: Precondition::Version(Version(8)),
    });
    roundtrip(QueryRequest {
        collection: "notes".into(),
        query: query.clone(),
    });
    roundtrip(QueryResponse {
        items: vec![],
        next: Some("cursor".into()),
    });
    roundtrip(CountRequest {
        collection: "notes".into(),
        filter: Filter::default(),
    });
    roundtrip(DeleteWhereRequest {
        collection: "notes".into(),
        filter: Filter::Not {
            filter: Box::new(Filter::All),
        },
    });
    roundtrip(ClaimRequest {
        collection: "notes".into(),
        filter: Filter::All,
        sort: vec![],
        patch: json!({"claimed":true}),
    });
    roundtrip(ReadCheck {
        collection: "notes".into(),
        id: "n".into(),
        expected: ExpectedDocument::Absent,
    });
    roundtrip(BatchRequest {
        checks: vec![],
        ops: vec![
            WriteOp::Put {
                collection: "notes".into(),
                id: "n".into(),
                doc: json!({"ok":true}),
                precondition: Precondition::Absent,
            },
            WriteOp::Delete {
                collection: "notes".into(),
                id: "old".into(),
                precondition: Precondition::None,
            },
        ],
    });
    roundtrip(BatchResponse::from([
        WriteResult::Put {
            version: Version(2),
        },
        WriteResult::Delete { removed: true },
    ]));
    roundtrip(SearchRequest {
        collection: "notes".into(),
        text: "term".into(),
        limit: 3,
    });
    roundtrip(SearchHit {
        id: "n".into(),
        score: 1.0,
    });
    roundtrip(EnsureCollectionRequest { spec: spec.clone() });
    roundtrip(ValidateCollectionSpecRequest { spec });
    roundtrip(CollectionRequest {
        collection: "notes".into(),
    });
    roundtrip(CollectionsRequest {
        prefix: "user_".into(),
    });
    roundtrip(WatchOpenRequest {
        collection: "notes".into(),
        filter: Filter::All,
        after: Some(vec![2, 3]),
        interval_ms: 1000,
    });
    roundtrip(WatchCloseRequest {
        watch_id: "host-watch".into(),
    });
    roundtrip(WatchChanged {
        watch_id: "host-watch".into(),
        notice: WatchNotice::ResetRequired {
            reason: WatchReset::CursorExpired,
        },
    });
}

#[test]
fn nested_schema_and_sort_fields_reject_unknown_data() {
    assert!(serde_json::from_value::<CollectionSpec>(json!({
        "name":"notes", "indexes":[{"name":"i","fields":["state"],"unique":false,"scope":"forged"}]
    })).is_err());
    assert!(
        serde_json::from_value::<Query>(json!({
            "sort":[{"field":"created","direction":"asc","collection":"forged"}]
        }))
        .is_err()
    );
}

#[test]
fn checked_batch_roundtrips_and_rejects_authority_selectors() {
    let batch = BatchRequest {
        checks: vec![ReadCheck {
            collection: "items".into(),
            id: "item-1".into(),
            expected: ExpectedDocument::Version(Version(4)),
        }],
        ops: vec![WriteOp::Put {
            collection: "items".into(),
            id: "item-1".into(),
            doc: json!({"state":"done"}),
            precondition: Precondition::Absent,
        }],
    };
    let encoded = serde_json::to_value(&batch).unwrap();
    assert_eq!(
        serde_json::from_value::<BatchRequest>(encoded.clone()).unwrap(),
        batch
    );
    let mut forged = encoded;
    forged["scope"] = json!("bob");
    assert!(serde_json::from_value::<BatchRequest>(forged).is_err());
}
