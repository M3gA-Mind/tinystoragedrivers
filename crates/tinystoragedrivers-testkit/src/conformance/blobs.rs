//! Blob key, content, range, and namespace checks.

use std::ops::Range;

use super::{empty, fails, ok, unique};
use bytes::Bytes;
use futures_util::stream;
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_types::{Capability, ErrorKind, TreeFile};

/// Runs blob behavior checks against one blob handle.
///
/// # Panics
/// Panics when the blob driver violates a required behavior.
pub async fn run(blobs: &dyn BlobStore) {
    let prefix = unique("blob");
    let key = format!("{prefix}/item");
    let meta = ok(
        blobs
            .put(&key, b"abcdef".to_vec(), Some("text/plain"))
            .await,
        "put blob",
    );
    assert_eq!(meta.key, key);
    assert_eq!(meta.len, 6);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    let blob = ok(blobs.get(&key).await, "get blob").expect("written blob exists");
    assert_eq!(blob.bytes, b"abcdef");
    assert_eq!(
        ok(
            blobs.get_range(&key, Range { start: 2, end: 5 }).await,
            "range"
        )
        .expect("written blob range exists"),
        b"cde"
    );
    let streamed = format!("{prefix}/streamed");
    ok(
        blobs
            .put_stream(
                &streamed,
                Box::pin(stream::iter([
                    Ok(Bytes::from_static(b"ab")),
                    Ok(Bytes::from_static(b"cd")),
                ])),
                4,
                None,
            )
            .await,
        "put exact stream",
    );
    assert_eq!(
        ok(blobs.get(&streamed).await, "get streamed blob")
            .unwrap()
            .bytes,
        b"abcd"
    );
    let empty_streamed = format!("{prefix}/empty-streamed");
    ok(
        blobs
            .put_stream(
                &empty_streamed,
                Box::pin(stream::empty::<tinystoragedrivers_types::Result<Bytes>>()),
                0,
                None,
            )
            .await,
        "put empty stream",
    );
    assert_eq!(
        ok(blobs.get(&empty_streamed).await, "get empty streamed blob")
            .unwrap()
            .bytes,
        b""
    );
    for (suffix, chunks, declared) in [
        ("short", vec![Ok(Bytes::from_static(b"abc"))], 4),
        ("long", vec![Ok(Bytes::from_static(b"abcde"))], 4),
    ] {
        let bad = format!("{prefix}/{suffix}");
        fails(
            blobs
                .put_stream(&bad, Box::pin(stream::iter(chunks)), declared, None)
                .await,
            ErrorKind::InvalidInput,
            "stream length mismatch",
        );
        assert!(ok(blobs.get(&bad).await, "failed stream is unpublished").is_none());
    }
    let failed = format!("{prefix}/error");
    fails(
        blobs
            .put_stream(
                &failed,
                Box::pin(stream::iter([
                    Ok(Bytes::from_static(b"a")),
                    Err(tinystoragedrivers_types::StorageError::backend(
                        "stream failure",
                    )),
                ])),
                2,
                None,
            )
            .await,
        ErrorKind::Backend,
        "failed input stream",
    );
    assert!(ok(blobs.get(&failed).await, "failed stream is unpublished").is_none());
    assert_eq!(
        ok(blobs.head(&key).await, "head").expect("written blob head exists"),
        meta
    );
    assert_eq!(ok(blobs.list(&prefix).await, "list").len(), 3);
    if blobs.capabilities().contains(Capability::BlobListing) {
        let first_page = ok(blobs.list_page(&prefix, None, 1).await, "first list page");
        assert_eq!(first_page.items.len(), 1);
        let cursor = first_page
            .next_after
            .expect("more than one blob needs an opaque cursor");
        let second_page = ok(
            blobs.list_page(&prefix, Some(&cursor), 10).await,
            "second list page",
        );
        assert!(!second_page.items.is_empty());
        assert!(
            !second_page
                .items
                .iter()
                .any(|item| item.meta.key == first_page.items[0].meta.key)
        );
        assert!(
            first_page
                .items
                .iter()
                .chain(second_page.items.iter())
                .any(|item| item.meta.key == key)
        );
    }
    fails(
        blobs.put("", vec![], None).await,
        ErrorKind::InvalidInput,
        "empty blob key",
    );
    assert!(ok(blobs.delete(&key).await, "delete"));
    assert!(ok(blobs.delete(&streamed).await, "delete streamed"));
    assert!(ok(
        blobs.delete(&empty_streamed).await,
        "delete empty stream"
    ));
    assert!(!ok(blobs.delete(&key).await, "delete missing"));
    assert!(ok(blobs.get(&key).await, "get after delete").is_none());
    empty(
        &ok(blobs.list(&prefix).await, "list after delete"),
        "deleted blob",
    );

    let tree = format!("{prefix}/tree");
    let written = ok(
        blobs
            .put_tree(
                &tree,
                vec![
                    TreeFile::new("nested/one", b"1".to_vec()),
                    TreeFile::new("two", b"2".to_vec()),
                ],
            )
            .await,
        "put tree",
    );
    assert_eq!(written.len(), 2);
    let children = ok(blobs.list_dir(&tree).await, "list tree directory");
    assert_eq!(children.len(), 2);
    assert_eq!(ok(blobs.delete_tree(&tree).await, "delete tree"), 2);
    fails(
        blobs.delete_tree("").await,
        ErrorKind::InvalidInput,
        "empty tree deletion",
    );
}
