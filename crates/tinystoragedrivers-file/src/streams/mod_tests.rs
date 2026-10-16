//! Stream edge cases: torn appends, interrupted truncation, long lines, and
//! damaged files.

use std::path::Path;

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, StorageBackend};

use super::*;
use crate::FileStorage;

fn open() -> (tempfile::TempDir, FileStorage, Arc<dyn StreamStore>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let streams = Arc::clone(storage.for_scope(&Scope::local()).unwrap().streams());
    (dir, storage, streams)
}

fn files(storage: &FileStorage, name: &str) -> (PathBuf, PathBuf) {
    let dir = storage.dir().join("scopes/local/streams");
    let stem = file_stem(name);
    (
        dir.join(format!("{stem}.jsonl")),
        dir.join(format!("{stem}.meta.json")),
    )
}

fn append_raw(path: &Path, bytes: &[u8]) {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
}

#[tokio::test]
async fn a_torn_final_line_is_ignored_then_replaced() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    append_raw(&data, br#"{"offset":1,"val"#);
    assert_eq!(streams.len("s").await.unwrap(), 1);
    assert_eq!(streams.read_window("s", 0, 10).await.unwrap().len(), 1);
    assert_eq!(streams.append("s", json!(1)).await.unwrap(), 1);
    let text = std::fs::read_to_string(&data).unwrap();
    assert_eq!(
        text,
        "{\"offset\":0,\"value\":0}\n{\"offset\":1,\"value\":1}\n"
    );
}

#[tokio::test]
async fn an_interrupted_truncation_still_hides_dropped_entries() {
    let (_dir, storage, streams) = open();
    streams
        .append_batch("s", vec![json!(0), json!(1), json!(2)])
        .await
        .unwrap();
    // Simulate a crash after raising `base` but before rewriting the file.
    let (_, meta) = files(&storage, "s");
    std::fs::write(&meta, br#"{"name":"s","base":2}"#).unwrap();
    let offsets: Vec<_> = streams
        .read_window("s", 0, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.offset)
        .collect();
    assert_eq!(offsets, [2]);
    assert_eq!(streams.len("s").await.unwrap(), 3);
    assert_eq!(streams.truncate_before("s", 1).await.unwrap(), 0);
}

#[tokio::test]
async fn a_truncated_stream_keeps_its_length_without_a_data_file() {
    let (_dir, storage, streams) = open();
    streams
        .append_batch("s", vec![json!(0), json!(1)])
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("s", 9).await.unwrap(), 2);
    let (data, _) = files(&storage, "s");
    std::fs::remove_file(&data).unwrap();
    assert_eq!(streams.len("s").await.unwrap(), 2);
    assert_eq!(streams.read_window("s", 0, 5).await.unwrap().len(), 0);
    assert_eq!(streams.append_batch("s", vec![]).await.unwrap(), 2);
    assert_eq!(streams.append("s", json!(2)).await.unwrap(), 2);
}

#[tokio::test]
async fn lines_longer_than_a_read_chunk_are_found() {
    let (_dir, _storage, streams) = open();
    let big = "x".repeat(20_000);
    streams
        .append_batch("s", vec![json!(big), json!(big)])
        .await
        .unwrap();
    assert_eq!(streams.len("s").await.unwrap(), 2);
    assert_eq!(streams.append("s", json!(big)).await.unwrap(), 2);
    let window = streams.read_window("s", 1, 1).await.unwrap();
    assert_eq!(window[0].value, json!(big));
}

#[tokio::test]
async fn a_corrupt_line_is_a_serialization_error() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    append_raw(&data, b"garbage\n");
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        streams.read_window("s", 0, 10).await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn a_meta_file_for_another_stream_is_a_collision() {
    let (_dir, storage, streams) = open();
    let name = "y".repeat(300);
    let (_, meta) = files(&storage, &name);
    std::fs::create_dir_all(meta.parent().unwrap()).unwrap();
    std::fs::write(&meta, br#"{"name":"other","base":0}"#).unwrap();
    assert_eq!(
        streams.len(&name).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.streams("").await.unwrap(),
        ["other"],
        "listing reports the name the file holds"
    );
}

#[tokio::test]
async fn reading_an_unopenable_stream_file_fails() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    std::fs::remove_file(&data).unwrap();
    // A directory in the data file's place cannot be read as a stream.
    std::fs::create_dir(&data).unwrap();
    assert_eq!(
        streams.read_window("s", 0, 5).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(format!("{streams:?}").contains("local"));
}

#[test]
fn length_prefers_the_larger_of_base_and_the_last_line() {
    let meta = StreamMeta {
        name: "s".into(),
        base: 5,
        sealed_len: None,
        sealed_ms: None,
        last_append_ms: None,
    };
    assert_eq!(length(&meta, None).unwrap(), 5);
    assert_eq!(
        length(&meta, Some(br#"{"offset":2,"value":0}"#)).unwrap(),
        5
    );
    assert_eq!(
        length(&meta, Some(br#"{"offset":7,"value":0}"#)).unwrap(),
        8
    );
}

#[tokio::test]
async fn an_exhausted_offset_space_is_an_error() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    std::fs::write(
        &data,
        format!("{{\"offset\":{},\"value\":0}}\n", u64::MAX - 1),
    )
    .unwrap();
    assert_eq!(streams.len("s").await.unwrap(), u64::MAX);
    assert_eq!(streams.append_batch("s", vec![]).await.unwrap(), u64::MAX);
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    std::fs::write(&data, format!("{{\"offset\":{},\"value\":0}}\n", u64::MAX)).unwrap();
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn a_new_stream_ignores_an_orphaned_data_file() {
    let (_dir, storage, streams) = open();
    let (data, _) = files(&storage, "s");
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    std::fs::write(&data, "{\"offset\":7,\"value\":0}\n").unwrap();
    assert_eq!(streams.append("s", json!(1)).await.unwrap(), 0);
    assert_eq!(streams.len("s").await.unwrap(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_data_file_is_refused_not_followed() {
    let (dir, storage, streams) = open();
    let (data, _) = files(&storage, "s");
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    let victim = dir.path().join("victim");
    std::fs::write(&victim, "{\"offset\":0,\"value\":\"secret\"}\n").unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    // Appending must neither write through the link nor truncate its target,
    // and a failed append leaves no stream behind.
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        std::fs::read_to_string(&victim).unwrap(),
        "{\"offset\":0,\"value\":\"secret\"}\n"
    );
    assert_eq!(streams.len("s").await.unwrap(), 0);
    assert_eq!(streams.streams("").await.unwrap().len(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn reads_and_truncation_refuse_a_symlinked_data_file() {
    let (dir, storage, streams) = open();
    streams.append("s", json!(1)).await.unwrap();
    streams.append("s", json!(2)).await.unwrap();
    let (data, _) = files(&storage, "s");
    let victim = dir.path().join("victim");
    std::fs::rename(&data, &victim).unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    for error in [
        streams.len("s").await.unwrap_err(),
        streams.read_window("s", 0, 10).await.unwrap_err(),
        streams.truncate_before("s", 1).await.unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::Backend);
    }
}

#[tokio::test]
async fn conditional_offset_survives_reopen_and_full_truncation() {
    let (dir, _, streams) = open();
    streams
        .append_batch_if_offset("s", 0, vec![json!(0), json!(1)])
        .await
        .unwrap();
    streams.truncate_before("s", 2).await.unwrap();
    let reopened = FileStorage::open(dir.path()).unwrap();
    let scope = reopened.for_scope(&Scope::local()).unwrap();
    assert_eq!(scope.streams().len("s").await.unwrap(), 2);
    assert_eq!(
        scope
            .streams()
            .append_batch_if_offset("s", 0, vec![json!(9)])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        scope
            .streams()
            .append_batch_if_offset("s", 2, vec![json!(2)])
            .await
            .unwrap(),
        2
    );
    assert_eq!(streams.len("s").await.unwrap(), 3);
}

mod sealing {
    //! Crash points of the sealing protocol: blob, then meta, then data. The
    //! steps are separate operations, so stopping after one leaves the state
    //! a crash there would.

    use crate::blobs::FileBlobs;
    use tinystoragedrivers_core::{SealOutcome, SealedLog, sealed_blob_key};

    use super::*;

    struct Fixture {
        _dir: tempfile::TempDir,
        storage: FileStorage,
        streams: FileStreams,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::open(dir.path()).unwrap();
        let streams = FileStreams::new(Arc::clone(&storage.db), Scope::local(), None);
        Fixture {
            _dir: dir,
            storage,
            streams,
        }
    }

    impl Fixture {
        async fn fill(&self, name: &str, count: u64) -> Vec<StreamEntry> {
            let values = (0..count).map(|i| json!({"i": i})).collect();
            self.streams.append_batch(name, values).await.unwrap();
            self.streams.read_window(name, 0, usize::MAX).await.unwrap()
        }

        fn data_exists(&self, name: &str) -> bool {
            files(&self.storage, name).0.exists()
        }

        // The sealed blob is reserved, so the tests reach it the way the
        // sealing code does, not through the blob port.
        fn has_blob(&self, name: &str) -> bool {
            self.blob_bytes(&sealed_blob_key(name).unwrap()).is_some()
        }

        fn blob_bytes(&self, key: &str) -> Option<Vec<u8>> {
            FileBlobs::get_locked(&self.storage.db, &Scope::local(), key).unwrap()
        }

        fn delete_blob(&self, name: &str) {
            let key = sealed_blob_key(name).unwrap();
            FileBlobs::delete_locked(&self.storage.db, &Scope::local(), &key).unwrap();
        }
    }

    #[tokio::test]
    async fn a_crash_after_the_blob_write_leaves_the_stream_readable_and_unsealed() {
        let f = fixture();
        let before = f.fill("j", 20).await;
        let written = f.streams.seal_write_blob("j").await.unwrap();
        assert!(f.has_blob("j"));

        assert_eq!(
            f.streams.read_window("j", 0, usize::MAX).await.unwrap(),
            before
        );
        assert_eq!(f.streams.len("j").await.unwrap(), 20);
        assert_eq!(f.streams.append("j", json!("more")).await.unwrap(), 20);

        let outcome = f.streams.seal("j").await.unwrap();
        assert_eq!((outcome.base, outcome.len), (0, 21));
        assert_ne!(outcome, written, "the stale blob was replaced");
        assert_eq!(
            f.streams
                .read_window("j", 0, usize::MAX)
                .await
                .unwrap()
                .len(),
            21
        );
        assert!(!f.data_exists("j"));
    }

    #[tokio::test]
    async fn a_crash_after_the_blob_write_resumes_to_the_same_outcome() {
        let f = fixture();
        let before = f.fill("j", 20).await;
        let written = f.streams.seal_write_blob("j").await.unwrap();
        assert_eq!(f.streams.seal("j").await.unwrap(), written);
        assert_eq!(
            f.streams.read_window("j", 0, usize::MAX).await.unwrap(),
            before
        );
        assert!(!f.data_exists("j"));
    }

    #[tokio::test]
    async fn a_crash_after_the_head_flip_reads_from_the_blob_and_resealing_drops_the_data() {
        let f = fixture();
        let before = f.fill("j", 20).await;
        let written = f.streams.seal_write_blob("j").await.unwrap();
        f.streams.seal_mark("j", written).await.unwrap();
        assert!(f.data_exists("j"), "the data file is still there");

        // Prove the reads do not come from the data file: replace it.
        std::fs::write(
            files(&f.storage, "j").0,
            b"{\"offset\":0,\"value\":\"stale\"}\n",
        )
        .unwrap();
        assert_eq!(
            f.streams.read_window("j", 0, usize::MAX).await.unwrap(),
            before
        );
        assert_eq!(f.streams.len("j").await.unwrap(), 20);
        assert_eq!(
            f.streams.append("j", json!(1)).await.unwrap_err().kind(),
            ErrorKind::Conflict
        );

        assert_eq!(f.streams.seal("j").await.unwrap(), written);
        assert!(!f.data_exists("j"));
        assert_eq!(
            f.streams.read_window("j", 0, usize::MAX).await.unwrap(),
            before
        );
        assert_eq!(f.streams.len("j").await.unwrap(), 20);
    }

    #[tokio::test]
    async fn marking_refuses_a_blob_the_stream_has_outgrown_or_lost() {
        let f = fixture();
        f.fill("j", 5).await;
        let written = f.streams.seal_write_blob("j").await.unwrap();
        f.streams.append("j", json!("late")).await.unwrap();
        let grown = f.streams.seal_mark("j", written).await.unwrap_err();
        assert_eq!(grown.kind(), ErrorKind::Conflict);

        let rewritten = f.streams.seal_write_blob("j").await.unwrap();
        f.streams.truncate_before("j", 2).await.unwrap();
        let cut = f.streams.seal_mark("j", rewritten).await.unwrap_err();
        assert_eq!(cut.kind(), ErrorKind::Conflict);

        let again = f.streams.seal_write_blob("j").await.unwrap();
        f.delete_blob("j");
        let lost = f.streams.seal_mark("j", again).await.unwrap_err();
        assert_eq!(lost.kind(), ErrorKind::Conflict, "no blob, no seal");
        assert_eq!(f.streams.append("j", json!(0)).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn the_sealed_blob_holds_the_entries_in_the_shared_format() {
        let f = fixture();
        let before = f.fill("a/b", 3).await;
        let outcome = f.streams.seal("a/b").await.unwrap();
        let bytes = f.blob_bytes("_tsd/sealed/a%2Fb").unwrap();
        assert_eq!(bytes.len() as u64, outcome.blob_bytes);
        assert_eq!(SealedLog::decode(&bytes).unwrap().window(0, 10), before);
        assert_eq!(outcome, SealOutcome::new(0, 3, bytes.len() as u64));
    }

    #[tokio::test]
    async fn a_sealed_stream_whose_blob_is_gone_reports_a_backend_error() {
        let f = fixture();
        f.fill("j", 3).await;
        f.streams.seal("j").await.unwrap();
        f.delete_blob("j");
        for error in [
            f.streams.read_window("j", 0, 10).await.unwrap_err(),
            f.streams.seal("j").await.unwrap_err(),
        ] {
            assert_eq!(error.kind(), ErrorKind::Backend);
        }
        assert!(f.streams.delete_stream("j").await.unwrap());
    }

    #[tokio::test]
    async fn deleting_a_stream_removes_its_blob_even_after_a_crashed_seal() {
        let f = fixture();
        f.fill("j", 3).await;
        f.streams.seal_write_blob("j").await.unwrap();
        assert!(f.streams.delete_stream("j").await.unwrap());
        assert!(!f.has_blob("j"));

        f.fill("k", 3).await;
        f.streams.seal("k").await.unwrap();
        assert!(f.streams.delete_stream("k").await.unwrap());
        assert!(!f.has_blob("k"));
        assert!(!files(&f.storage, "k").1.exists());
    }

    #[tokio::test]
    async fn a_data_file_that_disagrees_with_its_meta_is_not_sealed() {
        let f = fixture();
        f.fill("j", 3).await;
        std::fs::write(
            files(&f.storage, "j").0,
            b"{\"offset\":0,\"value\":0}\n{\"offset\":2,\"value\":2}\n",
        )
        .unwrap();
        assert_eq!(
            f.streams.seal("j").await.unwrap_err().kind(),
            ErrorKind::Backend
        );
        assert!(!f.has_blob("j"), "nothing was written");
    }

    #[tokio::test]
    async fn a_meta_file_from_before_sealing_reads_as_unsealed() {
        let f = fixture();
        f.fill("j", 2).await;
        std::fs::write(files(&f.storage, "j").1, br#"{"name":"j","base":0}"#).unwrap();
        assert_eq!(f.streams.append("j", json!(2)).await.unwrap(), 2);
        assert_eq!(f.streams.seal("j").await.unwrap().len, 3);
        let meta = std::fs::read_to_string(files(&f.storage, "j").1).unwrap();
        assert!(meta.contains("sealed_len"), "{meta}");
    }
}
