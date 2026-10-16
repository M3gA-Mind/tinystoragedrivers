//! Offset conversions at the edges of SQLite's integer range.

use super::*;

#[test]
fn offsets_round_trip_and_overflow_is_reported() {
    assert_eq!(to_offset(5), 5);
    assert_eq!(to_offset(-1), 0);
    assert_eq!(to_stored(7).unwrap(), 7);
    assert_eq!(
        to_stored(u64::MAX).unwrap_err().kind(),
        tinystoragedrivers_core::ErrorKind::Backend
    );
}

#[test]
fn handles_render_their_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("s.db")).unwrap();
    let streams = SqliteStreams::new(
        db,
        Arc::new(Tables::new("")),
        Scope::local(),
        None,
        Arc::new(|| 0),
    );
    assert!(format!("{streams:?}").contains("local"));
}

mod sealing {
    //! Crash points of the sealing protocol: blob, then head, then rows. The
    //! steps are separate operations, so stopping after one leaves the state
    //! a crash there would.

    use serde_json::json;
    use tinystoragedrivers_core::{BlobStore, ErrorKind, SealOutcome, SealedLog, sealed_blob_key};

    use super::*;
    use crate::blobs::SqliteBlobs;

    struct Fixture {
        _dir: tempfile::TempDir,
        db: Arc<Db>,
        tables: Arc<Tables>,
        streams: SqliteStreams,
        blobs: SqliteBlobs,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("s.db")).unwrap();
        let tables = Arc::new(Tables::new(""));
        db.run_now(|conn| tables.ensure(conn)).unwrap();
        let streams = SqliteStreams::new(
            Arc::clone(&db),
            Arc::clone(&tables),
            Scope::local(),
            None,
            Arc::new(|| 0),
        );
        let blobs = SqliteBlobs::new(
            Arc::clone(&db),
            Arc::clone(&tables),
            Scope::local(),
            Arc::new(|| 0),
            None,
        );
        Fixture {
            _dir: dir,
            db,
            tables,
            streams,
            blobs,
        }
    }

    impl Fixture {
        async fn fill(&self, name: &str, count: u64) -> Vec<StreamEntry> {
            let values = (0..count).map(|i| json!({"i": i})).collect();
            self.streams.append_batch(name, values).await.unwrap();
            self.streams.read_window(name, 0, usize::MAX).await.unwrap()
        }

        fn count(&self, table: &str) -> i64 {
            self.db
                .run_now(|conn| {
                    Ok(conn
                        .query_row(&format!("SELECT count(*) FROM {}", ident(table)), [], |r| {
                            r.get(0)
                        })
                        .unwrap())
                })
                .unwrap()
        }

        fn rows(&self) -> i64 {
            self.count(&self.tables.streams)
        }

        async fn has_blob(&self, name: &str) -> bool {
            self.blobs
                .head(&sealed_blob_key(name).unwrap())
                .await
                .unwrap()
                .is_some()
        }
    }

    #[tokio::test]
    async fn a_crash_after_the_blob_write_leaves_the_stream_readable_and_unsealed() {
        let f = fixture();
        let before = f.fill("j", 20).await;
        let written = f.streams.seal_write_blob("j").await.unwrap();
        assert!(f.has_blob("j").await);

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
        assert_eq!(f.rows(), 0);
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
        assert_eq!(f.rows(), 0);
    }

    #[tokio::test]
    async fn a_crash_after_the_head_flip_reads_from_the_blob_and_resealing_drops_the_rows() {
        let f = fixture();
        let before = f.fill("j", 20).await;
        let written = f.streams.seal_write_blob("j").await.unwrap();
        f.streams.seal_mark("j", written).await.unwrap();
        assert_eq!(f.rows(), 20, "the rows are still stored");

        // Prove the reads do not come from the rows: replace their values.
        f.db.run_now(|conn| {
            conn.execute(
                &format!("UPDATE {} SET body = '\"stale\"'", ident(&f.tables.streams)),
                [],
            )
            .unwrap();
            Ok(())
        })
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
        assert_eq!(f.rows(), 0);
        assert_eq!(
            f.streams.read_window("j", 0, usize::MAX).await.unwrap(),
            before
        );
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
        f.blobs
            .delete(&sealed_blob_key("j").unwrap())
            .await
            .unwrap();
        let lost = f.streams.seal_mark("j", again).await.unwrap_err();
        assert_eq!(lost.kind(), ErrorKind::Conflict, "no blob, no seal");
        assert_eq!(f.streams.append("j", json!(0)).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn the_sealed_blob_holds_the_entries_in_the_shared_format() {
        let f = fixture();
        let before = f.fill("a/b", 3).await;
        let outcome = f.streams.seal("a/b").await.unwrap();
        let blob = f.blobs.get("_tsd/sealed/a%2Fb").await.unwrap().unwrap();
        assert_eq!(blob.meta.len, outcome.blob_bytes);
        assert_eq!(
            SealedLog::decode(&blob.bytes).unwrap().window(0, 10),
            before
        );
        assert_eq!(outcome, SealOutcome::new(0, 3, blob.meta.len));
    }

    #[tokio::test]
    async fn a_sealed_stream_whose_blob_is_gone_reports_a_backend_error() {
        let f = fixture();
        f.fill("j", 3).await;
        f.streams.seal("j").await.unwrap();
        f.blobs
            .delete(&sealed_blob_key("j").unwrap())
            .await
            .unwrap();
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
        assert!(!f.has_blob("j").await);

        f.fill("k", 3).await;
        f.streams.seal("k").await.unwrap();
        assert!(f.streams.delete_stream("k").await.unwrap());
        assert!(!f.has_blob("k").await);
        assert_eq!(f.count(&f.tables.stream_heads), 0);
    }

    #[tokio::test]
    async fn a_stream_with_rows_that_disagree_with_its_head_is_not_sealed() {
        let f = fixture();
        f.fill("j", 3).await;
        f.db.run_now(|conn| {
            conn.execute(
                &format!("DELETE FROM {} WHERE seq = 1", ident(&f.tables.streams)),
                [],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(
            f.streams.seal("j").await.unwrap_err().kind(),
            ErrorKind::Backend
        );
        assert!(!f.has_blob("j").await, "nothing was written");
    }
}
