//! URL parsing, redaction and driver selection.

use super::*;
use tinystoragedrivers_types::{ErrorKind, Scope};

#[test]
fn parses_every_form() {
    assert_eq!(
        StorageConfig::parse(" memory ").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::parse("memory:").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::parse("sqlite:/var/lib/app").unwrap(),
        StorageConfig::Sqlite {
            path: "/var/lib/app".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("sqlite:///tmp/x.db").unwrap(),
        StorageConfig::Sqlite {
            path: "/tmp/x.db".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("./state").unwrap(),
        StorageConfig::Sqlite {
            path: "./state".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("../up").unwrap(),
        StorageConfig::Sqlite {
            path: "../up".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("file:data").unwrap(),
        StorageConfig::File { dir: "data".into() }
    );
    assert_eq!(
        StorageConfig::parse("mongodb+srv://h.example/app?retryWrites=true").unwrap(),
        StorageConfig::MongoDb {
            uri: "mongodb+srv://h.example/app?retryWrites=true".into(),
            database: "app".into()
        }
    );
}

#[test]
fn rejects_unusable_urls() {
    for bad in [
        "",
        "   ",
        "sqlite:",
        "file:",
        "postgres://h/db",
        "relative/path",
        "mongodb://h",
        "mongodb://h/",
        "mongodb://h/?x=1",
    ] {
        let error = StorageConfig::parse(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
}

#[test]
fn uri_redaction_helpers_handle_absent_schemes_and_overlapping_spans() {
    assert!(split_uri("not a URI").is_none());
    assert_eq!(
        split_uri("mongodb://user:pass@host/db?x=1").unwrap().path,
        "db"
    );
    assert_eq!(hex_value(b'0'), Some(0));
    assert_eq!(hex_value(b'9'), Some(9));
    assert_eq!(hex_value(b'a'), Some(10));
    assert_eq!(hex_value(b'F'), Some(15));
    assert_eq!(hex_value(b'G'), None);
    assert_eq!(redact_spans("safe", Vec::new()), "safe");
    assert_eq!(redact_spans("abcdef", vec![1..4, 3..5]), "a***f");
}

#[test]
fn never_prints_a_password() {
    let config = StorageConfig::parse("mongodb://app:s3cret@h:27017/db").unwrap();
    assert_eq!(config.to_string(), "mongodb://app:***@h:27017/db");
    assert!(!format!("{config:?}").contains("s3cret"));
    let error = StorageConfig::parse("mongodb://app:s3cret@h:27017").unwrap_err();
    assert!(!error.to_string().contains("s3cret"), "{error}");
    let error = StorageConfig::parse("redis://app:s3cret@h/0").unwrap_err();
    assert!(!error.to_string().contains("s3cret"), "{error}");
    assert_eq!(redact_url("mongodb://user@h/db"), "mongodb://user@h/db");
    assert_eq!(redact_url("mongodb://h/db"), "mongodb://h/db");
    assert_eq!(redact_url("no-scheme"), "no-scheme");
    assert_eq!(
        redact_url("no-scheme?password=secret"),
        "no-scheme?password=***"
    );
}

#[test]
fn names_its_driver_and_renders() {
    let cases = [
        ("memory", "memory", "memory"),
        ("sqlite:/d", "sqlite", "sqlite:/d"),
        ("file:/d", "file", "file:/d"),
        ("mongodb://h/db", "mongodb", "mongodb://h/db"),
    ];
    for (url, driver, shown) in cases {
        let config = StorageConfig::parse(url).unwrap();
        assert_eq!(config.driver(), driver);
        assert_eq!(config.to_string(), shown);
    }
}

#[tokio::test]
async fn opens_memory_and_names_missing_features() {
    assert!(open(&StorageConfig::Memory).await.is_ok());
    assert_eq!(StorageConfig::Memory.driver(), "memory");
    let mut missing = Vec::new();
    if cfg!(not(feature = "sqlite")) {
        missing.push("sqlite:/tmp/x");
    }
    if cfg!(not(feature = "mongodb")) {
        missing.push("mongodb://h/db");
    }
    if cfg!(not(feature = "file")) {
        missing.push("file:/tmp/x");
    }
    for url in missing {
        let config = StorageConfig::parse(url).unwrap();
        let error = open(&config).await.map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            error
                .message()
                .contains(&format!("`{}` driver", config.driver())),
            "{error}"
        );
    }
}

#[cfg(feature = "file")]
#[tokio::test]
async fn opens_a_file_url_with_the_file_feature() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::parse(&format!("file:{}", dir.path().display())).unwrap();
    let backend = open(&config).await.unwrap();
    let ports = backend
        .for_scope(&Scope::new("file-config-test").unwrap())
        .unwrap();
    ports.kv().put("probe", serde_json::json!(1)).await.unwrap();
    assert_eq!(
        ports.kv().get("probe").await.unwrap().unwrap().doc,
        serde_json::json!(1)
    );
    assert_eq!(config.driver(), "file");

    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let blocked = StorageConfig::File { dir: file };
    let error = open(&blocked).await.map(|_| ()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn redaction_survives_unencoded_credentials_and_secret_options() {
    let slash = StorageConfig::parse("mongodb://app:p/a@ss@host/db").unwrap();
    let slash_shown = slash.to_string();
    assert!(!slash_shown.contains("p/a"), "{slash_shown}");
    assert!(!slash_shown.contains("ss"), "{slash_shown}");
    assert!(slash_shown.contains("***"), "{slash_shown}");
    assert_eq!(
        redact_url("mongodb://app:prefix&password=suffix@host/db"),
        "mongodb://app:***@host/db"
    );
    assert_eq!(
        redact_url("mongodb://app:credential@host/db?password=first@second"),
        "mongodb://app:***"
    );
    assert_eq!(
        redact_url("mongodb://host:27017/db?password=first@second"),
        "mongodb://host:***"
    );
    assert_eq!(
        redact_url("mongodb://host/db:part@name?x=1"),
        "mongodb://host/db:***@name?x=1"
    );
    let StorageConfig::MongoDb { database, .. } = &slash else {
        panic!("expected a MongoDB config")
    };
    assert_eq!(database, "db");

    let aws = StorageConfig::parse(
        "mongodb+srv://h/db?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:tok&retryWrites=true",
    )
    .unwrap();
    let shown = aws.to_string();
    assert!(!shown.contains("tok"), "{shown}");
    assert!(shown.contains("authMechanismProperties=***"), "{shown}");
    assert!(shown.contains("retryWrites=true"), "{shown}");

    assert_eq!(
        redact_url("mongodb://h/?tlsCertificateKeyFilePassword=pw&x"),
        "mongodb://h/?tlsCertificateKeyFilePassword=***&x"
    );
    assert_eq!(redact_url("mongodb://h?x=1"), "mongodb://h/?x=1");
    assert_eq!(
        redact_url("mongodb://h/db?appName=a@b&password=secret"),
        "mongodb://h/db?appName=a@b&password=***"
    );
    let ambiguous = redact_url("mongodb://app:p?assword@host/db?password=secret");
    assert!(!ambiguous.contains("p?assword"), "{ambiguous}");
    assert!(!ambiguous.contains("secret"), "{ambiguous}");
    assert!(ambiguous.contains("***"), "{ambiguous}");
    assert_eq!(
        redact_url("mongodb://h/db?password=first?second@host"),
        "mongodb://h/db?password=***"
    );
    assert_eq!(
        redact_url("mongodb://h/db?password=first?second&authMechanismProperties=token&x=1#frag"),
        "mongodb://h/db?password=***&authMechanismProperties=***&x=1#frag"
    );
    assert_eq!(
        redact_url(
            "mongodb+srv://app:p@ss@h/db?password=first?second&authMechanismProperties=token#frag"
        ),
        "mongodb+srv://app:***@h/db?password=***&authMechanismProperties=***"
    );
    let ambiguous = redact_url("mongodb://app:credential@host/db?appName=a:b@c&password=second");
    assert!(!ambiguous.contains("credential"), "{ambiguous}");
    assert!(!ambiguous.contains("second"), "{ambiguous}");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn opens_sqlite_directories_and_files() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().display());
    let backend = open(&StorageConfig::parse(&url).unwrap()).await.unwrap();
    let ports = backend
        .for_scope(&Scope::new("sqlite-directory-test").unwrap())
        .unwrap();
    ports.kv().put("probe", serde_json::json!(1)).await.unwrap();
    assert_eq!(
        ports.kv().get("probe").await.unwrap().unwrap().doc,
        serde_json::json!(1)
    );
    assert_eq!(StorageConfig::parse(&url).unwrap().driver(), "sqlite");
    assert!(dir.path().join("storage.db").exists());
    let file = dir.path().join("single.db");
    let backend = open(&StorageConfig::Sqlite { path: file.clone() })
        .await
        .unwrap();
    let ports = backend
        .for_scope(&Scope::new("sqlite-file-test").unwrap())
        .unwrap();
    ports.kv().put("probe", serde_json::json!(2)).await.unwrap();
    assert_eq!(
        ports.kv().get("probe").await.unwrap().unwrap().doc,
        serde_json::json!(2)
    );
    assert_eq!(
        StorageConfig::Sqlite { path: file.clone() }.driver(),
        "sqlite"
    );
    assert!(file.exists());
}

#[tokio::test]
async fn memory_reopening_a_scope_preserves_values_without_cross_scope_reads() {
    use serde_json::json;
    use tinystoragedrivers_types::{Precondition, Scope};

    let backend = open(&StorageConfig::Memory).await.unwrap();
    let local = Scope::new("local").unwrap();
    let other = Scope::new("other").unwrap();
    let first = backend.for_scope(&local).unwrap();
    first.kv().put("answer", json!(42)).await.unwrap();
    first
        .documents()
        .put(
            "notes",
            "one",
            json!({"body": "private"}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    first
        .blobs()
        .put("image", b"local".to_vec(), None)
        .await
        .unwrap();
    first.secrets().set("token", b"private").await.unwrap();

    let reopened = backend.for_scope(&local).unwrap();
    assert_eq!(
        reopened.kv().get("answer").await.unwrap().unwrap().doc,
        json!(42)
    );
    assert!(
        reopened
            .documents()
            .get("notes", "one")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        reopened.blobs().get("image").await.unwrap().unwrap().bytes,
        b"local"
    );
    assert_eq!(
        reopened
            .secrets()
            .get("token")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"private"
    );

    let isolated = backend.for_scope(&other).unwrap();
    assert!(isolated.kv().get("answer").await.unwrap().is_none());
    assert!(
        isolated
            .documents()
            .get("notes", "one")
            .await
            .unwrap()
            .is_none()
    );
    assert!(isolated.blobs().get("image").await.unwrap().is_none());
    assert!(isolated.secrets().get("token").await.unwrap().is_none());
}

#[test]
fn a_question_mark_in_a_password_stays_hidden() {
    let shown = redact_url("mongodb://app:p?assword@host/db");
    assert!(!shown.contains("assword"), "{shown}");
    assert!(shown.contains("***"), "{shown}");
    let error = StorageConfig::parse("mongodb://app:p?assword@host").unwrap_err();
    assert!(!error.to_string().contains("assword"), "{error}");
    let query_at = StorageConfig::parse("mongodb://h/db?appName=a@b").unwrap();
    let StorageConfig::MongoDb { database, .. } = &query_at else {
        panic!("expected a MongoDB config")
    };
    assert_eq!(database, "db", "an @ in a query value is not userinfo");
}

#[test]
fn redaction_merges_original_credential_spans_across_uri_components() {
    let cases = [
        (
            "mongodb://user:credential@host:27017/db:part@name?password=first@second&x=kept#frag",
            "mongodb://user:***&x=kept#frag",
        ),
        (
            "mongodb://user:prefix&password=suffix@host/db?appName=a@b&password=secret",
            "mongodb://user:***@b&password=***",
        ),
        (
            "mongodb+srv://user:credential@host/db?authMechanismProperties=token&tlsCertificateKeyFilePassword=other#safe",
            "mongodb+srv://user:***@host/db?authMechanismProperties=***&tlsCertificateKeyFilePassword=***",
        ),
        (
            "mongodb://host:27017/db?password=first?second@third&x=kept#frag",
            "mongodb://host:***&x=kept#frag",
        ),
    ];

    for (input, expected) in cases {
        let output = redact_url(input);
        assert_eq!(output, expected, "input: {input}");
        for sentinel in [
            "credential",
            "suffix",
            "secret",
            "token",
            "other",
            "first",
            "second",
        ] {
            assert!(
                !output.contains(sentinel),
                "{input} leaked {sentinel}: {output}"
            );
        }
    }
}

#[test]
fn redaction_keeps_ambiguous_reserved_delimiters_inside_credentials_hidden() {
    for input in [
        "mongodb://app:123?secret@host/db",
        "mongodb://app:123/secret@host/db",
        "mongodb://app:123#secret@host/db",
        "mongodb://app:314159@middle/secretRight@host/db",
        "mongodb://app:314159@middle?secretRight@host/db",
        "mongodb://app:314159@middle#secretRight@host/db",
    ] {
        assert_eq!(redact_url(input), "mongodb://app:***@host/db", "{input}");
    }
}

#[test]
fn redaction_hides_multiple_at_passwords_across_reserved_delimiters() {
    for delimiter in ["/", "?", "#"] {
        let input = format!(
            "mongodb://app:314159@middle{delimiter}secretRight@host/db?password=querySecret@other&x=kept"
        );
        let output = redact_url(&input);
        assert!(!output.contains("314159"), "{input}: {output}");
        assert!(!output.contains("secretRight"), "{input}: {output}");
        assert!(!output.contains("querySecret"), "{input}: {output}");
        assert!(output.contains("***"), "{input}: {output}");
    }
}

#[test]
fn redaction_keeps_secret_query_spans_through_fragment_markers() {
    let cases = [
        (
            "mongodb://h/db?password=first#secretRight",
            "mongodb://h/db?password=***",
        ),
        (
            "mongodb://h/db#fragment?password=secretRight",
            "mongodb://h/db#fragment?password=***",
        ),
    ];
    for (input, expected) in cases {
        let output = redact_url(input);
        assert_eq!(output, expected, "{input}");
        assert!(!output.contains("secretRight"), "{input}: {output}");
    }
}

#[test]
fn redaction_matches_percent_encoded_secret_option_names() {
    let input = "mongodb://h/db?tlsCertificateKeyFilePass%77ord=querySecret";
    let output = redact_url(input);
    assert_eq!(output, "mongodb://h/db?tlsCertificateKeyFilePass%77ord=***");
    assert!(!output.contains("querySecret"));
}

#[test]
fn proxy_password_is_redacted_from_every_public_url_representation() {
    for key in ["proxyPassword", "PROXYPASSWORD", "proxy%50assword"] {
        let input = format!("mongodb://h/db?{key}=proxySecretValue");
        let output = redact_url(&input);
        assert_eq!(output, format!("mongodb://h/db?{key}=***"));
        assert!(!output.contains("proxySecretValue"), "{output}");

        let config = StorageConfig::parse(&input).unwrap();
        assert!(!config.to_string().contains("proxySecretValue"));
        assert!(!format!("{config:?}").contains("proxySecretValue"));

        let missing_database = format!("mongodb://h/?{key}=proxySecretValue");
        let error = StorageConfig::parse(&missing_database).unwrap_err();
        assert!(!error.to_string().contains("proxySecretValue"), "{error}");
        assert!(
            !format!("{error:?}").contains("proxySecretValue"),
            "{error:?}"
        );
    }
}

#[test]
fn secret_query_scanning_has_linear_work_for_dense_query_markers() {
    let prefix = "mongodb://h/db";
    let input = format!("{prefix}{}password=linearSecret", "?".repeat(4_096));
    let mut scanned_bytes = 0;
    let spans = scan_secret_query_value_spans(&input, || scanned_bytes += 1);
    let value_start = input.len() - "linearSecret".len();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0], value_start..input.len());
    assert!(
        scanned_bytes <= input.len() * 512,
        "visited {scanned_bytes} input bytes for {} URI bytes",
        input.len()
    );
}

#[test]
fn redaction_does_not_let_secret_query_scrubbing_expose_ambiguous_userinfo() {
    for input in [
        "mongodb://app:123?password=suffix@host/db",
        "mongodb://app:prefix?password=suffix@host/db",
    ] {
        let output = redact_url(input);
        assert!(output.starts_with("mongodb://app:***"), "{input}: {output}");
        assert!(!output.contains("123"), "{output}");
        assert!(!output.contains("prefix"), "{output}");
        assert!(!output.contains("suffix"), "{output}");
    }
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn opens_a_mongodb_url_with_the_driver() {
    // A port that is not a number fails the driver's own URI parse before
    // any I/O, so this proves the URL reaches the driver without a network.
    let config = StorageConfig::parse("mongodb://app:s3cret@h:notaport/db").unwrap();
    let error = open(&config).await.map(|_| ()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        !error.message().contains("not available in this build"),
        "the driver, not the missing-feature arm, answered: {error}"
    );
    assert!(!error.to_string().contains("s3cret"), "{error}");
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn facade_opens_a_live_mongodb_backend_and_binds_scoped_ports() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let database = format!(
        "tsd_facade_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let config = StorageConfig::MongoDb {
        uri: uri.clone(),
        database: database.clone(),
    };
    let backend = open(&config).await.unwrap();
    let scope = Scope::new(&format!("facade-{}", std::process::id())).unwrap();
    let ports = backend.for_scope(&scope).unwrap();
    ports
        .kv()
        .put("facade-probe", serde_json::json!({"value": 7}))
        .await
        .unwrap();
    assert_eq!(
        ports.kv().get("facade-probe").await.unwrap().unwrap().doc,
        serde_json::json!({"value": 7})
    );
    let connection = tinystoragedrivers_connect::MongoConnection::connect(&uri, &database)
        .await
        .unwrap();
    connection.database().drop().await.unwrap();
}
