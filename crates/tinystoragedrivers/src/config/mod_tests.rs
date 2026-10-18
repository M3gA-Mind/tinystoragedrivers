//! URL parsing, redaction and driver selection.

use super::*;
use tinystoragedrivers_core::ErrorKind;

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
    let backend = open(&StorageConfig::Memory).await.unwrap();
    assert_eq!(backend.driver(), "memory");
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
    assert_eq!(backend.driver(), "file");

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
        "mongodb://app:***@host/db?password=***"
    );
    assert_eq!(
        redact_url("mongodb://host:27017/db?password=first@second"),
        "mongodb://host:27017/db?password=***"
    );
    assert_eq!(
        redact_url("mongodb://host/db:part@name?x=1"),
        "mongodb://host/db:part@name?x=1"
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
    assert!(!ambiguous.contains("assword"), "{ambiguous}");
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
        "mongodb+srv://app:***@h/db?password=***&authMechanismProperties=***#frag"
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
    assert_eq!(backend.driver(), "sqlite");
    assert!(dir.path().join("storage.db").exists());
    let file = dir.path().join("single.db");
    let backend = open(&StorageConfig::Sqlite { path: file.clone() })
        .await
        .unwrap();
    assert_eq!(backend.driver(), "sqlite");
    assert!(file.exists());
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
fn redaction_respects_uri_component_boundaries() {
    let cases = [
        (
            "mongodb://user:credential@host:27017/db:part@name?password=first@second&x=kept#frag",
            "mongodb://user:***@host:27017/db:part@name?password=***&x=kept#frag",
        ),
        (
            "mongodb://user:prefix&password=suffix@host/db?appName=a@b&password=secret",
            "mongodb://user:***@host/db?appName=a@b&password=***",
        ),
        (
            "mongodb+srv://user:credential@host/db?authMechanismProperties=token&tlsCertificateKeyFilePassword=other#safe",
            "mongodb+srv://user:***@host/db?authMechanismProperties=***&tlsCertificateKeyFilePassword=***#safe",
        ),
        (
            "mongodb://host:27017/db?password=first?second@third&x=kept#frag",
            "mongodb://host:27017/db?password=***&x=kept#frag",
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
