//! The tree view's grouping and validation rules.

use super::*;

fn meta(key: &str) -> BlobMeta {
    BlobMeta {
        key: key.to_owned(),
        len: 1,
        content_type: None,
    }
}

#[test]
fn list_prefix_is_a_directory_boundary() {
    assert_eq!(list_prefix("").unwrap(), "");
    assert_eq!(list_prefix("skills/a").unwrap(), "skills/a/");
    assert!(list_prefix("/abs").is_err());
    assert!(list_prefix("a/../b").is_err());
}

#[test]
fn children_group_one_level_and_sort_by_name() {
    let listed = vec![
        meta("s/SKILL.md"),
        meta("s/scripts/b.sh"),
        meta("s/scripts/a.sh"),
        meta("s/refs/x/y.md"),
    ];
    let entries = children("s/", listed);
    let shape: Vec<_> = entries
        .iter()
        .map(|entry| {
            (
                entry.name().to_owned(),
                matches!(entry, TreeEntry::Dir { .. }),
            )
        })
        .collect();
    assert_eq!(
        shape,
        [
            ("SKILL.md".to_owned(), false),
            ("refs".to_owned(), true),
            ("scripts".to_owned(), true),
        ]
    );
    assert!(matches!(
        &entries[2],
        TreeEntry::Dir { prefix, .. } if prefix == "s/scripts"
    ));
}

#[test]
fn a_name_that_is_blob_and_directory_yields_both() {
    let entries = children("", vec![meta("a"), meta("a/b")]);
    assert_eq!(entries.len(), 2);
    assert!(matches!(entries[0], TreeEntry::Blob { .. }));
    assert!(matches!(entries[1], TreeEntry::Dir { .. }));
}

#[test]
fn children_ignore_keys_outside_the_prefix() {
    assert!(children("s/", vec![meta("t/x")]).is_empty());
}

#[test]
fn plan_rejects_bad_duplicate_and_overlong_paths() {
    let ok = plan(
        "s",
        &[TreeFile::new("a/b.md", "x"), TreeFile::new("c", "y")],
        1024,
    )
    .unwrap();
    assert_eq!(ok, ["s/a/b.md", "s/c"]);
    assert!(plan("s", &[TreeFile::new("../x", "")], 1024).is_err());
    assert!(plan("s", &[TreeFile::new("", "")], 1024).is_err());
    assert!(plan("s", &[TreeFile::new("a", ""), TreeFile::new("a", "")], 1024).is_err());
    assert!(plan("s", &[TreeFile::new("abcdef", "")], 5).is_err());
}
