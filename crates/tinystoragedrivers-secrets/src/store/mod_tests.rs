//! File-oriented input validation.

use super::*;

#[test]
fn require_utf8_rejects_binary_without_echoing_it() {
    assert_eq!(require_utf8(b"text", "file").unwrap(), "text");
    let error = require_utf8(&[0xff, 0xfe], "file").unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert!(error.to_string().contains("file"));
}
