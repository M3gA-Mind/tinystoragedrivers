use super::*;

#[test]
fn encoded_names_round_trip_and_escape_unsafe_bytes() {
    for name in ["", "lower_1-x", "Upper", "a/b\\c", "é", "a.b"] {
        assert_eq!(decode(&encode(name)).as_deref(), Some(name));
    }
    assert_eq!(encode("A/B"), "%41%2F%42");
    assert_eq!(decode("%"), None);
    assert_eq!(decode("%A"), None);
    assert_eq!(decode("%GG"), None);
    assert_eq!(decode("bad.name"), None);
    assert_eq!(decode("%FF"), None);
}

#[test]
fn windows_device_names_are_escaped_case_safely() {
    for name in ["con", "prn", "aux", "nul", "com0", "com9", "lpt1", "lpt9"] {
        let encoded = encode(name);
        assert!(encoded.starts_with('%'));
        assert_eq!(decode(&encoded).as_deref(), Some(name));
    }
    for name in ["com", "comx", "lpt10", "conx"] {
        assert_eq!(encode(name), name);
    }
}

#[test]
fn long_names_hash_to_bounded_stems_and_directories() {
    let plain = "a".repeat(MAX_PLAIN);
    assert_eq!(file_stem(&plain), plain);
    let long = "a".repeat(MAX_PLAIN + 1);
    let stem = file_stem(&long);
    assert_eq!(stem.len(), 33);
    assert!(stem.starts_with('~'));
    assert_eq!(stem, file_stem(&long));
    assert_ne!(stem, file_stem(&format!("{long}b")));

    let components = dir_components(&long);
    let parts: Vec<_> = components
        .iter()
        .map(|part| part.to_str().unwrap())
        .collect();
    assert_eq!(parts[0], "a".repeat(MAX_PLAIN));
    assert_eq!(parts[1], "+a");
    assert_eq!(parts.len(), 2);
    assert!(dir_components("").as_os_str().is_empty());
}

#[test]
fn fnv_hashes_match_stable_vectors() {
    assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a_128(b""), 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d);
    assert_ne!(fnv1a_128(b"a"), fnv1a_128(b"b"));
}
