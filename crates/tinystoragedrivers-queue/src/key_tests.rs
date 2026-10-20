use super::*;

#[test]
fn composite_ids_keep_the_v1_escaping_and_hash_encoding() {
    assert_eq!(
        of(&["~root", "%", "/", "\0"])
            .expect("escaped components")
            .as_str(),
        "%7Eroot/%25/%2F/%00"
    );
    assert_eq!(
        of(&["queue", "a/b"]).expect("escaped id").as_str(),
        "queue/a%2Fb"
    );
    assert_eq!(
        of(&["queue", &"x".repeat(600)])
            .expect("hashed id")
            .as_str(),
        "~".to_owned()
            + &data_encoding::BASE32_NOPAD
                .encode(&Sha256::digest(
                    format!("queue/{}", "x".repeat(600)).as_bytes()
                ))
                .to_ascii_lowercase()
    );
}
