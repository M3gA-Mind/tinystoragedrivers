use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn server(status: &str, body: Value) -> (String, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = body.to_string();
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut input = Vec::new();
        loop {
            let mut chunk = [0; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            input.extend_from_slice(&chunk[..n]);
            let raw = String::from_utf8_lossy(&input);
            if let Some((head, tail)) = raw.split_once("\r\n\r\n") {
                let length: usize = head
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(str::to_owned)
                    })
                    .unwrap()
                    .parse()
                    .unwrap();
                if tail.len() >= length {
                    break;
                }
            }
        }
        socket.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(input).unwrap()
    });
    (format!("http://{addr}"), task)
}
#[cfg(feature = "kms-gcp")]
#[derive(Debug)]
struct Tokens;
#[cfg(feature = "kms-gcp")]
#[async_trait]
impl GcpAccessTokenProvider for Tokens {
    async fn access_token(&self) -> Result<Zeroizing<String>> {
        Ok(Zeroizing::new("fake-workload-token".into()))
    }
}
#[cfg(feature = "kms-gcp")]
#[tokio::test]
async fn google_transport_sends_scope_aad_and_redacts_rejection_bodies() {
    let (endpoint, request) =
        server("200 OK", json!({"ciphertext": STANDARD.encode(b"wrapped")})).await;
    let mut kms = GcpKms::new(Arc::new(Tokens)).unwrap();
    kms.endpoint = Some(endpoint);
    let scope = Scope::new("tenant-a").unwrap();
    let id = "projects/project/locations/global/keyRings/ring/cryptoKeys/key";
    assert_eq!(kms.wrap(id, &scope, &[9; 32]).await.unwrap(), b"wrapped");
    let raw = request.await.unwrap();
    assert!(raw.starts_with(&format!("POST /v1/{id}:encrypt HTTP/1.1")));
    assert!(raw.contains("authorization: Bearer fake-workload-token"));
    let payload: Value = serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        STANDARD
            .decode(payload["additionalAuthenticatedData"].as_str().unwrap())
            .unwrap(),
        b"tinystoragedrivers-secrets/v1:tenant-a"
    );
    let (endpoint, request) =
        server("403 Forbidden", json!({"error": "sensitive-plaintext"})).await;
    kms.endpoint = Some(endpoint);
    let error = kms.wrap(id, &scope, &[9; 32]).await.unwrap_err();
    assert!(!error.to_string().contains("sensitive-plaintext"));
    request.await.unwrap();
}
#[cfg(feature = "kms-aws")]
#[derive(Debug)]
struct Credentials;
#[cfg(feature = "kms-aws")]
#[async_trait]
impl AwsCredentialsProvider for Credentials {
    async fn credentials(&self) -> Result<AwsCredentials> {
        Ok(AwsCredentials {
            access_key_id: Zeroizing::new("fake-access-key".into()),
            secret_access_key: Zeroizing::new("fake-secret-key".into()),
            session_token: Some(Zeroizing::new("fake-sts-token".into())),
        })
    }
}
#[cfg(feature = "kms-aws")]
#[tokio::test]
async fn aws_transport_signs_and_binds_scope_and_key_identity() {
    let (endpoint, request) = server(
        "200 OK",
        json!({"CiphertextBlob": STANDARD.encode(b"wrapped")}),
    )
    .await;
    let mut kms = AwsKms::new("us-east-1", Arc::new(Credentials)).unwrap();
    kms.endpoint = Some(endpoint);
    let scope = Scope::new("tenant-a").unwrap();
    assert_eq!(
        kms.wrap("key-id", &scope, &[9; 32]).await.unwrap(),
        b"wrapped"
    );
    let raw = request.await.unwrap();
    assert!(raw.contains("x-amz-target: TrentService.Encrypt"));
    assert!(raw.contains("x-amz-security-token: fake-sts-token"));
    assert!(
        raw.contains(
            "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token;x-amz-target"
        )
    );
    assert!(raw.contains("/us-east-1/kms/aws4_request"));
    assert!(!raw.contains("fake-secret-key"));
    let payload: Value = serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(payload["KeyId"], "key-id");
    assert_eq!(payload["EncryptionContext"]["scope"], "tenant-a");
    assert_eq!(payload["EncryptionContext"]["format"], LABEL);
    assert_eq!(
        format!("{:?}", Credentials.credentials().await.unwrap()),
        "AwsCredentials(..)"
    );
}
#[cfg(feature = "kms-aws")]
#[test]
fn signing_hmac_matches_rfc_4231_vector() {
    assert_eq!(
        hex(&mac(&[0x0b; 20], b"Hi There").unwrap()[..]),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[cfg(feature = "kms-aws")]
#[test]
fn aws_published_iam_signing_example_matches_canonical_hash_and_signature() {
    // AWS IAM SigV4 example: GET /?Action=ListUsers&Version=2010-05-08,
    // https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    use sha2_11::{Digest, Sha256};
    let headers = std::collections::BTreeMap::from([
        (
            "content-type",
            "application/x-www-form-urlencoded; charset=utf-8".into(),
        ),
        ("host", "iam.amazonaws.com".into()),
        ("x-amz-date", "20150830T123600Z".into()),
    ]);
    let (canonical, signed) = canonical_request(
        "GET",
        "/",
        "Action=ListUsers&Version=2010-05-08",
        &headers,
        b"",
    );
    assert_eq!(signed, "content-type;host;x-amz-date");
    assert_eq!(
        hex(&Sha256::digest(canonical.as_bytes())),
        "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    assert_eq!(
        signature(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
            &to_sign
        )
        .unwrap(),
        "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
    );
}

#[cfg(feature = "kms-aws")]
#[test]
fn mutable_aws_aliases_cannot_be_persisted_as_envelope_identities() {
    assert!(validate_aws_key_id("alias/cloud-secrets").is_err());
    assert!(validate_aws_key_id("arn:aws:kms:us-east-1:123456789012:alias/cloud-secrets").is_err());
    assert!(validate_aws_key_id("arn:aws:kms:us-east-1:123456789012:key/immutable-id").is_ok());
}

#[cfg(feature = "kms-gcp")]
#[tokio::test]
async fn google_unwrap_validates_plaintext_length_and_resource_identity() {
    let scope = Scope::new("tenant").unwrap();
    let id = "projects/project/locations/global/keyRings/ring/cryptoKeys/key";
    let mut kms = GcpKms::new(Arc::new(Tokens)).unwrap();
    for (plaintext, succeeds) in [(vec![7; 32], true), (vec![7; 31], false)] {
        let (endpoint, request) =
            server("200 OK", json!({"plaintext": STANDARD.encode(&plaintext)})).await;
        kms.endpoint = Some(endpoint);
        let result = kms.unwrap(id, &scope, b"wrapped").await;
        if succeeds {
            assert_eq!(*result.unwrap(), [7; 32]);
        } else {
            assert_eq!(
                result.unwrap_err().kind(),
                tinystoragedrivers_core::ErrorKind::Crypto
            );
        }
        let raw = request.await.unwrap();
        assert!(raw.starts_with(&format!("POST /v1/{id}:decrypt HTTP/1.1")));
    }
    assert_eq!(
        kms.wrap("projects/../injected", &scope, &[7; 32])
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::InvalidInput
    );
    assert_eq!(format!("{kms:?}"), "GcpKms(..)");
}

#[cfg(feature = "kms-aws")]
#[tokio::test]
async fn aws_unwrap_sends_decrypt_with_authenticated_scope() {
    let (endpoint, request) =
        server("200 OK", json!({"Plaintext": STANDARD.encode([6; 32])})).await;
    let mut kms = AwsKms::new("us-east-1", Arc::new(Credentials)).unwrap();
    kms.endpoint = Some(endpoint);
    let scope = Scope::new("tenant").unwrap();
    assert_eq!(
        *kms.unwrap("key-id", &scope, b"wrapped").await.unwrap(),
        [6; 32]
    );
    let raw = request.await.unwrap();
    assert!(raw.contains("x-amz-target: TrentService.Decrypt"));
    let payload: Value = serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(payload["EncryptionContext"]["scope"], "tenant");
    assert_eq!(payload["CiphertextBlob"], STANDARD.encode(b"wrapped"));
    assert_eq!(format!("{kms:?}"), "AwsKms(..)");
    assert!(AwsKms::new("invalid/region", Arc::new(Credentials)).is_err());
}

#[test]
fn sensitive_payload_encoding_preserves_nested_shape() {
    let expected = json!({"items": ["private-material", 7, null, true]});
    let payload = SensitiveJson(expected.clone());
    let encoded = payload.encode().unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), expected);
}

#[tokio::test]
async fn deterministic_kms_rejections_are_not_retryable_and_transient_statuses_are() {
    use tinystoragedrivers_core::ErrorKind;
    for (status, body, expected) in [
        (
            "400 Bad Request",
            json!({"__type":"InvalidCiphertextException", "message":"private"}),
            ErrorKind::Crypto,
        ),
        (
            "400 Bad Request",
            json!({"error":{"status":"INVALID_ARGUMENT"}}),
            ErrorKind::InvalidInput,
        ),
        (
            "404 Not Found",
            json!({"error":{"status":"NOT_FOUND"}}),
            ErrorKind::NotFound,
        ),
        (
            "403 Forbidden",
            json!({"error":{"status":"PERMISSION_DENIED"}}),
            ErrorKind::Backend,
        ),
        ("429 Too Many Requests", json!({}), ErrorKind::Unavailable),
        ("503 Service Unavailable", json!({}), ErrorKind::Unavailable),
    ] {
        let (endpoint, request) = server(status, body).await;
        let response = reqwest::Client::new()
            .post(endpoint)
            .body("{}")
            .send()
            .await
            .unwrap();
        let error = super::response(response).await.err().unwrap();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.is_retryable(), expected == ErrorKind::Unavailable);
        assert!(!error.to_string().contains("private"));
        request.await.unwrap();
    }
}
