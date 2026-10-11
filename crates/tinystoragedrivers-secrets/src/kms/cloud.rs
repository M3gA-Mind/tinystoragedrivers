//! Feature-gated REST KMS clients; credentials are supplied afresh per request.
use super::{KmsClient, Result, Scope, StorageError, Zeroizing};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{fmt, sync::Arc};
use zeroize::Zeroize;

// JSON request/response strings containing base64 plaintext are wiped on drop.
// Bytes keeps the zeroizing request owner alive until reqwest releases its body.
// HTTP/TLS internals still own transient transport copies; these cannot be
// explicitly wiped through reqwest's public API. Nothing logs these payloads.
struct SensitiveJson(Value);
impl SensitiveJson {
    fn encode(self) -> Result<Zeroizing<Vec<u8>>> {
        let mut bytes = Zeroizing::new(Vec::new());
        serde_json::to_writer(&mut *bytes, &self.0)
            .map_err(|_| StorageError::serialization("KMS payload encoding failed"))?;
        Ok(bytes)
    }
}
impl Drop for SensitiveJson {
    fn drop(&mut self) {
        fn wipe(value: &mut Value) {
            match value {
                Value::String(text) => text.zeroize(),
                Value::Array(items) => items.iter_mut().for_each(wipe),
                Value::Object(map) => map.values_mut().for_each(wipe),
                _ => {}
            }
        }
        wipe(&mut self.0);
    }
}

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_| StorageError::unavailable("KMS HTTP client initialization failed"))
}

const LABEL: &str = "tinystoragedrivers-secrets/v1";

async fn response(response: reqwest::Response) -> Result<SensitiveJson> {
    // Never echo cloud response bodies: an upstream could include plaintext.
    if !response.status().is_success() {
        return Err(StorageError::unavailable("KMS request rejected"));
    }
    response
        .json()
        .await
        .map(SensitiveJson)
        .map_err(|_| StorageError::crypto("invalid KMS response"))
}
fn field(value: &SensitiveJson, name: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(
            value
                .0
                .get(name)
                .and_then(Value::as_str)
                .ok_or_else(|| StorageError::crypto("KMS response missing key material"))?,
        )
        .map_err(|_| StorageError::crypto("KMS response has invalid base64"))
}
fn key(value: Vec<u8>) -> Result<Zeroizing<[u8; 32]>> {
    let value = Zeroizing::new(value);
    let bytes = value
        .as_slice()
        .try_into()
        .map_err(|_| StorageError::crypto("KMS returned wrong key length"))?;
    Ok(Zeroizing::new(bytes))
}

/// Refreshed Google OAuth access tokens (for example workload identity).
#[cfg(feature = "kms-gcp")]
#[async_trait]
pub trait GcpAccessTokenProvider: Send + Sync + fmt::Debug {
    /// Obtain a token authorized for Cloud KMS.
    ///
    /// # Errors
    /// Credential acquisition failed.
    async fn access_token(&self) -> Result<Zeroizing<String>>;
}
/// Google Cloud KMS symmetric encryption REST client with scope AAD.
#[cfg(feature = "kms-gcp")]
pub struct GcpKms {
    http: reqwest::Client,
    tokens: Arc<dyn GcpAccessTokenProvider>,
    #[cfg(test)]
    endpoint: Option<String>,
}
#[cfg(feature = "kms-gcp")]
impl fmt::Debug for GcpKms {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GcpKms(..)")
    }
}
#[cfg(feature = "kms-gcp")]
impl GcpKms {
    /// Construct a TLS client using a refreshable workload token source.
    ///
    /// # Errors
    /// TLS client initialization failed.
    pub fn new(tokens: Arc<dyn GcpAccessTokenProvider>) -> Result<Self> {
        Ok(Self {
            http: http_client()?,
            tokens,
            #[cfg(test)]
            endpoint: None,
        })
    }
    async fn request(&self, id: &str, op: &str, body: SensitiveJson) -> Result<SensitiveJson> {
        // A resource id cannot change the endpoint or inject a query/fragment.
        if !id.starts_with("projects/")
            || id.split('/').any(|part| {
                part.is_empty()
                    || !part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            })
        {
            return Err(StorageError::invalid_input("invalid GCP KMS resource id"));
        }
        let token = self.tokens.access_token().await?;
        let endpoint = format!("https://cloudkms.googleapis.com/v1/{id}:{op}");
        #[cfg(test)]
        let endpoint = self
            .endpoint
            .as_ref()
            .map_or(endpoint, |base| format!("{base}/v1/{id}:{op}"));
        let request = self
            .http
            .post(endpoint)
            .bearer_auth(token.as_str())
            .header("content-type", "application/json")
            .body(bytes::Bytes::from_owner(body.encode()?));
        response(
            request
                .send()
                .await
                .map_err(|_| StorageError::unavailable("KMS transport failed"))?,
        )
        .await
    }
}
#[cfg(feature = "kms-gcp")]
#[async_trait]
impl KmsClient for GcpKms {
    async fn wrap(&self, id: &str, scope: &Scope, plaintext: &[u8; 32]) -> Result<Vec<u8>> {
        let body = SensitiveJson(
            json!({ "plaintext": STANDARD.encode(plaintext), "additionalAuthenticatedData": STANDARD.encode(format!("{LABEL}:{}", scope.as_str())) }),
        );
        field(&self.request(id, "encrypt", body).await?, "ciphertext")
    }
    async fn unwrap(&self, id: &str, scope: &Scope, wrapped: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
        let body = SensitiveJson(
            json!({ "ciphertext": STANDARD.encode(wrapped), "additionalAuthenticatedData": STANDARD.encode(format!("{LABEL}:{}", scope.as_str())) }),
        );
        key(field(
            &self.request(id, "decrypt", body).await?,
            "plaintext",
        )?)
    }
}

/// AWS signing credentials. `Debug` hides every field; secret material is wiped.
#[cfg(feature = "kms-aws")]
pub struct AwsCredentials {
    /// AWS access key identifier.
    pub access_key_id: Zeroizing<String>,
    /// AWS signing secret.
    pub secret_access_key: Zeroizing<String>,
    /// STS session token, when temporary credentials are used.
    pub session_token: Option<Zeroizing<String>>,
}
#[cfg(feature = "kms-aws")]
impl fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AwsCredentials(..)")
    }
}
/// Refreshable AWS credentials, such as STS workload credentials.
#[cfg(feature = "kms-aws")]
#[async_trait]
pub trait AwsCredentialsProvider: Send + Sync + fmt::Debug {
    /// Obtain credentials for this request.
    ///
    /// # Errors
    /// Credential acquisition failed.
    async fn credentials(&self) -> Result<AwsCredentials>;
}
/// AWS KMS REST client with `SigV4` signing and authenticated scope context.
/// Envelope key identities must be immutable key IDs or key ARNs; mutable
/// aliases are rejected because moving an alias could strand old envelopes.
#[cfg(feature = "kms-aws")]
pub struct AwsKms {
    http: reqwest::Client,
    region: String,
    credentials: Arc<dyn AwsCredentialsProvider>,
    #[cfg(test)]
    endpoint: Option<String>,
}
#[cfg(feature = "kms-aws")]
impl fmt::Debug for AwsKms {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AwsKms(..)")
    }
}
#[cfg(feature = "kms-aws")]
impl AwsKms {
    /// Construct a TLS client for an AWS region and refreshable credentials.
    ///
    /// # Errors
    /// Invalid region.
    pub fn new(
        region: impl Into<String>,
        credentials: Arc<dyn AwsCredentialsProvider>,
    ) -> Result<Self> {
        let region = region.into();
        if region.is_empty()
            || !region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(StorageError::invalid_input("invalid AWS region"));
        }
        Ok(Self {
            http: http_client()?,
            region,
            credentials,
            #[cfg(test)]
            endpoint: None,
        })
    }
    async fn request(&self, operation: &str, body: SensitiveJson) -> Result<SensitiveJson> {
        use sha2::{Digest, Sha256};
        let creds = self.credentials.credentials().await?;
        let suffix = if self.region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        };
        let host = format!("kms.{}.{suffix}", self.region);
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[..8];
        let target = format!("TrentService.{operation}");
        let body = body.encode()?;
        let mut headers = std::collections::BTreeMap::from([
            ("content-type", "application/x-amz-json-1.1".to_string()),
            ("host", host.clone()),
            ("x-amz-date", timestamp.clone()),
            ("x-amz-target", target.clone()),
        ]);
        if let Some(token) = &creds.session_token {
            headers.insert("x-amz-security-token", token.to_string());
        }
        let (canonical, signed) = canonical_request("POST", "/", "", &headers, body.as_slice());
        let credential_scope = format!("{date}/{}/kms/aws4_request", self.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{}",
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        let signature = signature(
            creds.secret_access_key.as_str(),
            date,
            &self.region,
            "kms",
            &to_sign,
        )?;
        let authorization = Zeroizing::new(format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed}, Signature={signature}",
            creds.access_key_id.as_str()
        ));
        let endpoint = format!("https://{host}/");
        #[cfg(test)]
        let endpoint = self.endpoint.clone().unwrap_or(endpoint);
        let mut request = self.http.post(endpoint);
        for (name, value) in &headers {
            request = request.header(*name, value);
        }
        headers.values_mut().for_each(Zeroize::zeroize);
        request = request
            .header("authorization", authorization.as_str())
            .body(bytes::Bytes::from_owner(body));
        response(
            request
                .send()
                .await
                .map_err(|_| StorageError::unavailable("KMS transport failed"))?,
        )
        .await
    }
}
#[cfg(feature = "kms-aws")]
fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &std::collections::BTreeMap<&str, String>,
    payload: &[u8],
) -> (Zeroizing<String>, String) {
    use sha2::{Digest, Sha256};
    let signed = headers.keys().copied().collect::<Vec<_>>().join(";");
    let mut canonical_headers = Zeroizing::new(String::new());
    for (name, value) in headers {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        for (index, word) in value.split_ascii_whitespace().enumerate() {
            if index > 0 {
                canonical_headers.push(' ');
            }
            canonical_headers.push_str(word);
        }
        canonical_headers.push('\n');
    }
    // CanonicalHeaders ends in LF; the field separator contributes another.
    let request = format!(
        "{method}\n{path}\n{query}\n{}\n{signed}\n{}",
        canonical_headers.as_str(),
        hex(&Sha256::digest(payload))
    );
    (Zeroizing::new(request), signed)
}
#[cfg(feature = "kms-aws")]
fn signature(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    to_sign: &str,
) -> Result<String> {
    let seed = Zeroizing::new(format!("AWS4{secret}"));
    let date_key = mac(seed.as_bytes(), date.as_bytes())?;
    let region_key = mac(&date_key[..], region.as_bytes())?;
    let service_key = mac(&region_key[..], service.as_bytes())?;
    let signing_key = mac(&service_key[..], b"aws4_request")?;
    Ok(hex(&mac(&signing_key[..], to_sign.as_bytes())?[..]))
}
#[cfg(feature = "kms-aws")]
fn mac(key: &[u8], data: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key)
        .map_err(|_| StorageError::crypto("AWS signing failed"))?;
    mac.update(data);
    Ok(Zeroizing::new(mac.finalize().into_bytes().into()))
}
#[cfg(feature = "kms-aws")]
fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 15)]));
    }
    encoded
}
#[cfg(feature = "kms-aws")]
fn validate_aws_key_id(id: &str) -> Result<()> {
    if id.is_empty() || id.starts_with("alias/") || id.contains(":alias/") {
        return Err(StorageError::invalid_input(
            "AWS KMS requires an immutable key id or key ARN",
        ));
    }
    Ok(())
}
#[cfg(feature = "kms-aws")]
#[async_trait]
impl KmsClient for AwsKms {
    async fn wrap(&self, id: &str, scope: &Scope, plaintext: &[u8; 32]) -> Result<Vec<u8>> {
        validate_aws_key_id(id)?;
        let body = SensitiveJson(
            json!({ "KeyId": id, "Plaintext": STANDARD.encode(plaintext), "EncryptionContext": { "format": LABEL, "scope": scope.as_str() } }),
        );
        field(&self.request("Encrypt", body).await?, "CiphertextBlob")
    }
    async fn unwrap(&self, id: &str, scope: &Scope, wrapped: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
        validate_aws_key_id(id)?;
        let body = SensitiveJson(
            json!({ "KeyId": id, "CiphertextBlob": STANDARD.encode(wrapped), "EncryptionContext": { "format": LABEL, "scope": scope.as_str() } }),
        );
        key(field(&self.request("Decrypt", body).await?, "Plaintext")?)
    }
}

#[cfg(test)]
#[path = "cloud_tests.rs"]
mod tests;
