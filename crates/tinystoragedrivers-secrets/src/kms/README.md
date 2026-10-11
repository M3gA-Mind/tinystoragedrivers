# KMS envelope keys

This module implements the secrets crate’s async `KeyProvider` over a scoped
`StorageBackend` and a `KmsClient`. Public types are exported from the crate
root: `KmsKeyProvider`, `KmsClient`, envelope constants, and the feature-gated
AWS/GCP clients and credential-provider ports. `cloud.rs` contains the cloud
transports; sibling test modules cover persistence, concurrency and HTTP
contracts with fake services.

## Persistence and concurrency

Each scope stores one wrapped 32-byte data key in `secret_keys/data-key-v1`.
The envelope records its format and KMS key identity. Scope and format are
bound as AWS encryption context or GCP additional authenticated data. Only
wrapped key material is persisted. Concurrent creators use an absent write
precondition; losers unwrap the winner. Rotation rewraps the same data key
and uses the stored document version as a CAS precondition, preserving
existing secret ciphertext.

Loading and rotation serialize per scope. Unrelated scopes proceed
independently. Plaintext cache entries are zeroized and expire after the
configured TTL; zero TTL disables reuse. Every async lookup validates the
persisted envelope before using the cache. The synchronous provider method
fails closed because it cannot perform this validation.

## Operational constraints

The document backend must provide durable scoped writes and preconditions.
Reset must be fenced or serialized with secret writes: validation cannot
prevent a reset between acquiring a key and writing a secret. Missing,
malformed or inaccessible envelopes never fall back to a local key source.
Rotation changes wrapping identity rather than rotating secret ciphertext.

AWS credentials and GCP access tokens come from the supplied provider ports;
this module does not persist them. `kms-aws` and `kms-gcp` enable their HTTP
transports. Transient service responses are retryable; permanent request,
cryptographic and authorization failures are not. Diagnostics omit raw cloud
response bodies and credentials. Deployments must retain access to the KMS
keys referenced by existing envelopes and allow the required wrap/unwrap
operations before changing the initial key identity.
