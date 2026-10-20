# Encrypted file engine

`EncryptedFileSecrets` stores a JSON name-to-text map in one ChaCha20-Poly1305
blob. It keeps the established `secrets.enc` bytes unchanged, serializes
read-modify-write operations with the sibling `.lock` file, and uses private
staging files with verified atomic publication.

File-backed secret values are UTF-8. Reads return zeroizing byte buffers;
invalid names, invalid text, unreadable ciphertext, and failed publication
return typed errors without logging secret values or names.

Conditional writes hold the same cross-process lock while checking the secret
revision and publishing the replacement. Their opaque revision includes the
file identity and ciphertext snapshot, so an unrelated file update may
conservatively conflict with a prior read.
