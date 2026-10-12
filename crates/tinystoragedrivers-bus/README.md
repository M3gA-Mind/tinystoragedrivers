# Document collection bus contract

A compiled module uses DocumentClient for confidential Get, Put and Delete on
one host-captured collection. Requests contain only document identifiers, JSON
data and native compare-and-swap conditions. Unknown fields are rejected.

No native storage crate, URL resolver, credentials, database client, schema
declaration, scope selector or admission issuer is linked by this contract.
Versions and categories have a separate minimal wire representation. Native
error messages and source chains are not serialized.

Transport errors are the outer result; native storage refusals are the inner
Outcome. A missing Get returns Ok(None), rather than fabricating a document.
See [the contract](../../docs/specs/document-callback.md).
