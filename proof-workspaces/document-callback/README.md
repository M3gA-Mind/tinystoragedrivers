# Compiled document callback proof

Run from the repository root:

    bash scripts/test-document-callback.sh

This excluded fixture workspace builds a real TinyBus SDK module that depends
on the document contract and canonical three-port module client. The runner hashes the actual binary, stages
an allowlisted artifact beside its build output, and loads it through canonical
HostCallbackBinding. No unsafe ABI implementation is copied here.

The tests require the built artifact; they are neither ignored nor treated as
passing when it is absent. They cover native CAS/version/missing-document
results, captured collection/scope, confidential route refusals, attempted
selectors, revoked bindings and native stale-fence rejection. The fixture never opens a backend: port clients call the captured host server.

The fixture is a proof of the wire/admission boundary, not TinyMemes business
logic, a hostile-native-code sandbox, release availability, legacy import, or
the unfinished production TinyMemes persistence migration. In-process native
modules are trusted code with process privileges.

The wire uses document_id and strictly parsed inline document_json text. This
preserves arbitrary business JSON without confusing id-only data with native
TinyBus stream references. No privacy check or stream API is bypassed.

The scoped profile additionally covers metadata/schema, native CAS, cache-key
Get/Set/Get, environment secrets, JSON audit append/read/len, rejected authority
fields, empty collection declarations, blob/watch refusals, absent secrets,
native clock expiry, stale fences, retired generations and exact-instance
replacement. Every profile call uses the confidential route.

The replacement test uses two differently named SDK artifacts compiled from the
same relay source. Both have separate SHA allowlist admissions and distinct
provided bus names on one shared broker. It first reads a native replacement
marker through the replacement module, then rejects the old verified module and
an ordinary outsider against that actual callback destination. The SDK reserves
provided bus names, so copying the original artifact under another pathname
would not create a valid simultaneous second identity.

The broad monitor must receive public positive-control signals before and after
the confidential requests. The after signal is an event barrier following the
completed calls. Dead, lagged or timed-out observers fail; injected textual and
byte-encoded private sentinels prove the leak oracle fails when it should.
