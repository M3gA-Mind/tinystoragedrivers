# Compiled document callback proof

Run from the repository root:

    bash scripts/test-document-callback.sh

This excluded fixture workspace builds a real TinyBus SDK module that depends
on the bus-only document contract. The runner hashes the actual binary, stages
an allowlisted artifact beside its build output, and loads it through canonical
HostCallbackBinding. No unsafe ABI implementation is copied here.

The tests require the built artifact; they are neither ignored nor treated as
passing when it is absent. They cover native CAS/version/missing-document
results, captured collection/scope, confidential route refusals, attempted
selectors, revoked bindings, and native stale-fence rejection. The fixture
never opens a backend: port clients call the captured host server. The module
has no storage implementation dependency.

The fixture is a proof of the wire/admission boundary, not TinyMemes business
logic, a hostile-native-code sandbox, release availability, legacy import, or
the unfinished production TinyMemes persistence migration. In-process native
modules are trusted code with process privileges.

The wire uses document_id and strictly parsed inline document_json text. This
preserves arbitrary business JSON without confusing id-only data with native
TinyBus stream references. No privacy check or stream API is bypassed.
