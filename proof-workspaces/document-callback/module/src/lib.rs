//! Primary identity for the shared callback relay fixture.
const PROOF_INTERFACE: &str = "ai.tinyhumans.storage.DocumentProof1";
mod relay;
// The canonical SDK owns native ABI unsafe code. No local raw ABI implementation.
tinybus_module::module_export! {
    setup = relay::setup,
    config = serde_json::Value,
    worker_threads = 1,
    provides = ["ai.tinyhumans.storage.DocumentProof1"],
    methods = ["Get", "Put", "Delete", "ScopedCall", "ScopedPlainCall", "NativeRoundtrip", "CallbackAttempts"],
    signals = [], requires = [], optional = [], lazy = false,
}
