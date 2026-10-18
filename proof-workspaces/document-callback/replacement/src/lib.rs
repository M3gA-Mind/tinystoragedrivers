//! Replacement identity for the same callback relay fixture.
const PROOF_INTERFACE: &str = "ai.tinyhumans.storage.ReplacementProof1";
#[path = "../../module/src/relay.rs"]
mod relay;
// The canonical SDK owns native ABI unsafe code. No local raw ABI implementation.
tinybus_module::module_export! {
    setup = relay::setup,
    worker_threads = 1,
    provides = ["ai.tinyhumans.storage.ReplacementProof1"],
    methods = ["Get", "Put", "Delete", "ScopedCall", "ScopedPlainCall", "NativeRoundtrip"],
    signals = [], requires = [], optional = [], lazy = false,
}
