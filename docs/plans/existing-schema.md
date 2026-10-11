# Existing schema implementation

1. Add optional default-refusing backend and document methods with capability tests. Confirm Memory native behavioral regressions fail against defaults.
2. Implement native Memory declaration restriction and read-only validation; pin sticky named/fenced constructors and scoped deletion.
3. Pin SQLite/File missing named storage and declaration mutation regressions; implement native restricted construction and metadata validation without schema initialization.
4. Pin Mongo fresh-handle native catalog checks and lazy no-DDL behavior; retain fences and unsupported GridFS writes.
5. Verify default and all-feature tests, strict Clippy, build, formatting, rustdoc and live native fixtures where available. Document measured limitations and publish a canonical ready PR only after actual gates.
