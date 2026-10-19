# Scope purge contract

This module defines the optional native reset port, explicit logical database
addresses, host-issued operation requests, durable receipts and scope
generations. Public exports remain centralized in the core crate root.
`mod_tests.rs` pins request validation and optional capability refusal.

The trusted host persists an operation identity, authorizes its database and
scope manifest, stops writers when required, and issues replacement handles
only after completion. Native implementations retain admission and receipts
outside erased tenant data. Ordinary constructors keep generation zero;
they do not discover a newer generation and revive an old caller.

Atomic and Quiescent modes express different guarantees. Drivers must refuse
unsupported modes before mutation. The port does not promise root-wide atomic
erasure, external key revocation or coordination of independent blob stores.
