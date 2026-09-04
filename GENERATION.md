# MobilityNebula generation — the canonical per-binding generator policy

This document is the contract for how MobilityNebula is generated, under the ecosystem-wide
per-binding generator policy.

## The policy (ecosystem-wide)

Every MobilityDB language/surface binding is a **pure projection of the MEOS-API catalog**,
and **each binding owns its own generator, in its own repo**, in a canonical layout. The
single source of truth is the **catalog** (`MEOS-API/output/meos-idl.json`, generated from
the MEOS C headers). A binding is an independent, plug-and-play module that owns its
generation.

Each binding repo satisfies the same invariants: in-repo generator; own
`tools/pin/compose-order.txt`; vendored/pinned catalog; thin language projection
(language-neutral decisions live in the catalog); full automation toward a zero-hand-written
surface (generate-then-retire; the last green-CI version is the equivalence probe).

## MobilityNebula scope: generated NES MEOS operators

MobilityNebula's generator lives at **`tools/codegen/codegen_nebula.py`** (with
`codegen_aggregations.py` and `build_descriptor.py`). It reads `meos-idl.json` and emits the
NebulaStream (NES) MEOS-operator surface — the per-event operators plus the idempotent
build / grammar / QPC glue — **organized by type family**, with family gating emitted from
catalog metadata (`#if <FAMILY>` / CMake-subdir) so a regeneration reproduces it. The
operator-name token equals the MEOS symbol; nothing is hand-special-cased.

## Generate-then-retire — the green-CI version is the probe

The NES operator surface is **generated**, never hand-written: extend the generator and
regenerate. Each per-family wave is proven against the **last green-CI version** (the system
tests + the round-trip/aggregate gates) before it lands. Any operator that seems to need a
hand `#if` or bespoke glue is an irregularity to wipe — emit it from catalog metadata
instead; any symbol the binding can't reach is fixed at source in MEOS, never hand-added.

## Pinning

The vendored `meos-idl.json` is generated from a MobilityDB `ecosystem-pin-*` via the
MEOS-API `run.py`. That pin is the *catalog/surface* input; MobilityNebula's own
`tools/pin/compose-order.txt` governs *this repo's* PR accumulate. See it for the composing
set (the generator infra folds first, then the per-family operator waves).
