# Handoff → MEOS-C task: promote binding-needed internals to public `meos.h`

**From:** MobilityNebula streaming-parity task. **To:** the MEOS-C / MobilityDB
header-owner task. **Why now:** MobilitySpark recently needed `temporal_basetype`
explicitly; the Nebula streaming-parity grind hit the same wall. These functions
are **exported** in `libmeos` (`nm -D` confirms) but **declared only in
`meos_internal.h`**, so every binding that consumes the public header surface
(Nebula codegen, PyMEOS, Duck, Spark) must reach into internals to call them.
Per [[expose-meos-symbols-when-needed]], expose them rather than skip the gap.

## Request

Move these from `meos_internal.h` to **public `meos.h`** (and make `MeosType` /
`tempSubtype` / `interpType` / `meosOper` enums public where a public signature
needs them). All are already exported — this is a **declaration-visibility**
change, no ABI/codegen impact.

### 1. `*_hash_extended` — visibility ASYMMETRY (highest priority, trivial)

`set_`, `span_`, `spanset_`, `tbox_hash_extended` are **already public**; their
siblings are needlessly internal. Promote for symmetry:

- `cbuffer_hash_extended(const Cbuffer*, uint64 seed)`
- `npoint_hash_extended(const Npoint*, uint64 seed)`
- `pose_hash_extended(const Pose*, uint64 seed)`
- `stbox_hash_extended(const STBox*, uint64 seed)`

### 2. Type-dispatch predicates (the `temporal_basetype` family) — 43 fns

`bool <x>_type(MeosType)` / `<x>_basetype` / `<x>_spantype` etc. These are
**streaming-runtime metadata**: a window operator on a `tfloat`/`tpoint` calls
them (on the value's `temptype`) to dispatch deserialize/compare/aggregate — the
exact reason Spark needs `temporal_basetype`. Needs `MeosType` public too.
Representative: `temporal_basetype`, `temporal_type`, `tnumber_type`, `tgeo_type`,
`tpoint_type`, `tgeometry_type`, `tspatial_type`, `set_type`, `set_basetype`,
`span_type`, `span_basetype`, `spanset_type`, `numspan_type`, `geoset_type`,
`time_type`, … (full 43-list in the parity task memory).

### 3. enum↔name reflection — `interptype_name`, `meosoper_name`,
`tempsubtype_name`, `tempsubtype_from_string`, `geo_typename` (need the enums public).

## Nebula-side plan meanwhile (no MEOS change required to proceed)

The 12 public-or-promotable **data-input** ops (`temporal_subtype`, the 8
`*_hash_extended`, `int32/int64/text_cmp`) are implemented as per-event Nebula
operators now. The 43 type predicates are implemented **bound to the data value**
(`<pred>((MeosType) temp->temptype)` — `temptype` is a public struct field), the
codegen including `meos_internal.h` until #2 lands. When #1/#2 land, the Nebula
operators drop the internal include — no regen churn.
