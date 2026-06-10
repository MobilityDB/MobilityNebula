#!/usr/bin/env python3
"""Decouple generated MEOS operator TUs from the regenerated plugin registrar.

Each generated operator .cpp includes <PhysicalFunctionRegistry.hpp> (or the
Logical one), which pulls in <...GeneratedRegistrar.inc> — a file regenerated
every time any operator is added. That made all ~2000 operator TUs recompile on
every add. The registry headers now skip that .inc when NES_PLUGIN_OPERATOR_TU is
defined, so an operator TU only needs the registry types plus a self-declaration
of its own Register function.

This pass (idempotent) prepends `#define NES_PLUGIN_OPERATOR_TU` and inserts the
self-declaration before the operator's `namespace NES` block. Run standalone over
the whole Meos tree, and invoked by codegen_nebula.py for freshly emitted ops.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

MARK = "NES_PLUGIN_OPERATOR_TU"
NS_RE = re.compile(r"^namespace NES\b", re.MULTILINE)


def decouple_file(path: Path, registry: str) -> bool:
    """registry is 'PhysicalFunction' or 'LogicalFunction'. Returns True if changed."""
    txt = path.read_text()
    if MARK in txt:
        return False
    m = re.search(rf"{registry}GeneratedRegistrar::(Register\w+)\s*\(", txt)
    if not m:
        return False  # not a registering operator TU; leave untouched
    reg_fn = m.group(1)
    fwd = (f"/* Decoupled from the regenerated plugin registrar (see "
           f"{registry}Registry.hpp): only the registry types are pulled in, and "
           f"this operator declares its own Register function. */\n"
           f"namespace NES::{registry}GeneratedRegistrar {{ {registry}RegistryReturnType "
           f"{reg_fn}({registry}RegistryArguments); }}\n\n")
    ns = NS_RE.search(txt)
    if not ns:
        return False
    txt = txt[:ns.start()] + fwd + txt[ns.start():]
    txt = f"#define {MARK}\n" + txt
    path.write_text(txt)
    return True


def run(root: Path) -> int:
    changed = 0
    for sub, registry in (("nes-physical-operators/src/Functions/Meos", "PhysicalFunction"),
                          ("nes-logical-operators/src/Functions/Meos", "LogicalFunction")):
        d = root / sub
        if not d.is_dir():
            continue
        for cpp in sorted(d.glob("*.cpp")):
            if decouple_file(cpp, registry):
                changed += 1
    return changed


if __name__ == "__main__":
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    n = run(root)
    print(f"decoupled {n} operator TU(s)")
