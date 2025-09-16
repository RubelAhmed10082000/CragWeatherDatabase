"""
Quick import crawl to catch broken imports after refactors.

Env vars:
- PKG: top-level package name (default: 'arquivio')
- SRC: source root (default: 'src')
"""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
from pathlib import Path

PKG = os.environ.get("PKG", "arquivio")
SRC = os.environ.get("SRC", "src")

src_path = Path(SRC)
pkg_path = src_path / PKG.replace(".", "/")

if not pkg_path.exists():
    print(f"[FATAL] Package path not found: {pkg_path.resolve()}")
    sys.exit(1)

# Ensure src is importable for this process
sys.path.insert(0, str(src_path))

errors: list[tuple[str, str]] = []

for mod in pkgutil.walk_packages([str(pkg_path)], prefix=f"{PKG}."):
    name = mod.name
    try:
        importlib.import_module(name)
    except Exception as e:
        errors.append((name, repr(e)))

if errors:
    for name, err in errors:
        print(f"[IMPORT ERROR] {name}: {err}")
    sys.exit(1)

print(f"All modules under '{PKG}' imported OK.")
