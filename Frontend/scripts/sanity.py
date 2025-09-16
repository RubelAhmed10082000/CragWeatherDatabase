#!/usr/bin/env python3
"""
scripts/sanity.py - cross-platform health check.
Runs:
  1) import package
  2) import crawl (internal)
  3) pytest --collect-only
  4) pytest -q --maxfail=1
  5) ruff check src tests  (optional via --no-lint)
"""

import argparse
import importlib
import os
import pkgutil
import subprocess
import sys
from pathlib import Path


def run(cmd, env=None, verbose=False):
    if verbose:
        print("$", " ".join(cmd))
    r = subprocess.run(cmd, env=env)
    if r.returncode:
        sys.exit(r.returncode)


def step(name, fn):
    print(f"==> {name}")
    try:
        fn()
    except SystemExit as e:
        if e.code:
            print(f"? {name} FAILED (exit {e.code})")
            raise
        else:
            print(f"? {name}")
    except Exception as e:
        print(f"? {name} FAILED: {e}")
        sys.exit(1)
    else:
        print(f"? {name}")


def import_root(pkg, src):
    if src not in sys.path:
        sys.path.insert(0, src)
    importlib.import_module(pkg)
    print("OK: import", pkg)
    print("PYTHONPATH (added):", src)


def import_crawl(pkg, src):
    if src not in sys.path:
        sys.path.insert(0, src)
    pkg_path = Path(src) / pkg.replace(".", "/")
    if not pkg_path.exists():
        print(f"[FATAL] Package path not found: {pkg_path.resolve()}")
        sys.exit(1)
    errors = []
    for m in pkgutil.walk_packages([str(pkg_path)], prefix=f"{pkg}."):
        try:
            importlib.import_module(m.name)
        except Exception as e:
            errors.append((m.name, repr(e)))
    if errors:
        for name, err in errors:
            print(f"[IMPORT ERROR] {name}: {err}")
        sys.exit(1)
    print(f"All modules under '{pkg}' imported OK.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pkg", default="arquivio")
    ap.add_argument("--src", default="src")
    ap.add_argument("--no-lint", action="store_true")
    ap.add_argument("--skip-tests", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()

    env = os.environ.copy()
    env["PYTHONPATH"] = a.src

    step(f"Import package '{a.pkg}'", lambda: import_root(a.pkg, a.src))
    step("Import crawl", lambda: import_crawl(a.pkg, a.src))
    if not a.skip_tests:
        step("Pytest collect-only", lambda: run(["pytest", "--collect-only", "-q"], env, a.verbose))
        step("Pytest quick run", lambda: run(["pytest", "-q", "--maxfail=1"], env, a.verbose))
    else:
        print("Skipping pytest steps (--skip-tests).")
    if not a.no_lint:
        step(
            "Ruff lint (src tests)", lambda: run(["ruff", "check", a.src, "tests"], env, a.verbose)
        )
    else:
        print("Skipping Ruff lint (--no-lint).")


if __name__ == "__main__":
    main()
