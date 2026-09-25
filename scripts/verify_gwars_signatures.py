#!/usr/bin/env python3
"""Thin wrapper around pytest signature tests (backward compatible)."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_gwars_signatures.py", "-q"],
        cwd=ROOT,
    )
    if result.returncode == 0:
        print("OK: GWars signature checks passed")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
