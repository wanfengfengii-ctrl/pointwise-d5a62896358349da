#!/usr/bin/env python3
"""Idempotent setup check for the standard-library/SQLite Lake."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def main() -> int:
    if sys.version_info < (3, 9):
        print("Python 3.9+ is required", file=sys.stderr)
        return 1

    if sqlite3.sqlite_version_info < (3, 8):
        print("SQLite 3.8+ is required", file=sys.stderr)
        return 1

    import lake  # noqa: F401

    print(f"setup ok: python={sys.version.split()[0]} sqlite={sqlite3.sqlite_version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
