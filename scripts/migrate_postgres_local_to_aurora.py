#!/usr/bin/env python3
"""Alias retrocompatible → upsert_local_postgres_to_aurora.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upsert_local_postgres_to_aurora import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
