#!/usr/bin/env python3
"""
Test simple de conectividad a Aurora PostgreSQL.

Uso:
  export AURORA_HOST=...
  export AURORA_PORT=5432
  export AURORA_DB=...
  export AURORA_USER=...
  export AURORA_PASSWORD=...
  python scripts/test_aurora_connection.py
"""

from __future__ import annotations

import os
import sys

import psycopg2


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return value


def main() -> int:
    host = _required_env("AURORA_HOST")
    port = int(os.getenv("AURORA_PORT", "5432"))
    dbname = _required_env("AURORA_DB")
    user = _required_env("AURORA_USER")
    password = _required_env("AURORA_PASSWORD")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=os.getenv("AURORA_SSLMODE", "require"),
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT now(), current_database(), current_user")
            now, db, current_user = cur.fetchone()
            print(
                f"OK Aurora connection | now={now} | db={db} | user={current_user}"
            )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
