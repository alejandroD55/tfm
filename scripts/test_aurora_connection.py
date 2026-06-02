#!/usr/bin/env python3
"""
Test simple de conectividad a Aurora PostgreSQL.

Modos soportados:

1) Password estático por variables de entorno
   export AURORA_HOST=...
   export AURORA_PORT=5432
   export AURORA_DB=...
   export AURORA_USER=...
   export AURORA_PASSWORD=...
   python scripts/test_aurora_connection.py

2) IAM auth (recomendado para tu setup)
   - lee secreto `aurora/credentials` (por defecto)
   - si `auth_mode=iam`, genera token RDS automáticamente
   python scripts/test_aurora_connection.py --use-secret
"""

from __future__ import annotations

import os
import sys
import json

import boto3
import psycopg2


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return value


def _load_secret(secret_name: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    raw = resp.get("SecretString", resp.get("SecretBinary"))
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def _build_aurora_config(use_secret: bool) -> dict:
    region = os.getenv("AWS_REGION", "eu-north-1")

    if use_secret:
        secret_name = os.getenv("AURORA_SECRET_NAME", "aurora/credentials")
        creds = _load_secret(secret_name, region)
        host = creds["host"]
        port = int(creds.get("port", 5432))
        dbname = creds.get("dbname", "tfm")
        user = creds["username"]
        auth_mode = str(creds.get("auth_mode", "")).lower()

        if auth_mode == "iam":
            token = boto3.client("rds", region_name=region).generate_db_auth_token(
                DBHostname=host, Port=port, DBUsername=user, Region=region
            )
            return {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": token,
                "sslmode": "require",
            }

        password = creds.get("password")
        if not password:
            raise RuntimeError(
                "El secreto aurora/credentials no tiene password y auth_mode no es iam"
            )
        return {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "sslmode": os.getenv("AURORA_SSLMODE", "require"),
        }

    # fallback env vars
    return {
        "host": _required_env("AURORA_HOST"),
        "port": int(os.getenv("AURORA_PORT", "5432")),
        "dbname": _required_env("AURORA_DB"),
        "user": _required_env("AURORA_USER"),
        "password": _required_env("AURORA_PASSWORD"),
        "sslmode": os.getenv("AURORA_SSLMODE", "require"),
    }


def main(argv: list[str]) -> int:
    use_secret = "--use-secret" in argv
    cfg = _build_aurora_config(use_secret=use_secret)

    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        sslmode=cfg["sslmode"],
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT now(), current_database(), current_user")
            now, db, current_user = cur.fetchone()
            print(
                f"OK Aurora connection | now={now} | db={db} | user={current_user} | auth={'secret' if use_secret else 'env'}"
            )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
