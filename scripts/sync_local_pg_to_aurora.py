#!/usr/bin/env python3
"""
Reemplazo por rango de fechas desde PostgreSQL local -> Aurora PostgreSQL.

Diseñado para tu bootstrap local:
1) ejecutas bootstrap en local (rellena Postgres local y Mongo),
2) ejecutas este script para sustituir en Aurora el bloque [start_date, end_date].

Uso:
  export LOCAL_PG_DB=...
  export LOCAL_PG_USER=...
  export LOCAL_PG_PASSWORD=...
  export LOCAL_PG_HOST=localhost
  export LOCAL_PG_PORT=5432
  export LOCAL_PG_SSLMODE=disable

  # Opción A: por env vars (password estático)
  export AURORA_HOST=...
  export AURORA_PORT=5432
  export AURORA_DB=...
  export AURORA_USER=...
  export AURORA_PASSWORD=...
  export AURORA_SSLMODE=require

  # Opción B: IAM + Secrets Manager (recomendado)
  export AWS_REGION=eu-north-1
  export AURORA_SECRET_NAME=aurora/credentials
  # al ejecutar añade --use-secret

  python scripts/sync_local_pg_to_aurora.py 2025-01-01 2025-12-31
"""

from __future__ import annotations

import os
import sys
import json
from datetime import date
from typing import Dict, List, Sequence, Tuple

import boto3
import psycopg2
from psycopg2.extras import Json, execute_values

# Tablas usadas por el pipeline/bootstrap y reporting.
# Insert order (si existieran FK): padres primero, hijas después.
INSERT_TABLES: List[str] = [
    "batch_log",
    "pipeline_kpis",
    "macro_sentiment_scores",
    "market_regime_state",
    "technical_indicators",
    "sentiment_scores",
    "trading_signals",
    "signal_explanations",
    "position_state",
    "signal_outcomes",
]

# Delete order inverso para evitar problemas de integridad referencial.
DELETE_TABLES: List[str] = list(reversed(INSERT_TABLES))

# Columnas técnicas que no conviene replicar entre instancias
# cuando hay PK autoincremental independiente.
EXCLUDED_TRANSFER_COLUMNS = {"id"}


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return value


def local_conn():
    return psycopg2.connect(
        host=os.getenv("LOCAL_PG_HOST", "localhost"),
        port=int(os.getenv("LOCAL_PG_PORT", "5432")),
        dbname=_required_env("LOCAL_PG_DB"),
        user=_required_env("LOCAL_PG_USER"),
        password=_required_env("LOCAL_PG_PASSWORD"),
        sslmode=os.getenv("LOCAL_PG_SSLMODE", "disable"),
        connect_timeout=10,
    )


def aurora_conn():
    return _aurora_conn_from_env()


def _load_secret(secret_name: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    raw = resp.get("SecretString", resp.get("SecretBinary"))
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def _aurora_conn_from_secret():
    region = os.getenv("AWS_REGION", "eu-north-1")
    secret_name = os.getenv("AURORA_SECRET_NAME", "aurora/credentials")
    creds = _load_secret(secret_name, region)

    host = creds["host"]
    port = int(creds.get("port", 5432))
    dbname = creds.get("dbname", "tfm")
    username = creds["username"]
    auth_mode = str(creds.get("auth_mode", "")).lower()

    if auth_mode == "iam":
        token = boto3.client("rds", region_name=region).generate_db_auth_token(
            DBHostname=host, Port=port, DBUsername=username, Region=region
        )
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=username,
            password=token,
            sslmode="require",
            connect_timeout=10,
        )

    password = creds.get("password")
    if not password:
        raise RuntimeError(
            "El secreto aurora/credentials no tiene password y auth_mode no es iam"
        )
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=username,
        password=password,
        sslmode=os.getenv("AURORA_SSLMODE", "require"),
        connect_timeout=10,
    )


def _aurora_conn_from_env():
    return psycopg2.connect(
        host=_required_env("AURORA_HOST"),
        port=int(os.getenv("AURORA_PORT", "5432")),
        dbname=_required_env("AURORA_DB"),
        user=_required_env("AURORA_USER"),
        password=_required_env("AURORA_PASSWORD"),
        sslmode=os.getenv("AURORA_SSLMODE", "require"),
        connect_timeout=10,
    )


def table_exists(conn, table: str) -> bool:
    q = """
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = %s
    )
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        return bool(cur.fetchone()[0])


def get_columns(conn, table: str) -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        return [r[0] for r in cur.fetchall()]


def get_columns_with_types(conn, table: str) -> List[Tuple[str, str]]:
    q = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        return [(r[0], r[1]) for r in cur.fetchall()]


def check_schema_differences() -> Dict[str, Dict[str, object]]:
    report: Dict[str, Dict[str, object]] = {}
    with local_conn() as lconn, _aurora_conn_from_secret() as aconn:
        for table in INSERT_TABLES:
            l_exists = table_exists(lconn, table)
            a_exists = table_exists(aconn, table)
            item: Dict[str, object] = {
                "local_exists": l_exists,
                "aurora_exists": a_exists,
                "ok": False,
                "details": "",
            }
            if not l_exists or not a_exists:
                missing = []
                if not l_exists:
                    missing.append("local")
                if not a_exists:
                    missing.append("aurora")
                item["details"] = f"tabla ausente en: {', '.join(missing)}"
                report[table] = item
                continue

            local_cols = get_columns_with_types(lconn, table)
            aurora_cols = get_columns_with_types(aconn, table)
            local_set = set(local_cols)
            aurora_set = set(aurora_cols)

            only_local = sorted(local_set - aurora_set)
            only_aurora = sorted(aurora_set - local_set)
            same_columns_order = [c for c, _ in local_cols] == [c for c, _ in aurora_cols]

            if not only_local and not only_aurora:
                item["ok"] = True
                item["details"] = (
                    "schema alineado"
                    if same_columns_order
                    else "schema alineado (orden de columnas distinto, tolerado)"
                )
            else:
                parts = []
                if only_local:
                    parts.append(f"faltan en aurora: {only_local}")
                if only_aurora:
                    parts.append(f"sobran en aurora: {only_aurora}")
                item["details"] = " | ".join(parts)
            report[table] = item
    return report


def print_schema_report(report: Dict[str, Dict[str, object]]) -> int:
    print("Schema check local -> Aurora")
    mismatches = 0
    for table in INSERT_TABLES:
        if table not in report:
            continue
        item = report[table]
        ok = bool(item.get("ok", False))
        status = "OK" if ok else "MISMATCH"
        print(f"- {table}: {status} | {item.get('details', '')}")
        if not ok:
            mismatches += 1
    print(f"Total mismatches: {mismatches}")
    return mismatches


def fetch_rows_by_range(
    conn,
    table: str,
    start_date: date,
    end_date: date,
    columns: Sequence[str],
    date_column: str = "batch_date",
) -> List[Tuple]:
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    sql = f"""
        SELECT {cols_sql}
        FROM public.{table}
        WHERE {date_column} BETWEEN %s AND %s
        ORDER BY {date_column}
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        return cur.fetchall()


def delete_rows_by_range(
    conn,
    table: str,
    start_date: date,
    end_date: date,
    date_column: str = "batch_date",
) -> int:
    sql = f"DELETE FROM public.{table} WHERE {date_column} BETWEEN %s AND %s"
    with conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        return int(cur.rowcount or 0)


def insert_rows(
    conn,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Tuple],
    page_size: int = 5000,
) -> int:
    if not rows:
        return 0
    cols = ", ".join(columns)
    sql = f"INSERT INTO public.{table} ({cols}) VALUES %s"
    normalized_rows: List[Tuple] = []
    for row in rows:
        norm_values = []
        for value in row:
            if isinstance(value, (dict, list)):
                norm_values.append(Json(value))
            else:
                norm_values.append(value)
        normalized_rows.append(tuple(norm_values))

    with conn.cursor() as cur:
        execute_values(cur, sql, normalized_rows, page_size=page_size)
    return len(rows)


def replace_range(
    start_date: date, end_date: date, use_secret: bool = False
) -> Dict[str, Dict[str, int]]:
    summary: Dict[str, Dict[str, int]] = {}

    with local_conn() as lconn, (
        _aurora_conn_from_secret() if use_secret else _aurora_conn_from_env()
    ) as aconn:
        lconn.autocommit = False
        aconn.autocommit = False

        try:
            existing_tables: List[str] = []
            skipped_tables: List[str] = []
            for table in INSERT_TABLES:
                if table_exists(lconn, table) and table_exists(aconn, table):
                    existing_tables.append(table)
                else:
                    skipped_tables.append(table)

            for table in skipped_tables:
                summary[table] = {
                    "skipped": 1,
                    "deleted_aurora": 0,
                    "inserted_aurora": 0,
                    "fetched_local": 0,
                }

            # Verificar compatibilidad de columnas antes de tocar datos.
            col_map: Dict[str, List[str]] = {}
            for table in existing_tables:
                lcols = set(get_columns_with_types(lconn, table))
                acols_typed = get_columns_with_types(aconn, table)
                acols = set(acols_typed)
                if lcols != acols:
                    raise RuntimeError(
                        f"Schema mismatch en {table}: columnas/tipos local != aurora"
                    )
                # Orden Aurora para SELECT local e INSERT Aurora.
                transferable_cols = [
                    c for c, _ in acols_typed if c not in EXCLUDED_TRANSFER_COLUMNS
                ]
                if not transferable_cols:
                    raise RuntimeError(
                        f"Tabla {table} sin columnas transferibles tras exclusiones"
                    )
                col_map[table] = transferable_cols

            # 1) DELETE en Aurora (orden seguro)
            for table in DELETE_TABLES:
                if table not in existing_tables:
                    continue
                deleted = delete_rows_by_range(aconn, table, start_date, end_date)
                summary.setdefault(table, {})
                summary[table]["deleted_aurora"] = deleted

            # 2) INSERT desde local (orden lógico)
            for table in existing_tables:
                rows = fetch_rows_by_range(
                    lconn, table, start_date, end_date, columns=col_map[table]
                )
                inserted = insert_rows(aconn, table, col_map[table], rows)
                summary.setdefault(table, {})
                summary[table]["fetched_local"] = len(rows)
                summary[table]["inserted_aurora"] = inserted
                summary[table].setdefault("deleted_aurora", 0)
                summary[table].setdefault("skipped", 0)

            aconn.commit()
            return summary
        except Exception:
            aconn.rollback()
            raise


def _print_summary(summary: Dict[str, Dict[str, int]]) -> None:
    print("SYNC OK (local -> Aurora) by date range")
    for table in INSERT_TABLES:
        if table not in summary:
            continue
        s = summary[table]
        if s.get("skipped"):
            print(f"- {table}: SKIPPED (no existe en local o en Aurora)")
            continue
        print(
            f"- {table}: deleted={s.get('deleted_aurora', 0)} | "
            f"local={s.get('fetched_local', 0)} | inserted={s.get('inserted_aurora', 0)}"
        )


def main(argv: Sequence[str]) -> int:
    use_secret = False
    check_schema_only = False
    args = list(argv)
    if "--use-secret" in args:
        use_secret = True
        args.remove("--use-secret")
    if "--check-schema-only" in args:
        check_schema_only = True
        args.remove("--check-schema-only")

    if check_schema_only:
        # Para check de schema, usamos siempre secreto (IAM/secret) para Aurora
        report = check_schema_differences()
        mismatches = print_schema_report(report)
        return 1 if mismatches else 0

    if len(args) != 2:
        print(
            "Uso: python scripts/sync_local_pg_to_aurora.py [--use-secret] [--check-schema-only] YYYY-MM-DD YYYY-MM-DD",
            file=sys.stderr,
        )
        return 2

    start_date = date.fromisoformat(args[0])
    end_date = date.fromisoformat(args[1])
    if start_date > end_date:
        raise RuntimeError("start_date no puede ser mayor que end_date")

    summary = replace_range(start_date, end_date, use_secret=use_secret)
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
