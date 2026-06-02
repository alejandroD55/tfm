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

  export AURORA_HOST=...
  export AURORA_PORT=5432
  export AURORA_DB=...
  export AURORA_USER=...
  export AURORA_PASSWORD=...
  export AURORA_SSLMODE=require

  python scripts/sync_local_pg_to_aurora.py 2025-01-01 2025-12-31
"""

from __future__ import annotations

import os
import sys
from datetime import date
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values

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


def fetch_rows_by_range(
    conn,
    table: str,
    start_date: date,
    end_date: date,
    date_column: str = "batch_date",
) -> List[Tuple]:
    sql = f"""
        SELECT *
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
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)


def replace_range(start_date: date, end_date: date) -> Dict[str, Dict[str, int]]:
    summary: Dict[str, Dict[str, int]] = {}

    with local_conn() as lconn, aurora_conn() as aconn:
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
                lcols = get_columns(lconn, table)
                acols = get_columns(aconn, table)
                if lcols != acols:
                    raise RuntimeError(
                        f"Schema mismatch en {table}: columnas local != aurora"
                    )
                col_map[table] = acols

            # 1) DELETE en Aurora (orden seguro)
            for table in DELETE_TABLES:
                if table not in existing_tables:
                    continue
                deleted = delete_rows_by_range(aconn, table, start_date, end_date)
                summary.setdefault(table, {})
                summary[table]["deleted_aurora"] = deleted

            # 2) INSERT desde local (orden lógico)
            for table in existing_tables:
                rows = fetch_rows_by_range(lconn, table, start_date, end_date)
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
    if len(argv) != 3:
        print(
            "Uso: python scripts/sync_local_pg_to_aurora.py YYYY-MM-DD YYYY-MM-DD",
            file=sys.stderr,
        )
        return 2

    start_date = date.fromisoformat(argv[1])
    end_date = date.fromisoformat(argv[2])
    if start_date > end_date:
        raise RuntimeError("start_date no puede ser mayor que end_date")

    summary = replace_range(start_date, end_date)
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
