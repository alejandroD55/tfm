#!/usr/bin/env python3
"""
Upsert PostgreSQL local (bootstrap) → Aurora AWS.

Ejecutar DESPUÉS del bootstrap, de forma independiente:

  python scripts/upsert_local_postgres_to_aurora.py --yes

Requisitos:
  - Postgres local con datos (docker compose, puerto 5433 por defecto)
  - Schema aplicado en Aurora (migrations / Job tfm-db-migrate)
  - Túnel o acceso a Aurora (AURORA_* en .env o --aurora-secret)

Modos:
  replace (default) — TRUNCATE tablas destino + COPY completo desde local
  merge             — INSERT … ON CONFLICT DO UPDATE (sin borrar el resto)

Ver también: scripts/MIGRATE_LOCAL_TO_AURORA.md
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

REPO_ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger("upsert_aurora")

# Tablas escritas por bootstrap_365_days.py (orden de carga).
BOOTSTRAP_TABLES: List[str] = [
    "batch_log",
    "macro_sentiment_scores",
    "market_regime_state",
    "sentiment_scores",
    "technical_indicators",
    "trading_signals",
    "signal_explanations",
    "signal_outcomes",
    "pipeline_kpis",
    "position_state",
]

SERIAL_TABLES = [
    "batch_log",
    "sentiment_scores",
    "technical_indicators",
    "trading_signals",
    "signal_explanations",
    "pipeline_kpis",
    "signal_outcomes",
    "macro_sentiment_scores",
    "market_regime_state",
]

# Claves de conflicto para modo merge (deben coincidir con UNIQUE en Aurora).
TABLE_CONFLICT_KEYS: Dict[str, List[str]] = {
    "batch_log": ["run_id"],
    "macro_sentiment_scores": ["batch_date"],
    "market_regime_state": ["batch_date"],
    "sentiment_scores": ["batch_date", "ticker", "headline"],
    "technical_indicators": ["batch_date", "ticker"],
    "trading_signals": ["batch_date", "ticker"],
    "signal_explanations": ["batch_date", "ticker"],
    "signal_outcomes": ["batch_date", "ticker"],
    "pipeline_kpis": ["run_id", "stage"],
    "position_state": ["batch_date", "ticker"],
}

# Columna de filtro por rango de fechas (None = siempre copia tabla entera).
TABLE_DATE_COLUMN: Dict[str, Optional[str]] = {
    "batch_log": "batch_date",
    "macro_sentiment_scores": "batch_date",
    "market_regime_state": "batch_date",
    "sentiment_scores": "batch_date",
    "technical_indicators": "batch_date",
    "trading_signals": "batch_date",
    "signal_explanations": "batch_date",
    "signal_outcomes": "batch_date",
    "pipeline_kpis": "batch_date",
    "position_state": "batch_date",
}


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _load_local_config() -> Dict[str, Any]:
    load_dotenv(REPO_ROOT / ".env")
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("POSTGRES_PORT", "5433")),
        "user": os.getenv("POSTGRES_USER", "tfmadmin"),
        "password": os.getenv("POSTGRES_PASSWORD", "localpassword123"),
        "database": os.getenv("POSTGRES_DB", "tfm"),
    }


def _load_aurora_from_env() -> Optional[Dict[str, Any]]:
    load_dotenv(REPO_ROOT / ".env")
    host = os.getenv("AURORA_HOST")
    user = os.getenv("AURORA_USER")
    password = os.getenv("AURORA_PASSWORD")
    auth_mode = os.getenv("AURORA_AUTH_MODE", "").lower()
    secret_name = os.getenv("AURORA_SECRET_NAME", "").strip()

    if secret_name:
        region = os.getenv("AWS_REGION", "eu-north-1")
        return _load_aurora_from_secrets(secret_name, region)

    if not host or not user:
        return None
    if not password and auth_mode != "iam":
        return None
    return {
        "host": host,
        "port": int(os.getenv("AURORA_PORT", "5432")),
        "user": user,
        "password": password,
        "database": os.getenv("AURORA_DBNAME", os.getenv("AURORA_DB", "tfm")),
        "auth_mode": auth_mode,
        "aws_region": os.getenv("AWS_REGION", "eu-north-1"),
    }


def _load_aurora_from_secrets(secret_id: str, region: str) -> Dict[str, Any]:
    import boto3

    client = boto3.client("secretsmanager", region_name=region)
    creds = json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
    logger.info("Credenciales Aurora desde Secrets Manager: %s", secret_id)
    return {
        "host": creds["host"],
        "port": int(creds.get("port", 5432)),
        "user": creds["username"],
        "password": creds.get("password"),
        "database": creds.get("dbname", "tfm"),
        "auth_mode": str(creds.get("auth_mode", "")).lower(),
        "aws_region": region,
    }


def _iam_password(cfg: Dict[str, Any]) -> str:
    import boto3

    rds = boto3.client("rds", region_name=cfg["aws_region"])
    return rds.generate_db_auth_token(
        DBHostname=cfg["host"],
        Port=cfg["port"],
        DBUsername=cfg["user"],
        Region=cfg["aws_region"],
    )


def connect(cfg: Dict[str, Any], label: str):
    password = cfg.get("password")
    if cfg.get("auth_mode") == "iam":
        password = _iam_password(cfg)
    sslmode = cfg.get("sslmode") or (
        "disable" if label == "local" else os.getenv("AURORA_SSLMODE", "require")
    )
    try:
        return psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=password,
            dbname=cfg["database"],
            sslmode=sslmode,
            connect_timeout=20,
        )
    except psycopg2.Error as exc:
        logger.error(
            "Conexión %s fallida (%s:%s): %s",
            label,
            cfg["host"],
            cfg["port"],
            exc,
        )
        if label == "Aurora":
            logger.error(
                "Aurora suele requerir túnel (kubectl/SSM). "
                "Ver scripts/MIGRATE_LOCAL_TO_AURORA.md"
            )
        raise


def table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table,),
        )
        return cur.fetchone() is not None


def count_rows(conn, table: str, where_sql: str = "", params: tuple = ()) -> int:
    with conn.cursor() as cur:
        q = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
        if where_sql:
            q = q + sql.SQL(" ") + sql.SQL(where_sql)
        cur.execute(q, params)
        return int(cur.fetchone()[0])


def list_columns(conn, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


def _date_filter(
    table: str, from_date: Optional[str], to_date: Optional[str]
) -> Tuple[str, tuple]:
    col = TABLE_DATE_COLUMN.get(table)
    if not col or (not from_date and not to_date):
        return "", ()
    clauses, params = [], []
    if from_date:
        clauses.append(f"{col} >= %s")
        params.append(from_date)
    if to_date:
        clauses.append(f"{col} <= %s")
        params.append(to_date)
    return "WHERE " + " AND ".join(clauses), tuple(params)


def truncate_tables(conn, tables: List[str]) -> None:
    with conn.cursor() as cur:
        names = sql.SQL(", ").join(sql.Identifier(t) for t in tables)
        cur.execute(
            sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(names)
        )
    conn.commit()
    logger.info("TRUNCATE completado: %s", ", ".join(tables))


def delete_date_range(conn, table: str, from_date: Optional[str], to_date: Optional[str]) -> int:
    where, params = _date_filter(table, from_date, to_date)
    if not where:
        return 0
    with conn.cursor() as cur:
        q = sql.SQL("DELETE FROM {} ").format(sql.Identifier(table)) + sql.SQL(where)
        cur.execute(q, params)
        deleted = cur.rowcount
    conn.commit()
    return deleted


def _read_local_csv(
    local_conn, table: str, cols: List[str], where_sql: str, params: tuple
) -> io.StringIO:
    buf = io.StringIO()
    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    with local_conn.cursor() as cur:
        if where_sql:
            sub = sql.SQL("SELECT {} FROM {} ").format(
                col_sql, sql.Identifier(table)
            ) + sql.SQL(where_sql)
            copy_q = sql.SQL(
                "COPY ({}) TO STDOUT WITH (FORMAT csv, HEADER true)"
            ).format(sub)
            cur.copy_expert(cur.mogrify(copy_q, params).decode("utf-8"), buf)
        else:
            copy_q = sql.SQL(
                "COPY {} ({}) TO STDOUT WITH (FORMAT csv, HEADER true)"
            ).format(sql.Identifier(table), col_sql)
            cur.copy_expert(copy_q, buf)
    buf.seek(0)
    return buf


def copy_table_replace(
    local_conn,
    aurora_conn,
    table: str,
    cols: List[str],
    from_date: Optional[str],
    to_date: Optional[str],
    dry_run: bool,
) -> Tuple[int, int]:
    where_sql, params = _date_filter(table, from_date, to_date)
    src_n = count_rows(local_conn, table, where_sql, params)
    if src_n == 0:
        logger.info("  [skip] %s: sin filas en local para el filtro", table)
        return 0, 0
    if dry_run:
        logger.info("  [dry-run] %s: copiaría %d filas", table, src_n)
        return src_n, 0

    buf = _read_local_csv(local_conn, table, cols, where_sql, params)
    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    copy_in = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        sql.Identifier(table), col_sql
    )
    with aurora_conn.cursor() as acur:
        acur.copy_expert(copy_in, buf)
    aurora_conn.commit()
    dst_n = count_rows(aurora_conn, table, where_sql, params) if where_sql else count_rows(aurora_conn, table)
    logger.info("  [ok] %s: %d → %d filas (replace/copy)", table, src_n, dst_n)
    return src_n, dst_n


def merge_table(
    local_conn,
    aurora_conn,
    table: str,
    cols: List[str],
    conflict_keys: List[str],
    from_date: Optional[str],
    to_date: Optional[str],
    dry_run: bool,
) -> Tuple[int, int]:
    where_sql, params = _date_filter(table, from_date, to_date)
    src_n = count_rows(local_conn, table, where_sql, params)
    if src_n == 0:
        logger.info("  [skip] %s: sin filas en local", table)
        return 0, 0
    if dry_run:
        logger.info("  [dry-run] %s: haría merge de %d filas", table, src_n)
        return src_n, 0

    staging = f"_staging_{table}"
    buf = _read_local_csv(local_conn, table, cols, where_sql, params)
    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    conflict_sql = sql.SQL(", ").join(sql.Identifier(k) for k in conflict_keys)
    update_cols = [
        c for c in cols if c not in conflict_keys and c != "id"
    ]
    set_clause = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in update_cols
    )

    with aurora_conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(staging), sql.Identifier(table)
            )
        )
        copy_in = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
            sql.Identifier(staging), col_sql
        )
        cur.copy_expert(copy_in, buf)
        if set_clause.as_string(cur):
            upsert = sql.SQL(
                "INSERT INTO {} ({}) SELECT {} FROM {} "
                "ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql.Identifier(table),
                col_sql,
                col_sql,
                sql.Identifier(staging),
                conflict_sql,
                set_clause,
            )
        else:
            upsert = sql.SQL(
                "INSERT INTO {} ({}) SELECT {} FROM {} "
                "ON CONFLICT ({}) DO NOTHING"
            ).format(
                sql.Identifier(table),
                col_sql,
                col_sql,
                sql.Identifier(staging),
                conflict_sql,
            )
        cur.execute(upsert)
        merged = cur.rowcount
    aurora_conn.commit()
    logger.info("  [ok] %s: merge %d filas (rowcount=%d)", table, src_n, merged)
    return src_n, merged


def sync_table(
    local_conn,
    aurora_conn,
    table: str,
    mode: str,
    from_date: Optional[str],
    to_date: Optional[str],
    dry_run: bool,
) -> Tuple[int, int]:
    if not table_exists(local_conn, table):
        logger.warning("  [skip] %s: no existe en local", table)
        return 0, 0
    if not table_exists(aurora_conn, table):
        logger.warning("  [skip] %s: no existe en Aurora — ejecuta migraciones", table)
        return 0, 0

    local_cols = list_columns(local_conn, table)
    aurora_cols = list_columns(aurora_conn, table)
    cols = [c for c in local_cols if c in aurora_cols]
    missing = [c for c in local_cols if c not in aurora_cols]
    if missing:
        logger.warning("  [warn] %s: columnas omitidas en Aurora: %s", table, missing)
    if not cols:
        logger.warning("  [skip] %s: sin columnas comunes", table)
        return 0, 0

    if mode == "merge":
        keys = TABLE_CONFLICT_KEYS.get(table)
        if not keys:
            logger.error("  [skip] %s: sin claves de conflicto definidas", table)
            return 0, 0
        return merge_table(
            local_conn, aurora_conn, table, cols, keys, from_date, to_date, dry_run
        )
    return copy_table_replace(
        local_conn, aurora_conn, table, cols, from_date, to_date, dry_run
    )


def fix_sequences(conn) -> None:
    with conn.cursor() as cur:
        for table in SERIAL_TABLES:
            if not table_exists(conn, table):
                continue
            q = sql.SQL(
                "SELECT setval(pg_get_serial_sequence(%s, 'id'), "
                "COALESCE((SELECT MAX(id) FROM {}), 1), true)"
            ).format(sql.Identifier(table))
            cur.execute(q, (table,))
    conn.commit()
    logger.info("Secuencias SERIAL actualizadas en Aurora")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Upsert datos del bootstrap (Postgres local) hacia Aurora",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Tras bootstrap: subir todo a Aurora (túnel en 15432)
  python scripts/upsert_local_postgres_to_aurora.py --yes

  # Vista previa
  python scripts/upsert_local_postgres_to_aurora.py --dry-run

  # Solo un rango de fechas (merge incremental)
  python scripts/upsert_local_postgres_to_aurora.py --mode merge \\
    --from-date 2026-01-01 --to-date 2026-05-27

  # Credenciales desde Secrets Manager
  python scripts/upsert_local_postgres_to_aurora.py --aurora-secret aurora/credentials --yes
        """,
    )
    p.add_argument(
        "--mode",
        choices=("replace", "merge"),
        default="replace",
        help="replace: TRUNCATE+copy (default post-bootstrap). merge: ON CONFLICT",
    )
    p.add_argument("--tables", default="", help="Tablas separadas por coma (default: todas)")
    p.add_argument("--from-date", help="Filtro batch_date >= YYYY-MM-DD")
    p.add_argument("--to-date", help="Filtro batch_date <= YYYY-MM-DD")
    p.add_argument("--aurora-secret", default="", help="Secrets Manager (override .env)")
    p.add_argument("--aws-region", default=os.getenv("AWS_REGION", "eu-north-1"))
    p.add_argument(
        "--yes",
        action="store_true",
        help="Sin confirmación interactiva (necesario en replace sin rango)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument(
        "--fix-sequences-only",
        action="store_true",
        help="Solo reparar secuencias SERIAL en Aurora",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    _configure_logging(args.verbose)
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or BOOTSTRAP_TABLES

    local_cfg = _load_local_config()
    aurora_cfg = _load_aurora_from_env()
    if args.aurora_secret:
        aurora_cfg = _load_aurora_from_secrets(args.aurora_secret, args.aws_region)
    if not aurora_cfg:
        logger.error(
            "Configura Aurora en .env (AURORA_HOST, AURORA_USER, AURORA_PASSWORD) "
            "o AURORA_SECRET_NAME=aurora/credentials, o usa --aurora-secret"
        )
        return 1

    logger.info("Origen  local : %s:%s/%s", local_cfg["host"], local_cfg["port"], local_cfg["database"])
    logger.info("Destino Aurora: %s:%s/%s", aurora_cfg["host"], aurora_cfg["port"], aurora_cfg["database"])
    logger.info("Modo: %s | Tablas: %s", args.mode, ", ".join(tables))
    if args.from_date or args.to_date:
        logger.info("Rango fechas: %s → %s", args.from_date or "*", args.to_date or "*")

    local_conn = connect(local_cfg, "local")
    aurora_conn = connect(aurora_cfg, "Aurora")

    try:
        if args.fix_sequences_only:
            fix_sequences(aurora_conn)
            return 0

        if args.mode == "replace" and not args.dry_run:
            has_range = bool(args.from_date or args.to_date)
            if has_range:
                if not args.yes:
                    logger.info("Eliminando en Aurora solo el rango indicado…")
                for table in tables:
                    n = delete_date_range(aurora_conn, table, args.from_date, args.to_date)
                    if n:
                        logger.info("  DELETE %s: %d filas", table, n)
            elif not args.yes:
                print(
                    "\n⚠️  Modo replace sin rango: se hará TRUNCATE de:",
                    ", ".join(tables),
                    "\nUsa --yes para confirmar o --from-date/--to-date para parcial.\n",
                )
                if input("Escribe 'yes' para continuar: ").strip().lower() != "yes":
                    print("Cancelado.")
                    return 1
                truncate_tables(aurora_conn, tables)
            else:
                truncate_tables(aurora_conn, tables)

        total_src = total_dst = 0
        for table in tables:
            s, d = sync_table(
                local_conn,
                aurora_conn,
                table,
                args.mode,
                args.from_date,
                args.to_date,
                args.dry_run,
            )
            total_src += s
            total_dst += d

        if not args.dry_run and total_dst > 0:
            fix_sequences(aurora_conn)

        logger.info(
            "Resumen: %d filas leídas | %d afectadas en Aurora%s",
            total_src,
            total_dst,
            " (dry-run)" if args.dry_run else "",
        )
        return 0
    finally:
        local_conn.close()
        aurora_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
