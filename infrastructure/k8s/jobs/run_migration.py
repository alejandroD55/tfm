"""
run_migration.py
================
Ejecuta database_schema.sql contra Aurora PostgreSQL.
Lee las credenciales de:
  1. Variables de entorno (K8s Secret montado como env vars)
  2. AWS Secrets Manager como fallback

Se usa como K8s Job — se ejecuta una vez y termina.
"""

import os
import json
import subprocess
import sys
import logging
import boto3

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("migration")


def get_credentials():
    """Obtiene las credenciales Aurora desde env vars o Secrets Manager."""

    # ── Opción 1: variables de entorno (K8s Secret) ─────────────────────────
    host = os.getenv("AURORA_HOST")
    port = os.getenv("AURORA_PORT", "5432")
    user = os.getenv("AURORA_USER")
    password = os.getenv("AURORA_PASSWORD")
    dbname = os.getenv("AURORA_DBNAME", "tfm")
    auth_mode = str(os.getenv("AURORA_AUTH_MODE", "")).lower()

    if all([host, user]) and (password or auth_mode == "iam"):
        logger.info("Credenciales cargadas desde variables de entorno")
        return host, port, user, password, dbname, auth_mode

    # ── Opción 2: AWS Secrets Manager ────────────────────────────────────────
    secret_name = os.getenv("AURORA_SECRET_NAME", "aurora/credentials")
    region = os.getenv("AWS_REGION", "eu-north-1")

    logger.info(f"Cargando credenciales desde Secrets Manager ({secret_name})...")
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    creds = json.loads(response["SecretString"])

    return (
        creds["host"],
        str(creds.get("port", 5432)),
        creds["username"],
        creds.get("password", ""),
        creds.get("dbname", "tfm"),
        str(creds.get("auth_mode", "")).lower(),
    )


def build_psql_env(host, port, user, password, auth_mode):
    """Build env for psql. Supports static password or IAM token."""
    env = {**os.environ}
    if auth_mode == "iam":
        region = os.getenv("AWS_REGION", "eu-north-1")
        token = boto3.client("rds", region_name=region).generate_db_auth_token(
            DBHostname=host,
            Port=int(port),
            DBUsername=user,
            Region=region,
        )
        env["PGPASSWORD"] = token
    else:
        if not password:
            logger.error("AURORA_PASSWORD no disponible y auth_mode no es IAM.")
            sys.exit(1)
        env["PGPASSWORD"] = password
    return env


def run_migration():
    schema_path = "/migration/database_schema.sql"
    if not os.path.exists(schema_path):
        logger.error(f"Schema no encontrado: {schema_path}")
        sys.exit(1)

    host, port, user, password, dbname, auth_mode = get_credentials()

    logger.info(f"Conectando a Aurora: {host}:{port}/{dbname} como '{user}' (auth_mode={auth_mode or 'password'})")

    env = build_psql_env(host, port, user, password, auth_mode)

    # ── Verificar conectividad ──────────────────────────────────────────────
    check = subprocess.run(
        [
            "psql",
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            dbname,
            "-c",
            "SELECT version();",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    if check.returncode != 0:
        logger.error(f"No se puede conectar a Aurora:\n{check.stderr}")
        sys.exit(1)
    logger.info("Conexión a Aurora verificada")

    # ── Ejecutar schema SQL ─────────────────────────────────────────────────
    logger.info(f"Aplicando schema: {schema_path}")
    # Renovamos token IAM por si hubo demora entre pasos
    env = build_psql_env(host, port, user, password, auth_mode)
    result = subprocess.run(
        [
            "psql",
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            dbname,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            schema_path,
        ],
        env=env,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(f"Output:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"Warnings/info:\n{result.stderr}")

    if result.returncode != 0:
        logger.error("Error aplicando el schema")
        sys.exit(1)

    logger.info("Schema aplicado correctamente")

    # ── Verificar tablas creadas ────────────────────────────────────────────
    env = build_psql_env(host, port, user, password, auth_mode)
    verify = subprocess.run(
        [
            "psql",
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            dbname,
            "-c",
            r"\dt",
            "--no-psqlrc",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    if verify.returncode == 0:
        logger.info(f"Tablas en la base de datos:\n{verify.stdout}")

    logger.info("Migración completada con éxito")


if __name__ == "__main__":
    run_migration()
