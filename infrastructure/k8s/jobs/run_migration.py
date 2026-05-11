"""
run_migration.py
================
Runner de migraciones SQL versionadas para Aurora PostgreSQL.

- Ejecuta solo migraciones pendientes en /migration/migrations
- Registra cada ejecución en la tabla __migrations
- Soporta autenticación por password o IAM token
"""

import os
import json
import subprocess
import sys
import logging
import boto3
import hashlib
from pathlib import Path
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

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


def _log_iam_token_failure(exc):
    """IRSA / STS errors when signing RDS IAM auth token."""
    err = getattr(exc, "response", {}).get("Error", {}) if isinstance(exc, ClientError) else {}
    code = err.get("Code", "")
    msg = err.get("Message", str(exc))
    logger.error("Fallo al obtener credenciales AWS para IAM DB auth: %s — %s", code or type(exc).__name__, msg)
    if code == "InvalidIdentityToken" or "OpenIDConnect provider" in msg:
        logger.error(
            "IAM no tiene registrado el proveedor OIDC del issuer del cluster EKS "
            "(o el cluster se recreó y el issuer cambió). Asocie el proveedor OIDC al cluster, "
            "p. ej.: eksctl utils associate-iam-oidc-provider --cluster <nombre> "
            "--region eu-north-1 --approve"
        )
        logger.error(
            "Revise también que el rol del ServiceAccount (eks.amazonaws.com/role-arn) "
            "confíe en el issuer actual del cluster."
        )
    if code == "AccessDenied" and "AssumeRoleWithWebIdentity" in msg:
        logger.error(
            "La trust policy del rol IRSA no acepta este pod: suelen fallar :sub distinto "
            "(debe ser system:serviceaccount:tfm-dashboard:tfm-api-sa para el Job actual), "
            ":aud distinto de sts.amazonaws.com, o Principal.Federated que no coincide "
            "con el ARN del OIDC provider del cluster. Compare con: "
            "aws iam get-role --role-name <rol> --query Role.AssumeRolePolicyDocument"
        )
    logger.error(
        "Alternativa: en el Secret de Aurora quite auth_mode=iam y defina password "
        "para que el Job de migración use contraseña estática."
    )


def build_psql_env(host, port, user, password, auth_mode):
    """Build env for psql. Supports static password or IAM token."""
    env = {**os.environ}
    if auth_mode == "iam":
        region = os.getenv("AWS_REGION", "eu-north-1")
        try:
            token = boto3.client("rds", region_name=region).generate_db_auth_token(
                DBHostname=host,
                Port=int(port),
                DBUsername=user,
                Region=region,
            )
        except (ClientError, BotoCoreError, NoCredentialsError) as e:
            _log_iam_token_failure(e)
            sys.exit(1)
        env["PGPASSWORD"] = token
    else:
        if not password:
            logger.error("AURORA_PASSWORD no disponible y auth_mode no es IAM.")
            sys.exit(1)
        env["PGPASSWORD"] = password
    return env


def run_psql(host, port, user, dbname, password, auth_mode, sql=None, file_path=None, capture_output=True):
    env = build_psql_env(host, port, user, password, auth_mode)
    cmd = [
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
    ]
    if sql is not None:
        cmd += ["-c", sql]
    if file_path is not None:
        cmd += ["-f", str(file_path)]

    return subprocess.run(
        cmd,
        env=env,
        capture_output=capture_output,
        text=True,
    )


def ensure_migrations_table(host, port, user, dbname, password, auth_mode):
    sql = """
    CREATE TABLE IF NOT EXISTS __migrations (
        id BIGSERIAL PRIMARY KEY,
        filename TEXT NOT NULL UNIQUE,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    res = run_psql(host, port, user, dbname, password, auth_mode, sql=sql)
    if res.returncode != 0:
        logger.error(f"No se pudo crear tabla __migrations:\n{res.stderr}")
        sys.exit(1)


def load_applied_migrations(host, port, user, dbname, password, auth_mode):
    res = run_psql(
        host,
        port,
        user,
        dbname,
        password,
        auth_mode,
        sql="SELECT filename, checksum FROM __migrations ORDER BY filename;",
        capture_output=True,
    )
    if res.returncode != 0:
        logger.error(f"No se pudieron leer migraciones aplicadas:\n{res.stderr}")
        sys.exit(1)

    applied = {}
    for line in (res.stdout or "").splitlines():
        if "|" not in line:
            continue
        filename, checksum = line.split("|", 1)
        applied[filename.strip()] = checksum.strip()
    return applied


def list_migration_files():
    migrations_dir = Path("/migration/migrations")
    if not migrations_dir.exists():
        logger.error(f"Directorio de migraciones no encontrado: {migrations_dir}")
        sys.exit(1)
    return sorted(migrations_dir.glob("*.sql"))


def sha256_of_file(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def mark_migration_applied(host, port, user, dbname, password, auth_mode, filename, checksum):
    safe_filename = filename.replace("'", "''")
    safe_checksum = checksum.replace("'", "''")
    sql = (
        "INSERT INTO __migrations (filename, checksum) "
        f"VALUES ('{safe_filename}', '{safe_checksum}') "
        "ON CONFLICT (filename) DO UPDATE SET checksum = EXCLUDED.checksum, applied_at = CURRENT_TIMESTAMP;"
    )
    res = run_psql(host, port, user, dbname, password, auth_mode, sql=sql)
    if res.returncode != 0:
        logger.error(f"No se pudo registrar migración {filename}:\n{res.stderr}")
        sys.exit(1)


def run_migrations():
    host, port, user, password, dbname, auth_mode = get_credentials()
    logger.info(
        f"Conectando a Aurora: {host}:{port}/{dbname} como '{user}' "
        f"(auth_mode={auth_mode or 'password'})"
    )

    check = run_psql(
        host, port, user, dbname, password, auth_mode, sql="SELECT version();"
    )
    if check.returncode != 0:
        logger.error(f"No se puede conectar a Aurora:\n{check.stderr}")
        sys.exit(1)
    logger.info("Conexión a Aurora verificada")

    ensure_migrations_table(host, port, user, dbname, password, auth_mode)
    applied = load_applied_migrations(host, port, user, dbname, password, auth_mode)
    migration_files = list_migration_files()

    if not migration_files:
        logger.warning("No hay migraciones en /migration/migrations")
        return

    pending = []
    for mig_file in migration_files:
        filename = mig_file.name
        checksum = sha256_of_file(mig_file)
        if filename not in applied:
            pending.append((mig_file, checksum))
            continue
        if applied[filename] != checksum:
            logger.error(
                f"Checksum cambiado para migración ya aplicada: {filename}. "
                "No es seguro continuar."
            )
            sys.exit(1)

    if not pending:
        logger.info("No hay migraciones pendientes.")
        return

    logger.info(f"Migraciones pendientes: {len(pending)}")
    for mig_file, checksum in pending:
        logger.info(f"Aplicando migración: {mig_file.name}")
        res = run_psql(
            host,
            port,
            user,
            dbname,
            password,
            auth_mode,
            file_path=mig_file,
            capture_output=True,
        )
        if res.stdout:
            logger.info(res.stdout)
        if res.stderr:
            logger.warning(res.stderr)
        if res.returncode != 0:
            logger.error(f"Error aplicando migración {mig_file.name}")
            sys.exit(1)

        mark_migration_applied(
            host, port, user, dbname, password, auth_mode, mig_file.name, checksum
        )
        logger.info(f"Migración aplicada: {mig_file.name}")

    logger.info("Migraciones completadas correctamente.")


if __name__ == "__main__":
    run_migrations()
