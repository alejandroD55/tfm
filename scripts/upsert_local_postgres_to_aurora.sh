#!/usr/bin/env bash
# Upsert post-bootstrap: Postgres local (Docker) → Aurora AWS
# Uso típico tras bootstrap_365_days.py:
#   ./scripts/upsert_local_postgres_to_aurora.sh --yes
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY=python3
exec "$PY" "${ROOT}/scripts/upsert_local_postgres_to_aurora.py" "$@"
