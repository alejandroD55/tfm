#!/usr/bin/env bash
# Wrapper: delega en migrate_postgres_local_to_aurora.py
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY=python3
fi
exec "$PY" "${ROOT}/scripts/upsert_local_postgres_to_aurora.py" "$@"
