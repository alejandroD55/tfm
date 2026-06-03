#!/usr/bin/env bash
# Arranca la API FastAPI en local leyendo tfm/.env (Mongo Atlas + claves).
# Uso: desde la raíz del repo tfm/
#   ./scripts/run_api_local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
  echo "✓ Variables cargadas desde $ROOT/.env"
else
  echo "⚠ No existe $ROOT/.env — copia .env.example y rellena MONGODB_URI y DASHBOARD_API_KEY"
fi

: "${DASHBOARD_API_KEY:=25aded11b15417a5580f631e432efad66848df1fa2f620e94d26d6b588486431"
: "${MONGODB_DB:=tfm}"
: "${POSTGRES_HOST:=localhost}"
: "${POSTGRES_PORT:=5432}"

if [[ -z "${MONGODB_URI:-}" ]]; then
  echo "✗ MONGODB_URI no definida. Añádela en .env (cadena mongodb+srv:// de Atlas)."
  exit 1
fi

export DASHBOARD_API_KEY MONGODB_URI MONGODB_DB
export POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

API_DIR="$ROOT/infrastructure/k8s/api"
export PYTHONPATH="$ROOT/shared:${API_DIR}:${PYTHONPATH:-}"

if ! python3 -c "import uvicorn, fastapi, pymongo" 2>/dev/null; then
  echo "Instalando dependencias API..."
  python3 -m pip install -q -r "$API_DIR/requirements.txt"
fi

echo "→ API http://localhost:8000  |  Mongo: Atlas  |  PG (bootstrap): ${POSTGRES_HOST}:${POSTGRES_PORT}"
echo "→ En otro terminal: cd 'Sentiment analysis/pipeline-dashboard' && npm run start:dev"
cd "$API_DIR"
exec python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
