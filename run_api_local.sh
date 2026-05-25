#!/usr/bin/env bash
# =============================================================================
# run_api_local.sh — Levanta la FastAPI en local con las variables del .env
# =============================================================================
# Uso:
#   chmod +x run_api_local.sh
#   ./run_api_local.sh
#
# Después levanta el frontal con:
#   cd "Sentiment analysis/pipeline-dashboard"
#   npm start          # usa proxy.conf.json → redirige /api/ a localhost:8000
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
API_DIR="$SCRIPT_DIR/infrastructure/k8s/api"
SHARED_DIR="$SCRIPT_DIR/shared"

# ── Cargar variables del .env ─────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
  echo "📦 Cargando variables desde .env"
  set -o allexport
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +o allexport
else
  echo "⚠️  No se encontró .env en $SCRIPT_DIR"
fi

# ── Asegurarse de que mongo_utils está en PYTHONPATH ─────────────────────────
export PYTHONPATH="$SHARED_DIR:${PYTHONPATH:-}"

# ── Directorio de trabajo = carpeta de la API (para que importe mongo_utils) ──
cd "$API_DIR"
# Copiar mongo_utils.py y etf_universe.json donde la API los espera
cp "$SHARED_DIR/mongo_utils.py" .
cp "$SCRIPT_DIR/etf_universe.json" .

echo ""
echo "🚀 Arrancando FastAPI en http://localhost:8000"
echo "   Swagger UI → http://localhost:8000/docs"
echo ""

# ── Usar el venv del TFM si existe, sino el Python del sistema ───────────────
VENV="$SCRIPT_DIR/.venv"
if [[ -f "$VENV/bin/uvicorn" ]]; then
  UVICORN="$VENV/bin/uvicorn"
else
  UVICORN="uvicorn"
fi

exec "$UVICORN" main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --reload \
  --log-level info
