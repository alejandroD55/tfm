# ─── TFM Dashboard API ────────────────────────────────────────────────────────
# Pod EKS: FastAPI que sirve los datos del datalake S3
FROM python:3.11-slim AS base

WORKDIR /app

# Dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Instalar dependencias Python ──────────────────────────────────────────────
COPY infrastructure/k8s/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copiar código fuente ───────────────────────────────────────────────────────
COPY infrastructure/k8s/api/main.py .

# ── Usuario no-root ────────────────────────────────────────────────────────────
RUN useradd -m -u 1001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# ── Health check ───────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
