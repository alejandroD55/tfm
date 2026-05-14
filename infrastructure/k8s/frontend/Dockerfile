# ─── TFM Dashboard Frontend ───────────────────────────────────────────────────
# Multistage build:
#   Stage 1 (builder) : Node.js compila Angular en modo producción
#   Stage 2 (runtime) : nginx sirve los assets + proxea /api/ al pod API
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Build Angular ────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Copiar manifiestos de dependencias primero (mejor cache de capas)
COPY ["Sentiment analysis/pipeline-dashboard/package.json", "./"]
COPY ["Sentiment analysis/pipeline-dashboard/package-lock.json", "./"]

RUN npm cache clean --force && npm ci --no-audit

# Copiar el resto del proyecto Angular
COPY ["Sentiment analysis/pipeline-dashboard/", "./"]

# Build de producción
# --configuration production activa AOT, tree-shaking y minificación
RUN npx ng build --configuration production --output-path /dist

# ── Stage 2: nginx runtime ────────────────────────────────────────────────────
FROM nginx:1.25-alpine AS runtime

LABEL maintainer="tfm-trading" \
      component="frontend" \
      description="TFM Dashboard Angular + nginx reverse proxy"

# Copiar assets compilados
COPY --from=builder /dist/browser /usr/share/nginx/html

# Copiar configuración de nginx
COPY infrastructure/k8s/frontend/nginx.conf /etc/nginx/nginx.conf

# Eliminar config por defecto de nginx
RUN rm -f /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -O- http://localhost/nginx-health || exit 1

CMD ["nginx", "-g", "daemon off;"]
