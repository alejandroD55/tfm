#!/bin/bash
# =============================================================================
# deploy_lambdas.sh — Build, push y versioning de las 5 Lambdas con ECR
# =============================================================================
# Uso:
#   ./deploy_lambdas.sh <AWS_REGION> <ACCOUNT_ID> <ROLE_ARN> [LAMBDA_NAME]
#
# Ejemplos:
#   ./deploy_lambdas.sh eu-north-1 123456789012 arn:aws:iam::123456789012:role/tfm-lambda-role
#   ./deploy_lambdas.sh eu-north-1 123456789012 arn:aws:iam::123456789012:role/tfm-lambda-role lambda_bayesian
#
# Qué hace:
#   1. Crea repositorio ECR para cada Lambda (si no existe)
#   2. Build de la imagen Docker desde infrastructure/lambdas/Dockerfile.{nombre}
#   3. Tag con el SHA del commit y con 'latest'
#   4. Push a ECR
#   5. Actualiza la función Lambda para usar la nueva imagen
#   6. Publica una nueva versión de Lambda (Lambda Versioning)
#   7. Apunta el alias 'live' a esa versión
#   8. Imprime tabla resumen con versiones publicadas
# =============================================================================
set -euo pipefail

# ── Colores ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC}   $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERR]${NC}  $1"; exit 1; }

# ── Argumentos ────────────────────────────────────────────────────────────────
[ $# -lt 3 ] && error "Uso: ./deploy_lambdas.sh <REGION> <ACCOUNT_ID> <ROLE_ARN> [LAMBDA_NAME]"

AWS_REGION=$1
ACCOUNT_ID=$2
ROLE_ARN=$3
FILTER=${4:-"all"}   # 'all' o el nombre de una Lambda concreta

ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ALIAS_NAME="live"
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")

LAMBDAS=(lambda_ingestion lambda_sentiment lambda_indicators lambda_bayesian lambda_report)

get_lambda_memory() {
  case "$1" in
    lambda_ingestion) echo "512" ;;
    lambda_sentiment) echo "512" ;;
    lambda_indicators) echo "512" ;;
    lambda_bayesian) echo "1024" ;;
    lambda_report) echo "512" ;;
    *) error "Lambda desconocida para memoria: $1" ;;
  esac
}

get_lambda_timeout() {
  case "$1" in
    lambda_ingestion|lambda_sentiment|lambda_indicators|lambda_bayesian|lambda_report) echo "300" ;;
    *) error "Lambda desconocida para timeout: $1" ;;
  esac
}

echo ""
echo -e "${CYAN}================================================================${NC}"
echo -e "${CYAN}  TFM Lambda Deploy — Container Images + Versioning + Aliases${NC}"
echo -e "${CYAN}================================================================${NC}"
echo -e "  Región:    ${AWS_REGION}"
echo -e "  Cuenta:    ${ACCOUNT_ID}"
echo -e "  ECR:       ${ECR_REGISTRY}"
echo -e "  Git SHA:   ${GIT_SHA}"
echo -e "  Alias:     ${ALIAS_NAME}"
echo ""

# ── 1. Login en ECR ───────────────────────────────────────────────────────────
info "Autenticando en Amazon ECR..."
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}" > /dev/null
success "Login ECR completado"

# ── Tabla de resultados ───────────────────────────────────────────────────────
RESULT_LINES=()

# ── Función principal por Lambda ──────────────────────────────────────────────
deploy_lambda() {
  local LAMBDA_NAME=$1
  local MEMORY
  local TIMEOUT
  MEMORY=$(get_lambda_memory "${LAMBDA_NAME}")
  TIMEOUT=$(get_lambda_timeout "${LAMBDA_NAME}")
  local DOCKERFILE="infrastructure/lambdas/Dockerfile.${LAMBDA_NAME#lambda_}"
  local ECR_REPO="${ECR_REGISTRY}/${LAMBDA_NAME}"
  local IMAGE_TAG="${ECR_REPO}:${GIT_SHA}"
  local IMAGE_LATEST="${ECR_REPO}:latest"
  local LAMBDA_ARCH="x86_64"

  echo ""
  echo -e "${CYAN}──────────────────────────────────────────────────────${NC}"
  echo -e "${CYAN}  ${LAMBDA_NAME}${NC}"
  echo -e "${CYAN}──────────────────────────────────────────────────────${NC}"

  # ── Crear repo ECR si no existe ─────────────────────────────────────────────
  if ! aws ecr describe-repositories \
       --repository-names "${LAMBDA_NAME}" \
       --region "${AWS_REGION}" > /dev/null 2>&1; then
    info "Creando repositorio ECR '${LAMBDA_NAME}'..."
    aws ecr create-repository \
      --repository-name "${LAMBDA_NAME}" \
      --image-scanning-configuration scanOnPush=true \
      --region "${AWS_REGION}" > /dev/null
    success "Repositorio ECR creado"
  else
    info "Repositorio ECR '${LAMBDA_NAME}' ya existe"
  fi

  # ── Build imagen Docker ──────────────────────────────────────────────────────
  info "Building ${DOCKERFILE} → ${IMAGE_TAG}..."
  local BUILD_LOG
  BUILD_LOG=$(mktemp)
  if ! docker build \
    --file "${DOCKERFILE}" \
    --platform "linux/amd64" \
    --provenance=false \
    --sbom=false \
    --tag "${IMAGE_TAG}" \
    --tag "${IMAGE_LATEST}" \
    --label "git-sha=${GIT_SHA}" \
    --label "lambda-name=${LAMBDA_NAME}" \
    --label "build-date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    . > "${BUILD_LOG}" 2>&1; then
    error "Falló docker build para ${LAMBDA_NAME}. Revisa el log: ${BUILD_LOG}"
  fi
  tail -5 "${BUILD_LOG}"
  rm -f "${BUILD_LOG}"
  success "Imagen construida"

  # ── Push a ECR ───────────────────────────────────────────────────────────────
  info "Push a ECR..."
  docker push "${IMAGE_TAG}" > /dev/null
  docker push "${IMAGE_LATEST}" > /dev/null
  success "Push completado → ${IMAGE_TAG}"

  # ── Crear o actualizar función Lambda ────────────────────────────────────────
  if aws lambda get-function \
       --function-name "${LAMBDA_NAME}" \
       --region "${AWS_REGION}" > /dev/null 2>&1; then

    info "Actualizando código de la función..."
    aws lambda update-function-code \
      --function-name "${LAMBDA_NAME}" \
      --image-uri "${IMAGE_TAG}" \
      --region "${AWS_REGION}" > /dev/null

    info "Esperando que la actualización de código termine..."
    aws lambda wait function-updated \
      --function-name "${LAMBDA_NAME}" \
      --region "${AWS_REGION}"

    info "Actualizando configuración (memoria=${MEMORY}MB, timeout=${TIMEOUT}s)..."
    aws lambda update-function-configuration \
      --function-name "${LAMBDA_NAME}" \
      --memory-size "${MEMORY}" \
      --timeout "${TIMEOUT}" \
      --region "${AWS_REGION}" > /dev/null

    aws lambda wait function-updated \
      --function-name "${LAMBDA_NAME}" \
      --region "${AWS_REGION}"

  else
    info "Creando nueva función Lambda desde imagen ECR..."
    aws lambda create-function \
      --function-name "${LAMBDA_NAME}" \
      --package-type Image \
      --code "ImageUri=${IMAGE_TAG}" \
      --role "${ROLE_ARN}" \
      --architectures "${LAMBDA_ARCH}" \
      --memory-size "${MEMORY}" \
      --timeout "${TIMEOUT}" \
      --region "${AWS_REGION}" > /dev/null

    info "Esperando que la función esté activa..."
    aws lambda wait function-active-v2 \
      --function-name "${LAMBDA_NAME}" \
      --region "${AWS_REGION}"
  fi
  success "Función Lambda actualizada"

  # ── Publicar nueva versión ────────────────────────────────────────────────────
  info "Publicando nueva versión..."
  PUBLISHED_VERSION=$(aws lambda publish-version \
    --function-name "${LAMBDA_NAME}" \
    --description "git=${GIT_SHA} build=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --region "${AWS_REGION}" \
    --query 'Version' \
    --output text)
  success "Versión publicada: ${PUBLISHED_VERSION}"

  # ── Crear o actualizar alias 'live' ───────────────────────────────────────────
  if aws lambda get-alias \
       --function-name "${LAMBDA_NAME}" \
       --name "${ALIAS_NAME}" \
       --region "${AWS_REGION}" > /dev/null 2>&1; then
    info "Actualizando alias '${ALIAS_NAME}' → v${PUBLISHED_VERSION}..."
    aws lambda update-alias \
      --function-name "${LAMBDA_NAME}" \
      --name "${ALIAS_NAME}" \
      --function-version "${PUBLISHED_VERSION}" \
      --description "Desplegado: $(date -u +%Y-%m-%dT%H:%M:%SZ) | git=${GIT_SHA}" \
      --region "${AWS_REGION}" > /dev/null
  else
    info "Creando alias '${ALIAS_NAME}' → v${PUBLISHED_VERSION}..."
    aws lambda create-alias \
      --function-name "${LAMBDA_NAME}" \
      --name "${ALIAS_NAME}" \
      --function-version "${PUBLISHED_VERSION}" \
      --description "Desplegado: $(date -u +%Y-%m-%dT%H:%M:%SZ) | git=${GIT_SHA}" \
      --region "${AWS_REGION}" > /dev/null
  fi
  success "Alias '${ALIAS_NAME}' → arn:...${LAMBDA_NAME}:${ALIAS_NAME} (v${PUBLISHED_VERSION})"

  RESULT_LINES+=("${LAMBDA_NAME}|v${PUBLISHED_VERSION}|${ALIAS_NAME}")
}

# ── Ejecutar despliegues ──────────────────────────────────────────────────────
for LAMBDA in "${LAMBDAS[@]}"; do
  if [ "${FILTER}" == "all" ] || [ "${FILTER}" == "${LAMBDA}" ]; then
    deploy_lambda "${LAMBDA}"
  fi
done

# ── Tabla resumen ─────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}================================================================${NC}"
echo -e "${CYAN}  RESUMEN DE DESPLIEGUE${NC}"
echo -e "${CYAN}================================================================${NC}"
printf "  %-25s %-10s %-10s\n" "LAMBDA" "VERSION" "ALIAS"
printf "  %-25s %-10s %-10s\n" "-------------------------" "-------" "-----"
for RESULT in "${RESULT_LINES[@]}"; do
  LAMBDA_NAME=$(echo "${RESULT}" | cut -d'|' -f1)
  VERSION=$(echo "${RESULT}" | cut -d'|' -f2)
  ALIAS=$(echo "${RESULT}" | cut -d'|' -f3)
  printf "  ${GREEN}%-25s${NC} %-10s %-10s\n" \
    "${LAMBDA_NAME}" "${VERSION}" "${ALIAS}"
done
echo ""
echo -e "  Git SHA:   ${GIT_SHA}"
echo -e "  ECR:       ${ECR_REGISTRY}"
echo ""
echo -e "  Para ver todas las versiones de una función:"
echo -e "  aws lambda list-versions-by-function --function-name lambda_ingestion --region ${AWS_REGION}"
echo ""
success "Despliegue completado"
