#!/bin/bash
# =============================================================================
# deploy_all.sh — Orquestador completo del TFM
# =============================================================================
# Despliega TODO el sistema en el orden correcto:
#
#   Fase 1: Aurora RDS    → BD disponible
#   Fase 2: Lambdas       → funciones de pipeline desplegadas con VPC + alias
#   Fase 3: EKS           → frontend y API en K8s
#   Fase 4: Migración     → schema SQL aplicado contra Aurora
#
# Uso:
#   ./deploy_all.sh \
#     --region     eu-north-1 \
#     --account    123456789012 \
#     --role-arn   arn:aws:iam::123456789012:role/tfm-lambda-role \
#     --vpc-id     vpc-0abc1234 \
#     --subnets    subnet-aaa,subnet-bbb,subnet-ccc \
#     --eks-sg     sg-0eks12345 \
#     --cluster    tfm-eks-cluster \
#     --api-key    mi-api-key-segura \
#     [--skip-aurora]   # si Aurora ya está desplegado
#     [--skip-lambdas]  # si solo quieres actualizar K8s
#     [--skip-k8s]      # si solo quieres actualizar Lambdas
#     [--skip-migrate]  # si el schema ya está aplicado
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "  \033[0;34m[INFO]\033[0m  $1"; }
success() { echo -e "  ${GREEN}[OK]${NC}    $1"; }
warn()    { echo -e "  ${YELLOW}[WARN]${NC}  $1"; }
error()   { echo -e "  ${RED}[ERR]${NC}   $1"; exit 1; }
phase()   { echo -e "\n${CYAN}${BOLD}╔══════════════════════════════════════════╗${NC}"; \
            echo -e "${CYAN}${BOLD}║  $1${NC}"; \
            echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════╝${NC}"; }

# ── Parsear argumentos ────────────────────────────────────────────────────────
AWS_REGION=""
ACCOUNT_ID=""
ROLE_ARN=""
VPC_ID=""
SUBNET_IDS=""
EKS_SG=""
EKS_CLUSTER=""
API_KEY=""
DB_PASSWORD=""
SKIP_AURORA=false
SKIP_LAMBDAS=false
SKIP_K8S=false
SKIP_MIGRATE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --region)      AWS_REGION="$2";    shift 2 ;;
    --account)     ACCOUNT_ID="$2";   shift 2 ;;
    --role-arn)    ROLE_ARN="$2";     shift 2 ;;
    --vpc-id)      VPC_ID="$2";       shift 2 ;;
    --subnets)     SUBNET_IDS="$2";   shift 2 ;;
    --eks-sg)      EKS_SG="$2";       shift 2 ;;
    --cluster)     EKS_CLUSTER="$2";  shift 2 ;;
    --api-key)     API_KEY="$2";      shift 2 ;;
    --db-password) DB_PASSWORD="$2";  shift 2 ;;
    --skip-aurora)   SKIP_AURORA=true;  shift ;;
    --skip-lambdas)  SKIP_LAMBDAS=true; shift ;;
    --skip-k8s)      SKIP_K8S=true;     shift ;;
    --skip-migrate)  SKIP_MIGRATE=true; shift ;;
    *) error "Argumento desconocido: $1" ;;
  esac
done

# Validar argumentos obligatorios
for VAR in AWS_REGION ACCOUNT_ID ROLE_ARN VPC_ID SUBNET_IDS EKS_SG EKS_CLUSTER API_KEY; do
  [ -z "${!VAR}" ] && error "Falta el argumento --$(echo $VAR | tr '_' '-' | tr '[:upper:]' '[:lower:]')"
done

START_TIME=$(date +%s)

echo ""
echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}${BOLD}║   TFM Trading System — Deploy Completo                       ║${NC}"
echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Región:    ${AWS_REGION}  |  Cuenta: ${ACCOUNT_ID}"
echo -e "  VPC:       ${VPC_ID}  |  EKS: ${EKS_CLUSTER}"
echo ""

# ── FASE 1: Aurora ────────────────────────────────────────────────────────────
phase "FASE 1 · Aurora Serverless v2"
if $SKIP_AURORA; then
  warn "Saltando Aurora (--skip-aurora)"
else
  if [ -n "${DB_PASSWORD}" ]; then
    ./deploy_aurora.sh "${AWS_REGION}" "${ACCOUNT_ID}" \
      "${VPC_ID}" "${SUBNET_IDS}" "${EKS_SG}" "${DB_PASSWORD}"
  else
    ./deploy_aurora.sh "${AWS_REGION}" "${ACCOUNT_ID}" \
      "${VPC_ID}" "${SUBNET_IDS}" "${EKS_SG}"
  fi
  success "Fase 1 completada"
fi

# ── FASE 2: Lambdas ───────────────────────────────────────────────────────────
phase "FASE 2 · Lambda Functions (ECR + Versioning)"
if $SKIP_LAMBDAS; then
  warn "Saltando Lambdas (--skip-lambdas)"
else
  ./deploy_lambdas.sh "${AWS_REGION}" "${ACCOUNT_ID}" "${ROLE_ARN}"
  success "Fase 2 completada"
fi

# ── FASE 3: EKS ───────────────────────────────────────────────────────────────
phase "FASE 3 · EKS (Frontend + API pods)"
if $SKIP_K8S; then
  warn "Saltando EKS (--skip-k8s)"
else
  ./deploy_k8s.sh "${AWS_REGION}" "${ACCOUNT_ID}" \
    "${EKS_CLUSTER}" "${API_KEY}"
  success "Fase 3 completada"
fi

# ── FASE 4: Migración schema ──────────────────────────────────────────────────
phase "FASE 4 · Migración schema Aurora"
if $SKIP_MIGRATE; then
  warn "Saltando migración (--skip-migrate)"
else
  ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
  MIGRATE_IMAGE="${ECR_REGISTRY}/tfm-db-migrate:latest"
  NAMESPACE="tfm-dashboard"

  # Build y push de la imagen de migración
  info "Building imagen de migración..."
  docker build \
    --file infrastructure/k8s/jobs/Dockerfile.migrate \
    --tag "${MIGRATE_IMAGE}" \
    . 2>&1 | tail -3
  docker push "${MIGRATE_IMAGE}" > /dev/null
  success "Imagen de migración push completado"

  # Crear Secret de Aurora en K8s (lee el endpoint de Secrets Manager)
  info "Sincronizando credenciales Aurora → K8s Secret..."
  AURORA_CREDS=$(aws secretsmanager get-secret-value \
    --secret-id "aurora/credentials" \
    --region "${AWS_REGION}" \
    --query SecretString --output text)

  AURORA_HOST=$(echo "${AURORA_CREDS}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['host'])")
  AURORA_PORT=$(echo "${AURORA_CREDS}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port',5432))")
  AURORA_USER=$(echo "${AURORA_CREDS}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['username'])")
  AURORA_PASS=$(echo "${AURORA_CREDS}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['password'])")
  AURORA_DB=$(echo "${AURORA_CREDS}"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('dbname','tfm'))")

  kubectl create secret generic tfm-aurora-secret \
    --namespace "${NAMESPACE}" \
    --from-literal=host="${AURORA_HOST}" \
    --from-literal=port="${AURORA_PORT}" \
    --from-literal=username="${AURORA_USER}" \
    --from-literal=password="${AURORA_PASS}" \
    --from-literal=dbname="${AURORA_DB}" \
    --dry-run=client -o yaml | kubectl apply -f -
  success "Secret 'tfm-aurora-secret' aplicado"

  # Eliminar Job anterior si existe (para poder relanzarlo)
  kubectl delete job tfm-db-migrate \
    --namespace "${NAMESPACE}" \
    --ignore-not-found=true > /dev/null

  # Aplicar Job de migración
  info "Lanzando Job de migración..."
  sed \
    -e "s|ACCOUNT_ID.dkr.ecr.AWS_REGION.amazonaws.com/tfm-db-migrate:latest|${MIGRATE_IMAGE}|g" \
    infrastructure/k8s/jobs/db-migrate.yaml \
    | kubectl apply -f -

  # Esperar a que termine el Job
  info "Esperando que el Job termine (timeout: 5min)..."
  kubectl wait job/tfm-db-migrate \
    --namespace "${NAMESPACE}" \
    --for=condition=complete \
    --timeout=300s

  # Mostrar logs
  echo ""
  kubectl logs -n "${NAMESPACE}" -l app=tfm-db-migrate --tail=30
  success "Schema aplicado correctamente"
  success "Fase 4 completada"
fi

# ── Resumen final ─────────────────────────────────────────────────────────────
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}${BOLD}║   DEPLOY COMPLETADO ✓  (${MINUTES}m ${SECONDS}s)${NC}"
echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# URL del dashboard
LB_URL=$(kubectl get service tfm-frontend-service \
  --namespace "tfm-dashboard" \
  --output jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "pendiente")

echo -e "  ${BOLD}Dashboard:${NC}  http://${LB_URL}"
echo -e "  ${BOLD}API docs:${NC}   http://${LB_URL}/api/docs"
echo ""
echo -e "  ${BOLD}Comandos útiles:${NC}"
echo -e "  kubectl get pods -n tfm-dashboard"
echo -e "  kubectl logs -n tfm-dashboard -l app=tfm-api -f"
echo -e "  aws lambda list-aliases --function-name lambda_ingestion --region ${AWS_REGION}"
echo ""
