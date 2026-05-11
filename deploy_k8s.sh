#!/bin/bash
# =============================================================================
# deploy_k8s.sh — Build, push y deploy del Dashboard en EKS
# =============================================================================
# Uso:
#   ./deploy_k8s.sh <AWS_REGION> <ACCOUNT_ID> <EKS_CLUSTER> <API_KEY> \
#                   [AWS_ACCESS_KEY_ID] [AWS_SECRET_ACCESS_KEY]
#
# Ejemplos:
#   # Con IRSA (sin credenciales explícitas):
#   ./deploy_k8s.sh eu-north-1 123456789012 tfm-cluster mi-api-key-segura
#
#   # Con credenciales explícitas (si no usas IRSA):
#   ./deploy_k8s.sh eu-north-1 123456789012 tfm-cluster mi-api-key AKIAXX secretXX
#
# Qué hace:
#   1. Crea repos ECR para frontend y api (si no existen)
#   2. Build y push del pod API   (FastAPI)
#   3. Build Angular en producción
#   4. Build y push del pod Frontend (nginx + Angular)
#   5. Conecta kubectl al cluster EKS
#   6. Crea namespace 'tfm-dashboard' (si no existe)
#   7. Aplica ServiceAccount (para IRSA)
#   8. Crea/actualiza el Secret con API Key y credenciales AWS
#   9. Sustituye placeholders en los manifests y los aplica
#  10. Espera a que los pods estén Ready
#  11. Imprime la URL pública del LoadBalancer
# =============================================================================
set -euo pipefail

# ── Colores ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
success() { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
header()  { echo -e "\n${CYAN}${BOLD}── $1 ──${NC}"; }
error()   { echo -e "${RED}[ERR]${NC}   $1"; exit 1; }

# ── Argumentos ────────────────────────────────────────────────────────────────
[ $# -lt 4 ] && error "Uso: ./deploy_k8s.sh <REGION> <ACCOUNT_ID> <EKS_CLUSTER> <API_KEY> [ACCESS_KEY] [SECRET_KEY]"

AWS_REGION=$1
ACCOUNT_ID=$2
EKS_CLUSTER=$3
API_KEY=$4
AWS_ACCESS_KEY_ID_ARG=${5:-""}
AWS_SECRET_ACCESS_KEY_ARG=${6:-""}

ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
API_REPO="tfm-dashboard-api"
FRONTEND_REPO="tfm-dashboard-frontend"
NAMESPACE="tfm-dashboard"
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
API_IMAGE="${ECR_REGISTRY}/${API_REPO}:${GIT_SHA}"
FRONTEND_IMAGE="${ECR_REGISTRY}/${FRONTEND_REPO}:${GIT_SHA}"
K8S_IMAGE_PLATFORM="${K8S_IMAGE_PLATFORM:-linux/amd64}"

echo -e "\n${CYAN}${BOLD}================================================================${NC}"
echo -e "${CYAN}${BOLD}  TFM Dashboard — EKS Deploy${NC}"
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo -e "  Región:   ${AWS_REGION}"
echo -e "  Cuenta:   ${ACCOUNT_ID}"
echo -e "  Cluster:  ${EKS_CLUSTER}"
echo -e "  ECR:      ${ECR_REGISTRY}"
echo -e "  Git SHA:  ${GIT_SHA}"
echo -e "  Platform: ${K8S_IMAGE_PLATFORM}"
echo ""

# ── 1. Login ECR ──────────────────────────────────────────────────────────────
header "1/10 · Login ECR"
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}" > /dev/null
success "Login ECR completado"

# ── Helper: crear repo ECR ────────────────────────────────────────────────────
create_ecr_repo() {
  local REPO=$1
  if ! aws ecr describe-repositories --repository-names "${REPO}" \
       --region "${AWS_REGION}" > /dev/null 2>&1; then
    info "Creando repositorio ECR '${REPO}'..."
    aws ecr create-repository \
      --repository-name "${REPO}" \
      --image-scanning-configuration scanOnPush=true \
      --region "${AWS_REGION}" > /dev/null
    success "Repositorio '${REPO}' creado"
  else
    info "Repositorio ECR '${REPO}' ya existe"
  fi
}

# ── 2. Build y push pod API ───────────────────────────────────────────────────
header "2/10 · Build & Push — Pod API (FastAPI)"
create_ecr_repo "${API_REPO}"

info "Building imagen API → ${API_IMAGE}..."
docker build \
  --platform "${K8S_IMAGE_PLATFORM}" \
  --provenance=false \
  --sbom=false \
  --file infrastructure/k8s/api/Dockerfile \
  --tag "${API_IMAGE}" \
  --tag "${ECR_REGISTRY}/${API_REPO}:latest" \
  --label "git-sha=${GIT_SHA}" \
  --label "component=api" \
  . 2>&1 | tail -5
success "Imagen API construida"

docker push "${API_IMAGE}" > /dev/null
docker push "${ECR_REGISTRY}/${API_REPO}:latest" > /dev/null
success "Push API completado → ${API_IMAGE}"

# ── 3. Build Angular ──────────────────────────────────────────────────────────
header "3/10 · Build Angular (producción)"
ANGULAR_DIR="Sentiment analysis/pipeline-dashboard"

if [ ! -d "${ANGULAR_DIR}/node_modules" ]; then
  info "Instalando dependencias npm..."
  (cd "${ANGULAR_DIR}" && npm ci --prefer-offline --no-audit)
fi

info "Compilando Angular en modo producción..."
(cd "${ANGULAR_DIR}" && npx ng build --configuration production) 2>&1 | tail -10
success "Angular compilado"

# ── 4. Build y push pod Frontend ──────────────────────────────────────────────
header "4/10 · Build & Push — Pod Frontend (nginx)"
create_ecr_repo "${FRONTEND_REPO}"

info "Building imagen Frontend → ${FRONTEND_IMAGE}..."
docker build \
  --platform "${K8S_IMAGE_PLATFORM}" \
  --provenance=false \
  --sbom=false \
  --file infrastructure/k8s/frontend/Dockerfile \
  --tag "${FRONTEND_IMAGE}" \
  --tag "${ECR_REGISTRY}/${FRONTEND_REPO}:latest" \
  --label "git-sha=${GIT_SHA}" \
  --label "component=frontend" \
  . 2>&1 | tail -5
success "Imagen Frontend construida"

docker push "${FRONTEND_IMAGE}" > /dev/null
docker push "${ECR_REGISTRY}/${FRONTEND_REPO}:latest" > /dev/null
success "Push Frontend completado → ${FRONTEND_IMAGE}"

# ── 5. Conectar kubectl al cluster EKS ────────────────────────────────────────
header "5/10 · Configurar kubectl → EKS"
aws eks update-kubeconfig \
  --region "${AWS_REGION}" \
  --name "${EKS_CLUSTER}" > /dev/null
success "kubectl configurado para cluster '${EKS_CLUSTER}'"

# ── 6. Namespace ──────────────────────────────────────────────────────────────
header "6/10 · Namespace '${NAMESPACE}'"
kubectl apply -f infrastructure/k8s/namespace.yaml
success "Namespace listo"

# ── 7. ServiceAccount (IRSA) ──────────────────────────────────────────────────
header "7/10 · ServiceAccount"
# Sustituir ACCOUNT_ID en el serviceaccount
sed "s/ACCOUNT_ID/${ACCOUNT_ID}/g" infrastructure/k8s/serviceaccount.yaml \
  | kubectl apply -f -
success "ServiceAccount aplicado"

# ── 8. Secret con credenciales ────────────────────────────────────────────────
header "8/10 · Secret 'tfm-dashboard-secret'"
kubectl create secret generic tfm-dashboard-secret \
  --namespace "${NAMESPACE}" \
  --from-literal=api-key="${API_KEY}" \
  --from-literal=aws-access-key-id="${AWS_ACCESS_KEY_ID_ARG}" \
  --from-literal=aws-secret-access-key="${AWS_SECRET_ACCESS_KEY_ARG}" \
  --dry-run=client -o yaml \
  | kubectl apply -f -
success "Secret aplicado"

# ── 9. Deployments y Services ─────────────────────────────────────────────────
header "9/10 · Aplicar manifests K8s"

# Obtener ARN de la state machine (si existe) para configurar el pod API
STATE_MACHINE_ARN_VALUE=""
if aws stepfunctions list-state-machines --region "${AWS_REGION}" \
   --query "stateMachines[?name=='tfm-pipeline'].stateMachineArn" \
   --output text 2>/dev/null | grep -q "arn:"; then
  STATE_MACHINE_ARN_VALUE=$(aws stepfunctions list-state-machines \
    --region "${AWS_REGION}" \
    --query "stateMachines[?name=='tfm-pipeline'].stateMachineArn" \
    --output text)
  info "State Machine ARN: ${STATE_MACHINE_ARN_VALUE}"
else
  warn "State Machine 'tfm-pipeline' no encontrada. El trigger de pipeline no funcionara hasta crearla."
fi

# Función que sustituye los placeholders de imagen y ARNs, y aplica
apply_manifest() {
  local FILE=$1
  local IMAGE=$2
  sed \
    -e "s|ACCOUNT_ID.dkr.ecr.AWS_REGION.amazonaws.com/.*|${IMAGE}|g" \
    -e "s|arn:aws:states:eu-north-1:ACCOUNT_ID:stateMachine:tfm-pipeline|${STATE_MACHINE_ARN_VALUE}|g" \
    "${FILE}" | kubectl apply -f -
}

# Pod API
apply_manifest infrastructure/k8s/api/deployment.yaml "${API_IMAGE}"
kubectl apply -f infrastructure/k8s/api/service.yaml
success "Pod API aplicado"

# Pod Frontend
apply_manifest infrastructure/k8s/frontend/deployment.yaml "${FRONTEND_IMAGE}"
kubectl apply -f infrastructure/k8s/frontend/service.yaml
success "Pod Frontend aplicado"

# ── 10. Esperar pods Ready ────────────────────────────────────────────────────
header "10/10 · Esperando pods Ready"

info "Esperando pod API..."
kubectl rollout status deployment/tfm-api \
  --namespace "${NAMESPACE}" --timeout=180s
success "Pod API Ready"

info "Esperando pod Frontend..."
kubectl rollout status deployment/tfm-frontend \
  --namespace "${NAMESPACE}" --timeout=180s
success "Pod Frontend Ready"

# ── Obtener URL pública ────────────────────────────────────────────────────────
echo ""
info "Obteniendo URL del LoadBalancer (puede tardar 1-2 min en propagarse)..."
LB_URL=""
for i in $(seq 1 20); do
  LB_URL=$(kubectl get service tfm-frontend-service \
    --namespace "${NAMESPACE}" \
    --output jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
  [ -n "${LB_URL}" ] && break
  echo -n "."
  sleep 10
done
echo ""

# ── Resumen final ──────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo -e "${CYAN}${BOLD}  DESPLIEGUE COMPLETADO${NC}"
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo ""
echo -e "  ${BOLD}Pods desplegados:${NC}"
kubectl get pods --namespace "${NAMESPACE}" \
  --no-headers \
  -o custom-columns="  POD:.metadata.name,ESTADO:.status.phase,LISTO:.status.conditions[-1].status"
echo ""
echo -e "  ${BOLD}Servicios:${NC}"
kubectl get services --namespace "${NAMESPACE}" --no-headers
echo ""

if [ -n "${LB_URL}" ]; then
  echo -e "  ${GREEN}${BOLD}Dashboard URL:${NC}   http://${LB_URL}"
  echo -e "  ${GREEN}${BOLD}API health:${NC}      http://${LB_URL}/api/health"
  echo -e "  ${GREEN}${BOLD}API reports:${NC}     http://${LB_URL}/api/reports  (header: x-api-key: ${API_KEY})"
else
  warn "LoadBalancer aún sin DNS asignado. Ejecuta en un momento:"
  echo -e "  kubectl get service tfm-frontend-service -n ${NAMESPACE}"
fi

echo ""
echo -e "  ${BOLD}Git SHA:${NC}  ${GIT_SHA}"
echo -e "  ${BOLD}API Image:${NC}       ${API_IMAGE}"
echo -e "  ${BOLD}Frontend Image:${NC}  ${FRONTEND_IMAGE}"
echo ""
echo -e "  ${BOLD}Comandos útiles:${NC}"
echo -e "  kubectl get pods -n ${NAMESPACE} -w"
echo -e "  kubectl logs -n ${NAMESPACE} -l app=tfm-api --tail=50 -f"
echo -e "  kubectl logs -n ${NAMESPACE} -l app=tfm-frontend --tail=50 -f"
echo ""
success "Deploy EKS completado"
