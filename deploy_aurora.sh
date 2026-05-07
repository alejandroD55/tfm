#!/bin/bash
# =============================================================================
# deploy_aurora.sh — Aurora PostgreSQL Serverless v2 para el TFM
# =============================================================================
# Uso:
#   ./deploy_aurora.sh <AWS_REGION> <ACCOUNT_ID> <VPC_ID> \
#                      <SUBNET_IDS> <EKS_SG_ID> [DB_PASSWORD] [--force-express] [ENGINE_VERSION]
#
# Parámetros:
#   AWS_REGION   → Región AWS (ej: eu-north-1)
#   ACCOUNT_ID   → ID de la cuenta AWS
#   VPC_ID       → ID de la VPC donde viven el EKS y las Lambdas
#   SUBNET_IDS   → IDs de subnets privadas separadas por coma
#                  (ej: subnet-aaa,subnet-bbb,subnet-ccc)
#   EKS_SG_ID    → Security Group del EKS (para permitir acceso desde los nodos)
#   DB_PASSWORD  → Contraseña de Aurora (opcional, se genera si no se especifica)
#   --force-express → Obliga creación con WithExpressConfiguration
#   ENGINE_VERSION  → Versión de aurora-postgresql (opcional)
#
# Ejemplo:
#   ./deploy_aurora.sh eu-north-1 123456789012 vpc-0abc1234 \
#     subnet-0aaa,subnet-0bbb,subnet-0ccc sg-0eks12345
#
# Qué hace:
#   1.  Crea DB Subnet Group con las subnets privadas
#   2.  Crea Security Group 'tfm-aurora-sg' (5432 desde EKS + Lambdas)
#   3.  Crea Aurora Serverless v2 cluster (PostgreSQL 15)
#   4.  Crea la instancia writer del cluster
#   5.  Espera a que el cluster esté disponible
#   6.  Crea/actualiza secreto 'aurora/credentials' en Secrets Manager
#   7.  Crea/actualiza secreto 'aurora/endpoint' con el endpoint del cluster
#   8.  Actualiza la configuración VPC de las 5 Lambdas para que accedan a Aurora
#   9.  Imprime el endpoint y las credenciales
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
[ $# -lt 5 ] && error "Uso: ./deploy_aurora.sh <REGION> <ACCOUNT_ID> <VPC_ID> <SUBNET_IDS> <EKS_SG_ID> [DB_PASSWORD] [--force-express] [ENGINE_VERSION]"

AWS_REGION=$1
ACCOUNT_ID=$2
VPC_ID=$3
SUBNET_IDS_RAW=$4         # "subnet-aaa,subnet-bbb,subnet-ccc"
EKS_SG_ID=$5
DB_PASSWORD=${6:-$(openssl rand -base64 16 | tr -dc 'A-Za-z0-9' | head -c 20)}
FORCE_EXPRESS=${7:-}
ENGINE_VERSION_OVERRIDE=${8:-}
USE_EXPRESS=false
DB_PASSWORD=$(echo -n "${DB_PASSWORD}" | tr -d '\r\n\t')

if [ "${DB_PASSWORD}" == "--force-express" ]; then
  DB_PASSWORD=$(openssl rand -base64 16 | tr -dc 'A-Za-z0-9' | head -c 20)
  FORCE_EXPRESS="--force-express"
fi

if [ "${FORCE_EXPRESS}" == "--force-express" ]; then
  USE_EXPRESS=true
fi

if [[ "${FORCE_EXPRESS}" =~ ^[0-9]+\.[0-9]+$ ]] && [ -z "${ENGINE_VERSION_OVERRIDE}" ]; then
  ENGINE_VERSION_OVERRIDE="${FORCE_EXPRESS}"
  FORCE_EXPRESS=""
  USE_EXPRESS=false
fi

# Config Aurora
CLUSTER_ID="tfm-aurora-cluster"
INSTANCE_ID="tfm-aurora-instance"
DB_NAME="tfm"
DB_USER="tfmadmin"
SG_NAME="tfm-aurora-sg"
SUBNET_GROUP_NAME="tfm-aurora-subnet-group"
ENGINE_VERSION="15.4"        # Valor por defecto (se valida y ajusta por región)
MIN_ACU=0.5                  # Serverless v2: escala desde 0.5 ACU (mínimo coste)
MAX_ACU=4                    # Máximo 4 ACU para TFM

# Convertir la lista de subnets en array para AWS CLI
IFS=',' read -ra SUBNET_ARRAY <<< "$SUBNET_IDS_RAW"
SUBNET_CLI_ARGS=$(printf " %s" "${SUBNET_ARRAY[@]}")

resolve_engine_version() {
  local requested="$1"
  local preferred_major="$2"
  local resolved=""

  if [ -n "${requested}" ]; then
    local exists
    exists=$(aws rds describe-db-engine-versions \
      --engine aurora-postgresql \
      --engine-version "${requested}" \
      --region "${AWS_REGION}" \
      --query 'length(DBEngineVersions)' \
      --output text 2>/dev/null || echo "0")
    if [ "${exists}" != "0" ] && [ "${exists}" != "None" ]; then
      echo -n "${requested}" | tr -d '\r\n\t '
      return
    fi
    warn "La versión solicitada '${requested}' no está disponible en ${AWS_REGION}. Se buscará alternativa."
  fi

  resolved=$(aws rds describe-db-engine-versions \
    --engine aurora-postgresql \
    --region "${AWS_REGION}" \
    --query "sort(DBEngineVersions[?starts_with(EngineVersion, '${preferred_major}.')].EngineVersion)[-1]" \
    --output text 2>/dev/null || true)

  if [ -n "${resolved}" ] && [ "${resolved}" != "None" ]; then
    echo -n "${resolved}" | tr -d '\r\n\t '
    return
  fi

  resolved=$(aws rds describe-db-engine-versions \
    --engine aurora-postgresql \
    --region "${AWS_REGION}" \
    --query "sort(DBEngineVersions[].EngineVersion)[-1]" \
    --output text 2>/dev/null || true)

  if [ -n "${resolved}" ] && [ "${resolved}" != "None" ]; then
    echo -n "${resolved}" | tr -d '\r\n\t '
    return
  fi

  error "No se pudo resolver una versión válida de aurora-postgresql en ${AWS_REGION}"
}

if [ -n "${ENGINE_VERSION_OVERRIDE}" ]; then
  ENGINE_VERSION="${ENGINE_VERSION_OVERRIDE}"
fi

ENGINE_VERSION=$(resolve_engine_version "${ENGINE_VERSION}" "15")
ENGINE_VERSION=$(echo -n "${ENGINE_VERSION}" | tr -d '\r\n\t ')

echo ""
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo -e "${CYAN}${BOLD}  TFM — Aurora PostgreSQL Serverless v2 Deployment${NC}"
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo -e "  Región:       ${AWS_REGION}"
echo -e "  VPC:          ${VPC_ID}"
echo -e "  Subnets:      ${SUBNET_IDS_RAW}"
echo -e "  EKS SG:       ${EKS_SG_ID}"
echo -e "  Cluster ID:   ${CLUSTER_ID}"
echo -e "  Engine:       Aurora PostgreSQL ${ENGINE_VERSION}"
echo -e "  Serverless:   ${MIN_ACU} – ${MAX_ACU} ACU"
echo ""

# ── 1. DB Subnet Group ────────────────────────────────────────────────────────
header "1/9 · DB Subnet Group"
if aws rds describe-db-subnet-groups \
     --db-subnet-group-name "${SUBNET_GROUP_NAME}" \
     --region "${AWS_REGION}" > /dev/null 2>&1; then
  info "Subnet group '${SUBNET_GROUP_NAME}' ya existe"
else
  info "Creando subnet group '${SUBNET_GROUP_NAME}'..."
  aws rds create-db-subnet-group \
    --db-subnet-group-name "${SUBNET_GROUP_NAME}" \
    --db-subnet-group-description "Subnet group para Aurora TFM" \
    --subnet-ids ${SUBNET_CLI_ARGS} \
    --tags Key=Project,Value=tfm-trading \
    --region "${AWS_REGION}" > /dev/null
  success "Subnet group creado"
fi

# ── 2. Security Group para Aurora ─────────────────────────────────────────────
header "2/9 · Security Group Aurora"
AURORA_SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
  --query 'SecurityGroups[0].GroupId' \
  --region "${AWS_REGION}" \
  --output text 2>/dev/null || echo "None")

if [ "${AURORA_SG_ID}" == "None" ] || [ -z "${AURORA_SG_ID}" ]; then
  info "Creando security group '${SG_NAME}'..."
  AURORA_SG_ID=$(aws ec2 create-security-group \
    --group-name "${SG_NAME}" \
    --description "Aurora PostgreSQL - acceso desde EKS y Lambdas TFM" \
    --vpc-id "${VPC_ID}" \
    --region "${AWS_REGION}" \
    --query 'GroupId' --output text)
  success "Security Group creado: ${AURORA_SG_ID}"
else
  info "Security group '${SG_NAME}' ya existe: ${AURORA_SG_ID}"
fi

# Regla: PostgreSQL (5432) desde EKS
add_sg_rule() {
  local SRC_SG=$1
  local DESC=$2
  aws ec2 authorize-security-group-ingress \
    --group-id "${AURORA_SG_ID}" \
    --protocol tcp \
    --port 5432 \
    --source-group "${SRC_SG}" \
    --region "${AWS_REGION}" > /dev/null 2>&1 \
    && info "Regla añadida: 5432 desde ${DESC} (${SRC_SG})" \
    || info "Regla ya existe: ${DESC}"
}

add_sg_rule "${EKS_SG_ID}" "EKS nodes"

# ── 3. Cluster Aurora Serverless v2 ───────────────────────────────────────────
header "3/9 · Cluster Aurora Serverless v2"
CLUSTER_EXISTS=$(aws rds describe-db-clusters \
  --db-cluster-identifier "${CLUSTER_ID}" \
  --region "${AWS_REGION}" \
  --query 'DBClusters[0].Status' \
  --output text 2>/dev/null || echo "not-found")

if [ "${CLUSTER_EXISTS}" == "not-found" ]; then
  info "Creando cluster Aurora Serverless v2..."
  CREATE_OUTPUT=""

  if ${USE_EXPRESS}; then
    info "Modo forzado: WithExpressConfiguration"
    aws rds create-db-cluster \
      --db-cluster-identifier "${CLUSTER_ID}" \
      --engine aurora-postgresql \
      --master-username "${DB_USER}" \
      --with-express-configuration \
      --tags Key=Project,Value=tfm-trading Key=ManagedBy,Value=deploy_aurora.sh \
      --region "${AWS_REGION}" > /dev/null
  else
    set +e
    CREATE_OUTPUT=$(aws rds create-db-cluster \
      --db-cluster-identifier "${CLUSTER_ID}" \
      --engine aurora-postgresql \
      --engine-version "${ENGINE_VERSION}" \
      --database-name "${DB_NAME}" \
      --master-username "${DB_USER}" \
      --master-user-password "${DB_PASSWORD}" \
      --db-subnet-group-name "${SUBNET_GROUP_NAME}" \
      --vpc-security-group-ids "${AURORA_SG_ID}" \
      --serverless-v2-scaling-configuration "MinCapacity=${MIN_ACU},MaxCapacity=${MAX_ACU}" \
      --backup-retention-period 1 \
      --preferred-backup-window "03:00-04:00" \
      --preferred-maintenance-window "mon:04:00-mon:05:00" \
      --tags Key=Project,Value=tfm-trading Key=ManagedBy,Value=deploy_aurora.sh \
      --region "${AWS_REGION}" 2>&1)
    CREATE_STATUS=$?
    set -e

    if [ ${CREATE_STATUS} -ne 0 ]; then
      if [[ "${CREATE_OUTPUT}" == *"FreeTierRestrictionError"* ]] || [[ "${CREATE_OUTPUT}" == *"WithExpressConfiguration"* ]]; then
        warn "Cuenta Free detectada: reintentando con WithExpressConfiguration..."
        aws rds create-db-cluster \
          --db-cluster-identifier "${CLUSTER_ID}" \
          --engine aurora-postgresql \
          --master-username "${DB_USER}" \
          --with-express-configuration \
          --tags Key=Project,Value=tfm-trading Key=ManagedBy,Value=deploy_aurora.sh \
          --region "${AWS_REGION}" > /dev/null
        USE_EXPRESS=true
      else
        error "Falló create-db-cluster: ${CREATE_OUTPUT}"
      fi
    fi
  fi
  success "Cluster creado"
else
  info "Cluster '${CLUSTER_ID}' ya existe (estado: ${CLUSTER_EXISTS})"
fi

# ── 4. Instancia writer ───────────────────────────────────────────────────────
header "4/9 · Instancia writer"
if ${USE_EXPRESS}; then
  info "Express configuration crea writer automáticamente; se omite create-db-instance."
  INSTANCE_EXISTS="managed-by-express"
else
MEMBER_COUNT=$(aws rds describe-db-clusters \
  --db-cluster-identifier "${CLUSTER_ID}" \
  --region "${AWS_REGION}" \
  --query 'length(DBClusters[0].DBClusterMembers)' \
  --output text 2>/dev/null || echo "0")

if [ "${MEMBER_COUNT}" != "0" ] && [ "${MEMBER_COUNT}" != "None" ]; then
  info "El cluster ya tiene ${MEMBER_COUNT} instancia(s) asociada(s); se omite create-db-instance."
  INSTANCE_EXISTS="cluster-has-members"
else
INSTANCE_EXISTS=$(aws rds describe-db-instances \
  --db-instance-identifier "${INSTANCE_ID}" \
  --region "${AWS_REGION}" \
  --query 'DBInstances[0].DBInstanceStatus' \
  --output text 2>/dev/null || echo "not-found")

if [ "${INSTANCE_EXISTS}" == "not-found" ]; then
  info "Creando instancia writer (db.serverless)..."
  aws rds create-db-instance \
    --db-instance-identifier "${INSTANCE_ID}" \
    --db-cluster-identifier "${CLUSTER_ID}" \
    --engine aurora-postgresql \
    --db-instance-class db.serverless \
    --region "${AWS_REGION}" > /dev/null
  success "Instancia writer creada"
else
  info "Instancia '${INSTANCE_ID}' ya existe (estado: ${INSTANCE_EXISTS})"
fi
fi
fi

# ── 5. Esperar disponibilidad del cluster ─────────────────────────────────────
header "5/9 · Esperando disponibilidad del cluster"
info "Esto puede tardar 5-10 minutos..."

for i in $(seq 1 40); do
  STATUS=$(aws rds describe-db-clusters \
    --db-cluster-identifier "${CLUSTER_ID}" \
    --region "${AWS_REGION}" \
    --query 'DBClusters[0].Status' \
    --output text 2>/dev/null || echo "creating")

  if [ "${STATUS}" == "available" ]; then
    success "Cluster disponible"
    break
  fi
  echo -ne "\r  Estado: ${STATUS} (${i}/40)..."
  sleep 15
done

CLUSTER_ENDPOINT=$(aws rds describe-db-clusters \
  --db-cluster-identifier "${CLUSTER_ID}" \
  --region "${AWS_REGION}" \
  --query 'DBClusters[0].Endpoint' \
  --output text)

READER_ENDPOINT=$(aws rds describe-db-clusters \
  --db-cluster-identifier "${CLUSTER_ID}" \
  --region "${AWS_REGION}" \
  --query 'DBClusters[0].ReaderEndpoint' \
  --output text)

echo ""
info "Writer endpoint: ${CLUSTER_ENDPOINT}"
info "Reader endpoint: ${READER_ENDPOINT}"

# ── 6. Secrets Manager — aurora/credentials ───────────────────────────────────
header "6/9 · Secrets Manager — aurora/credentials"
if ${USE_EXPRESS}; then
SECRET_VALUE=$(cat <<EOF
{
  "host":     "${CLUSTER_ENDPOINT}",
  "port":     5432,
  "username": "${DB_USER}",
  "dbname":   "${DB_NAME}",
  "auth_mode":"iam"
}
EOF
)
else
SECRET_VALUE=$(cat <<EOF
{
  "host":     "${CLUSTER_ENDPOINT}",
  "port":     5432,
  "username": "${DB_USER}",
  "password": "${DB_PASSWORD}",
  "dbname":   "${DB_NAME}"
}
EOF
)
fi

if aws secretsmanager describe-secret \
     --secret-id "aurora/credentials" \
     --region "${AWS_REGION}" > /dev/null 2>&1; then
  info "Actualizando secreto 'aurora/credentials'..."
  aws secretsmanager update-secret \
    --secret-id "aurora/credentials" \
    --secret-string "${SECRET_VALUE}" \
    --region "${AWS_REGION}" > /dev/null
else
  info "Creando secreto 'aurora/credentials'..."
  aws secretsmanager create-secret \
    --name "aurora/credentials" \
    --description "Credenciales Aurora PostgreSQL — TFM Trading" \
    --secret-string "${SECRET_VALUE}" \
    --region "${AWS_REGION}" > /dev/null
fi
success "Secreto 'aurora/credentials' actualizado"

# ── 7. Secrets Manager — aurora/endpoint ─────────────────────────────────────
header "7/9 · Secrets Manager — aurora/endpoint"
ENDPOINT_SECRET=$(cat <<EOF
{
  "writer": "${CLUSTER_ENDPOINT}",
  "reader": "${READER_ENDPOINT}",
  "port":   5432,
  "dbname": "${DB_NAME}"
}
EOF
)

if aws secretsmanager describe-secret \
     --secret-id "aurora/endpoint" \
     --region "${AWS_REGION}" > /dev/null 2>&1; then
  aws secretsmanager update-secret \
    --secret-id "aurora/endpoint" \
    --secret-string "${ENDPOINT_SECRET}" \
    --region "${AWS_REGION}" > /dev/null
else
  aws secretsmanager create-secret \
    --name "aurora/endpoint" \
    --description "Endpoints Aurora — TFM Trading" \
    --secret-string "${ENDPOINT_SECRET}" \
    --region "${AWS_REGION}" > /dev/null
fi
success "Secreto 'aurora/endpoint' actualizado"

# ── 8. Configurar VPC en las 5 Lambdas ────────────────────────────────────────
header "8/9 · Configurar VPC en las Lambdas"
if ${USE_EXPRESS}; then
  warn "Aurora creado con ExpressConfiguration: se omite forzar VPC en Lambdas."
  warn "El acceso al endpoint dependerá de la conectividad de red saliente de tus Lambdas."
else
# Las Lambdas necesitan estar en la misma VPC para alcanzar Aurora (que es privada)
LAMBDAS=(lambda_ingestion lambda_sentiment lambda_indicators lambda_bayesian lambda_report)

for LAMBDA in "${LAMBDAS[@]}"; do
  if aws lambda get-function \
       --function-name "${LAMBDA}" \
       --region "${AWS_REGION}" > /dev/null 2>&1; then

    info "Configurando VPC en ${LAMBDA}..."

    # Obtener el security group de Lambda (crear uno específico si no existe)
    LAMBDA_SG_NAME="tfm-lambda-sg"
    LAMBDA_SG_ID=$(aws ec2 describe-security-groups \
      --filters "Name=group-name,Values=${LAMBDA_SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
      --query 'SecurityGroups[0].GroupId' \
      --region "${AWS_REGION}" \
      --output text 2>/dev/null || echo "None")

    if [ "${LAMBDA_SG_ID}" == "None" ] || [ -z "${LAMBDA_SG_ID}" ]; then
      info "Creando security group '${LAMBDA_SG_NAME}' para Lambdas..."
      LAMBDA_SG_ID=$(aws ec2 create-security-group \
        --group-name "${LAMBDA_SG_NAME}" \
        --description "SG para Lambdas TFM — acceso a Aurora y HTTPS saliente" \
        --vpc-id "${VPC_ID}" \
        --region "${AWS_REGION}" \
        --query 'GroupId' --output text)
      # Permitir todo el tráfico saliente (para que las Lambdas lleguen a Aurora y APIs externas)
      aws ec2 authorize-security-group-egress \
        --group-id "${LAMBDA_SG_ID}" \
        --protocol -1 --port -1 --cidr "0.0.0.0/0" \
        --region "${AWS_REGION}" > /dev/null 2>&1 || true
    fi

    # Añadir regla en el SG de Aurora para aceptar desde Lambda SG
    add_sg_rule "${LAMBDA_SG_ID}" "Lambdas"

    # Actualizar VPC config de la Lambda
    aws lambda update-function-configuration \
      --function-name "${LAMBDA}" \
      --vpc-config "SubnetIds=${SUBNET_IDS_RAW//,/ },SecurityGroupIds=${LAMBDA_SG_ID}" \
      --region "${AWS_REGION}" > /dev/null

    aws lambda wait function-updated \
      --function-name "${LAMBDA}" \
      --region "${AWS_REGION}"

    success "${LAMBDA} configurada con VPC"
  else
    warn "${LAMBDA} no existe todavía — se configurará en el siguiente deploy_lambdas.sh"
  fi
done
fi

# ── 9. Resumen ────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo -e "${CYAN}${BOLD}  AURORA DESPLEGADO — RESUMEN${NC}"
echo -e "${CYAN}${BOLD}================================================================${NC}"
echo ""
echo -e "  ${BOLD}Cluster:${NC}         ${CLUSTER_ID}"
echo -e "  ${BOLD}Writer endpoint:${NC} ${CLUSTER_ENDPOINT}"
echo -e "  ${BOLD}Reader endpoint:${NC} ${READER_ENDPOINT}"
echo -e "  ${BOLD}Puerto:${NC}          5432"
echo -e "  ${BOLD}Base de datos:${NC}   ${DB_NAME}"
echo -e "  ${BOLD}Usuario:${NC}         ${DB_USER}"
echo -e "  ${BOLD}Security Group:${NC}  ${AURORA_SG_ID}"
echo -e "  ${BOLD}Capacidad:${NC}       ${MIN_ACU}–${MAX_ACU} ACU (Serverless v2)"
echo ""
echo -e "  ${BOLD}Secretos en Secrets Manager:${NC}"
echo -e "    aurora/credentials  → host, port, username, password, dbname"
echo -e "    aurora/endpoint     → writer, reader, port, dbname"
echo ""
if ${USE_EXPRESS}; then
  echo -e "  ${YELLOW}${BOLD}IMPORTANTE:${NC} Express habilita IAM DB Auth por defecto."
  echo -e "  ${BOLD}Autenticación:${NC} IAM (sin password estática en create-db-cluster)"
else
  echo -e "  ${YELLOW}${BOLD}IMPORTANTE:${NC} Guarda esta contraseña en un lugar seguro:"
  echo -e "  ${BOLD}DB Password:${NC} ${DB_PASSWORD}"
fi
echo ""
echo -e "  ${BOLD}Siguiente paso — aplicar el schema:${NC}"
echo -e "  ${CYAN}./deploy_k8s.sh ...${NC}  (incluye el Job de migración automáticamente)"
echo -e "  O manualmente:"
echo -e "  ${CYAN}psql -h ${CLUSTER_ENDPOINT} -U ${DB_USER} -d ${DB_NAME} -f database_schema.sql${NC}"
echo ""
success "Aurora Serverless v2 listo"
