#!/bin/bash

# ═══════════════════════════════════════════════════════════════════════════════
# SCRIPT DE INSTALACIÓN - TFM Trading System
# ═══════════════════════════════════════════════════════════════════════════════
#
# USO:
#   ./install.sh --help
#   ./install.sh --setup-variables
#   ./install.sh --create-infrastructure
#   ./install.sh --deploy-lambdas
#   ./install.sh --run-pipeline
#

set -e

# Colors para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ═══════════════════════════════════════════════════════════════════════════════

print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PASO 1: CARGAR/CREAR VARIABLES
# ═══════════════════════════════════════════════════════════════════════════════

setup_variables() {
    print_header "PASO 1: Configurar Variables de Entorno"

    # Archivo de configuración
    CONFIG_FILE="$HOME/.tfm_config.sh"

    if [ -f "$CONFIG_FILE" ]; then
        print_info "Archivo de configuración encontrado: $CONFIG_FILE"
        read -p "¿Usar las variables existentes? [y/n] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            source $CONFIG_FILE
            print_success "Variables cargadas"
            return
        fi
    fi

    # Solicitar variables
    print_info "Ingresa tus variables de configuración:\n"

    read -p "AWS Account ID: " AWS_ACCOUNT_ID
    read -p "AWS Region [us-east-1]: " AWS_REGION
    AWS_REGION=${AWS_REGION:-us-east-1}

    read -p "Aurora Endpoint: " AURORA_ENDPOINT
    read -p "Aurora User [postgres]: " AURORA_USER
    AURORA_USER=${AURORA_USER:-postgres}
    read -sp "Aurora Password: " AURORA_PASSWORD
    echo
    read -p "Aurora Database [tfm_db]: " AURORA_DATABASE
    AURORA_DATABASE=${AURORA_DATABASE:-tfm_db}

    read -p "Finnhub API Key (obtener en https://finnhub.io): " FINNHUB_API_KEY

    # Crear archivo de configuración
    cat > $CONFIG_FILE << EOF
#!/bin/bash
# TFM Configuration - Auto-generated

export AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID"
export AWS_REGION="$AWS_REGION"
export AWS_PROFILE="default"

export AURORA_ENDPOINT="$AURORA_ENDPOINT"
export AURORA_PORT="5432"
export AURORA_USER="$AURORA_USER"
export AURORA_PASSWORD="$AURORA_PASSWORD"
export AURORA_DATABASE="$AURORA_DATABASE"

export FINNHUB_API_KEY="$FINNHUB_API_KEY"

export LAMBDA_ROLE_NAME="tfm-lambda-execution-role"
export LAMBDA_ROLE_ARN="arn:aws:iam::\${AWS_ACCOUNT_ID}:role/\${LAMBDA_ROLE_NAME}"

export S3_CONFIG_BUCKET="tfm-config"
export S3_DATALAKE_BUCKET="tfm-datalake"

export ETFS="SPY,QQQ,IWM,EEM,VWO,AGG,TLT,GLD,DBC,USO"
EOF

    chmod 600 $CONFIG_FILE
    source $CONFIG_FILE

    print_success "Archivo de configuración creado: $CONFIG_FILE"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PASO 2: CREAR INFRAESTRUCTURA AWS
# ═══════════════════════════════════════════════════════════════════════════════

create_infrastructure() {
    print_header "PASO 2: Crear Infraestructura AWS"

    # Verificar AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI no está instalado"
        exit 1
    fi

    print_info "Verificando credenciales AWS..."
    aws sts get-caller-identity --region $AWS_REGION > /dev/null || {
        print_error "No se puede conectar a AWS. Verifica tus credenciales."
        exit 1
    }
    print_success "Credenciales válidas"

    # Crear IAM Role
    print_info "Creando IAM Role para Lambda..."

    cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

    aws iam create-role \
        --role-name $LAMBDA_ROLE_NAME \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --region $AWS_REGION 2>/dev/null || print_warning "Role ya existe"

    aws iam put-role-policy \
        --role-name $LAMBDA_ROLE_NAME \
        --policy-name tfm-lambda-policy \
        --policy-document file://iam_policy.json \
        --region $AWS_REGION

    print_success "IAM Role creado"

    # Crear Secretos
    print_info "Creando secretos en Secrets Manager..."

    aws secretsmanager create-secret \
        --name aurora/credentials \
        --description "Aurora PostgreSQL credentials" \
        --secret-string "{
            \"host\": \"$AURORA_ENDPOINT\",
            \"port\": 5432,
            \"username\": \"$AURORA_USER\",
            \"password\": \"$AURORA_PASSWORD\",
            \"dbname\": \"$AURORA_DATABASE\"
        }" \
        --region $AWS_REGION 2>/dev/null || print_warning "Secreto Aurora ya existe"

    aws secretsmanager create-secret \
        --name finnhub/api_key \
        --description "Finnhub API key" \
        --secret-string "{\"api_key\": \"$FINNHUB_API_KEY\"}" \
        --region $AWS_REGION 2>/dev/null || print_warning "Secreto Finnhub ya existe"

    print_success "Secretos creados"

    # Crear S3 Buckets
    print_info "Creando buckets S3..."

    aws s3 mb s3://$S3_CONFIG_BUCKET --region $AWS_REGION 2>/dev/null || print_warning "Bucket config ya existe"
    aws s3 mb s3://$S3_DATALAKE_BUCKET --region $AWS_REGION 2>/dev/null || print_warning "Bucket datalake ya existe"

    # Subir configuración
    cat > /tmp/etf_universe.json << EOF
{
  "tickers": $(echo $ETFS | sed 's/,/", "/g' | sed 's/^/["/;s/$/"]/')
}
EOF

    aws s3 cp /tmp/etf_universe.json s3://$S3_CONFIG_BUCKET/etf_universe.json --region $AWS_REGION

    print_success "S3 buckets creados y configurados"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PASO 3: SETUP BASE DE DATOS
# ═══════════════════════════════════════════════════════════════════════════════

setup_database() {
    print_header "PASO 3: Setup Base de Datos Aurora"

    # Verificar psql
    if ! command -v psql &> /dev/null; then
        print_error "psql no está instalado"
        print_info "Instala PostgreSQL client:"
        print_info "  macOS: brew install postgresql"
        print_info "  Ubuntu: sudo apt-get install postgresql-client"
        exit 1
    fi

    print_info "Conectando a Aurora..."
    PGPASSWORD=$AURORA_PASSWORD psql \
        -h $AURORA_ENDPOINT \
        -U $AURORA_USER \
        -d $AURORA_DATABASE \
        -p 5432 \
        -f database_schema.sql 2>/dev/null || {
        print_error "No se pudo conectar a Aurora"
        exit 1
    }

    print_success "Schema de base de datos creado"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PASO 4: DESPLEGAR LAMBDAS
# ═══════════════════════════════════════════════════════════════════════════════

deploy_lambdas() {
    print_header "PASO 4: Desplegar Lambdas"

    LAMBDAS=("lambda_ingestion" "lambda_sentiment" "lambda_indicators" "lambda_bayesian" "lambda_report")

    for LAMBDA in "${LAMBDAS[@]}"; do
        print_info "Desplegando $LAMBDA..."

        cd ${LAMBDA}

        # Empacar
        zip -r ${LAMBDA}.zip ${LAMBDA}.py > /dev/null 2>&1
        pip install -r requirements.txt -t . > /dev/null 2>&1
        zip -r ${LAMBDA}.zip . > /dev/null 2>&1

        # Desplegar
        if [ "$LAMBDA" = "lambda_sentiment" ]; then
            # Usar script especial para FinBERT
            chmod +x deploy_finbert.sh
            ./deploy_finbert.sh $AWS_REGION $AWS_ACCOUNT_ID $LAMBDA_ROLE_ARN
        else
            # Despliegue normal
            aws lambda create-function \
                --function-name $LAMBDA \
                --runtime python3.11 \
                --role $LAMBDA_ROLE_ARN \
                --handler ${LAMBDA}.handler \
                --zip-file fileb://${LAMBDA}.zip \
                --timeout 300 \
                --memory-size 512 \
                --region $AWS_REGION 2>/dev/null || {
                aws lambda update-function-code \
                    --function-name $LAMBDA \
                    --zip-file fileb://${LAMBDA}.zip \
                    --region $AWS_REGION
            }
        fi

        print_success "$LAMBDA desplegada"
        cd ..
    done

    print_success "Todas las lambdas desplegadas"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PASO 5: EJECUTAR PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

run_pipeline() {
    print_header "PASO 5: Ejecutar Pipeline"

    LAMBDAS=("lambda_ingestion" "lambda_sentiment" "lambda_indicators" "lambda_bayesian" "lambda_report")

    for LAMBDA in "${LAMBDAS[@]}"; do
        print_info "Ejecutando $LAMBDA..."

        aws lambda invoke \
            --function-name $LAMBDA \
            --region $AWS_REGION \
            /tmp/${LAMBDA}_response.json

        # Ver resultado
        if grep -q "statusCode.*200" /tmp/${LAMBDA}_response.json; then
            print_success "$LAMBDA completada"
        else
            print_warning "$LAMBDA completada (revisar logs)"
        fi

        # Esperar un poco entre invocaciones
        sleep 2
    done

    print_success "Pipeline completado"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PASO 6: LEER RESULTADOS
# ═══════════════════════════════════════════════════════════════════════════════

read_results() {
    print_header "PASO 6: Leer Resultados"

    FECHA=$(date +%Y-%m-%d)

    # Resultados de Aurora
    print_info "Datos en Aurora:"
    PGPASSWORD=$AURORA_PASSWORD psql \
        -h $AURORA_ENDPOINT \
        -U $AURORA_USER \
        -d $AURORA_DATABASE \
        -c "SELECT ticker, signal, prob_up FROM trading_signals WHERE batch_date = '$FECHA' LIMIT 5;"

    # Reporte de S3
    print_info "Descargando reporte de S3..."
    aws s3 cp s3://$S3_DATALAKE_BUCKET/results/$FECHA/report.json /tmp/report.json --region $AWS_REGION 2>/dev/null

    if [ -f /tmp/report.json ]; then
        print_success "Reporte disponible en /tmp/report.json"
        cat /tmp/report.json | jq '.'
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════════════════════════════

show_help() {
    cat << EOF
${BLUE}TFM Trading System - Installation Script${NC}

${YELLOW}OPCIONES:${NC}

  --help                    Mostrar esta ayuda
  --setup-variables         Crear archivo de configuración
  --create-infrastructure   Crear infraestructura AWS
  --setup-database          Crear schema en Aurora
  --deploy-lambdas          Desplegar las 5 lambdas
  --run-pipeline            Ejecutar el pipeline completo
  --read-results            Leer resultados
  --full                    Ejecutar TODO (default)

${YELLOW}EJEMPLOS:${NC}

  # Instalación completa
  ./install.sh --full

  # Solo configuración
  ./install.sh --setup-variables

  # Desplegar y ejecutar
  ./install.sh --deploy-lambdas
  ./install.sh --run-pipeline

${YELLOW}VARIABLES REQUERIDAS:${NC}

  Antes de ejecutar, configura:
  - AWS_ACCOUNT_ID
  - AWS_REGION
  - AURORA_ENDPOINT
  - FINNHUB_API_KEY

  Se guarda en: ~/.tfm_config.sh

EOF
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    print_header "TFM Trading System - Installation"

    # Default: --full
    MODE=${1:-"--full"}

    case $MODE in
        --help)
            show_help
            ;;
        --setup-variables)
            setup_variables
            ;;
        --create-infrastructure)
            setup_variables
            create_infrastructure
            ;;
        --setup-database)
            setup_variables
            setup_database
            ;;
        --deploy-lambdas)
            setup_variables
            deploy_lambdas
            ;;
        --run-pipeline)
            setup_variables
            run_pipeline
            ;;
        --read-results)
            setup_variables
            read_results
            ;;
        --full)
            setup_variables
            create_infrastructure
            setup_database
            deploy_lambdas
            print_header "INSTALACIÓN COMPLETA"
            print_info "¿Ejecutar pipeline ahora? [y/n]"
            read -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                run_pipeline
                read_results
            fi
            ;;
        *)
            print_error "Opción desconocida: $MODE"
            show_help
            exit 1
            ;;
    esac

    print_header "✓ Listo"
}

main "$@"
