#!/bin/bash

# Script de despliegue para las funciones Lambda del TFM
# Uso: ./deploy.sh <AWS_REGION> <ACCOUNT_ID> <ROLE_ARN>

set -e

if [ $# -lt 3 ]; then
    echo "Uso: ./deploy.sh <AWS_REGION> <ACCOUNT_ID> <ROLE_ARN>"
    echo "Ejemplo: ./deploy.sh us-east-1 123456789012 arn:aws:iam::123456789012:role/lambda-execution-role"
    exit 1
fi

AWS_REGION=$1
ACCOUNT_ID=$2
ROLE_ARN=$3

echo "Iniciando despliegue de funciones Lambda en región $AWS_REGION"

# Arrays de lambdas
LAMBDAS=("lambda_ingestion" "lambda_sentiment" "lambda_indicators" "lambda_bayesian" "lambda_report")
MEMORY_SIZES=("512" "512" "512" "1024" "512")
TIMEOUTS=("300" "300" "300" "300" "300")
RUNTIMES=("python3.11" "python3.11" "python3.11" "python3.11" "python3.11")

for i in "${!LAMBDAS[@]}"; do
    LAMBDA_NAME=${LAMBDAS[$i]}
    MEMORY=${MEMORY_SIZES[$i]}
    TIMEOUT=${TIMEOUTS[$i]}
    RUNTIME=${RUNTIMES[$i]}

    echo ""
    echo "=========================================="
    echo "Desplegando $LAMBDA_NAME"
    echo "=========================================="

    # Crear directorio temporal
    TEMP_DIR=$(mktemp -d)
    echo "Directorio temporal: $TEMP_DIR"

    # Copiar código y dependencias
    cp -r $LAMBDA_NAME/* $TEMP_DIR/
    cd $TEMP_DIR

    # Instalar dependencias
    echo "Instalando dependencias..."
    if [ -f requirements.txt ]; then
        pip install -r requirements.txt -t . > /dev/null 2>&1
    fi

    # Crear ZIP
    ZIP_FILE="${LAMBDA_NAME}.zip"
    zip -r -q $ZIP_FILE .

    # Volver al directorio original
    cd -

    # Desplegar o actualizar función
    if aws lambda get-function --function-name $LAMBDA_NAME --region $AWS_REGION > /dev/null 2>&1; then
        echo "Actualizando función existente..."
        aws lambda update-function-code \
            --function-name $LAMBDA_NAME \
            --zip-file fileb://$TEMP_DIR/$ZIP_FILE \
            --region $AWS_REGION > /dev/null

        # Esperar a que la actualización se complete
        sleep 5

        # Actualizar configuración
        aws lambda update-function-configuration \
            --function-name $LAMBDA_NAME \
            --timeout $TIMEOUT \
            --memory-size $MEMORY \
            --runtime $RUNTIME \
            --region $AWS_REGION > /dev/null
    else
        echo "Creando nueva función..."
        aws lambda create-function \
            --function-name $LAMBDA_NAME \
            --runtime $RUNTIME \
            --role $ROLE_ARN \
            --handler ${LAMBDA_NAME}.handler \
            --zip-file fileb://$TEMP_DIR/$ZIP_FILE \
            --timeout $TIMEOUT \
            --memory-size $MEMORY \
            --region $AWS_REGION > /dev/null
    fi

    # Limpiar
    rm -rf $TEMP_DIR

    echo "✓ $LAMBDA_NAME desplegada exitosamente"
done

echo ""
echo "=========================================="
echo "Despliegue completado"
echo "=========================================="
echo ""
echo "Funciones disponibles:"
for LAMBDA_NAME in "${LAMBDAS[@]}"; do
    echo "  - $LAMBDA_NAME"
done

echo ""
echo "Próximos pasos:"
echo "1. Verificar que los secretos existan en Secrets Manager:"
echo "   - aurora/credentials"
echo "   - finnhub/api_key"
echo "2. Crear las tablas en Aurora"
echo "3. Subir etf_universe.json a s3://tfm-config/"
echo "4. Configurar Step Functions para orquestar las lambdas"
