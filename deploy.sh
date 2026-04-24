#!/bin/bash

# Script de despliegue para las funciones Lambda del TFM
set -e

if [ $# -lt 3 ]; then
    echo "Uso: ./deploy.sh <AWS_REGION> <ACCOUNT_ID> <ROLE_ARN>"
    exit 1
fi

AWS_REGION=$1
ACCOUNT_ID=$2
ROLE_ARN=$3
S3_BUCKET="tfm-unir-config"

echo "Iniciando despliegue de funciones Lambda en región $AWS_REGION usando S3 ($S3_BUCKET)..."

LAMBDAS=("lambda_ingestion" "lambda_sentiment" "lambda_indicators" "lambda_bayesian" "lambda_report")
MEMORY_SIZES=("512" "512" "512" "1024" "512")
TIMEOUTS=("300" "300" "300" "300" "300")
RUNTIMES=("python3.9" "python3.9" "python3.9" "python3.9" "python3.9")

for i in "${!LAMBDAS[@]}"; do
    LAMBDA_NAME=${LAMBDAS[$i]}
    MEMORY=${MEMORY_SIZES[$i]}
    TIMEOUT=${TIMEOUTS[$i]}
    RUNTIME=${RUNTIMES[$i]}

    echo ""
    echo "=========================================="
    echo "Desplegando $LAMBDA_NAME"
    echo "=========================================="

    TEMP_DIR=$(mktemp -d)
    echo "Directorio temporal: $TEMP_DIR"

    cp -r $LAMBDA_NAME/* $TEMP_DIR/
    cd $TEMP_DIR

    echo "Instalando dependencias nativas..."

    if [ -f requirements.txt ]; then
        # --- LA CIRUGÍA MAESTRA PARA LAMBDA BAYESIANA ---
        if [ "$LAMBDA_NAME" == "lambda_bayesian" ]; then
            echo "Aplicando instalación estricta para lambda_bayesian..."
            # Instalamos las librerías normales (AÑADIMOS PANDAS, SCIPY, NETWORKX, JOBLIB, TQDM Y PYPARSING)
            pip install psycopg2-binary "numpy<2.0" networkx scipy pandas joblib tqdm pyparsing -t . --no-cache-dir
            # Instalamos pgmpy SIN SUS DEPENDENCIAS para evitar descargar PyTorch
            pip install pgmpy==0.1.19 -t . --no-deps --no-cache-dir

            # Borramos librerías de AWS que ya vienen preinstaladas en Lambda
            rm -rf boto3* botocore* s3transfer* urllib3* jmespath*
            # Borramos librerías pesadas que no usamos
            rm -rf sklearn* scikit_learn* statsmodels* patsy*
            rm -rf torch* torchvision* torchaudio* triton* nvidia*

            # LIPOSUCCIÓN EXTREMA: Borrar carpetas de pruebas de Pandas y Scipy (ahorra +50MB)
            find . -type d -name "tests" -exec rm -rf {} +
            find . -type d -name "__pycache__" -exec rm -rf {} +
            echo "Tamaño tras limpieza:"
            du -sh .
        else
            pip install -r requirements.txt -t . --no-cache-dir
        fi
    fi

    ZIP_FILE="${LAMBDA_NAME}.zip"
    zip -r -q $ZIP_FILE .

    echo "Subiendo $ZIP_FILE a S3..."
    aws s3 cp $ZIP_FILE s3://$S3_BUCKET/$ZIP_FILE > /dev/null

    cd - > /dev/null

    if aws lambda get-function --function-name $LAMBDA_NAME --region $AWS_REGION > /dev/null 2>&1; then
        echo "Actualizando función existente..."
        aws lambda update-function-code \
            --function-name $LAMBDA_NAME \
            --s3-bucket $S3_BUCKET \
            --s3-key $ZIP_FILE \
            --region $AWS_REGION > /dev/null

        aws lambda wait function-updated --function-name $LAMBDA_NAME --region $AWS_REGION

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
            --code S3Bucket=$S3_BUCKET,S3Key=$ZIP_FILE \
            --timeout $TIMEOUT \
            --memory-size $MEMORY \
            --region $AWS_REGION > /dev/null
            
        aws lambda wait function-active-v2 --function-name $LAMBDA_NAME --region $AWS_REGION
    fi

    echo "Limpiando S3 y temporales..."
    aws s3 rm s3://$S3_BUCKET/$ZIP_FILE > /dev/null
    rm -rf $TEMP_DIR

    echo "✓ $LAMBDA_NAME desplegada exitosamente"
done

echo ""
echo "=========================================="
echo "Despliegue completado"
echo "=========================================="
