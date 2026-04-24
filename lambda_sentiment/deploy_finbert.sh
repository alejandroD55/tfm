#!/bin/bash

# Script de deployment específico para lambda_sentiment con FinBERT
# El modelo de FinBERT es pesado (~400MB), así que necesitamos usar un Layer

set -e

if [ $# -lt 3 ]; then
    echo "Uso: ./deploy_finbert.sh <AWS_REGION> <ACCOUNT_ID> <ROLE_ARN>"
    echo "Ejemplo: ./deploy_finbert.sh us-east-1 123456789012 arn:aws:iam::123456789012:role/lambda-execution-role"
    exit 1
fi

AWS_REGION=$1
ACCOUNT_ID=$2
ROLE_ARN=$3

LAMBDA_NAME="lambda_sentiment"

echo "=========================================="
echo "Desplegando $LAMBDA_NAME con FinBERT"
echo "=========================================="
echo ""
echo "⚠️  IMPORTANTE: Este script va a:"
echo "   1. Crear un Layer con transformers + torch"
echo "   2. Descargar el modelo FinBERT (~400MB)"
echo "   3. Compilar con arquitectura arm64 (Lambda usar arm64)"
echo ""
echo "Esto puede tomar 5-10 minutos la primera vez."
echo ""

# Step 1: Crear Layer con dependencias
echo "Paso 1: Creando Layer con dependencias (transformers + torch)..."
LAYER_DIR=$(mktemp -d)
LAYER_PYTHON_DIR="$LAYER_DIR/python/lib/python3.11/site-packages"
mkdir -p "$LAYER_PYTHON_DIR"

# Instalar con arquitectura arm64 (Lambda usa graviton processors)
pip install \
    --platform manylinux2014_aarch64 \
    --implementation cp \
    --python 3.11 \
    --only-binary=:all: \
    -t "$LAYER_PYTHON_DIR" \
    transformers==4.36.2 \
    torch==2.1.2 \
    > /dev/null 2>&1

echo "✓ Dependencias instaladas en Layer"

# Step 2: Crear ZIP del Layer
echo ""
echo "Paso 2: Comprimiendo Layer (~450MB, esperar...)..."
cd "$LAYER_DIR"
LAYER_ZIP="finbert-dependencies-layer.zip"
zip -r -q "$LAYER_ZIP" python/
LAYER_SIZE=$(du -h "$LAYER_ZIP" | cut -f1)
echo "✓ Layer comprimido: $LAYER_SIZE"

# Step 3: Publicar Layer en Lambda
echo ""
echo "Paso 3: Publicando Layer en AWS Lambda..."
LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name finbert-dependencies \
    --zip-file fileb://"$LAYER_ZIP" \
    --compatible-runtimes python3.11 \
    --region "$AWS_REGION" \
    --query 'LayerVersionArn' \
    --output text)

echo "✓ Layer publicado: $LAYER_ARN"

# Step 4: Preparar código Lambda
echo ""
echo "Paso 4: Preparando código Lambda..."
cd - > /dev/null
TEMP_DIR=$(mktemp -d)
cp lambda_sentiment.py "$TEMP_DIR/"
cd "$TEMP_DIR"

# El modelo se descargará automáticamente la primera vez que Lambda se invoque
# Esto ocurre en el cold start, pero solo una vez por Lambda

zip -r -q lambda_sentiment.zip lambda_sentiment.py
FUNC_ZIP_SIZE=$(du -h lambda_sentiment.zip | cut -f1)
echo "✓ Código Lambda preparado: $FUNC_ZIP_SIZE"

# Step 5: Crear o actualizar función Lambda
echo ""
echo "Paso 5: Desplegando función Lambda..."

if aws lambda get-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION" > /dev/null 2>&1; then
    echo "  Actualizando función existente..."
    aws lambda update-function-code \
        --function-name "$LAMBDA_NAME" \
        --zip-file fileb://lambda_sentiment.zip \
        --region "$AWS_REGION" \
        > /dev/null

    # Esperar a que se actualice
    sleep 5

    # Actualizar configuración
    aws lambda update-function-configuration \
        --function-name "$LAMBDA_NAME" \
        --timeout 300 \
        --memory-size 2048 \
        --layers "$LAYER_ARN" \
        --environment Variables="{HF_HOME=/tmp/.cache/huggingface}" \
        --region "$AWS_REGION" \
        > /dev/null

else
    echo "  Creando nueva función..."
    aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime python3.11 \
        --role "$ROLE_ARN" \
        --handler lambda_sentiment.handler \
        --zip-file fileb://lambda_sentiment.zip \
        --timeout 300 \
        --memory-size 2048 \
        --layers "$LAYER_ARN" \
        --environment Variables="{HF_HOME=/tmp/.cache/huggingface}" \
        --ephemeral-storage Size=2048 \
        --region "$AWS_REGION" \
        > /dev/null
fi

echo "✓ Función Lambda desplegada"

# Step 6: Limpiar archivos temporales
echo ""
echo "Paso 6: Limpiando archivos temporales..."
rm -rf "$LAYER_DIR" "$TEMP_DIR"
echo "✓ Limpieza completa"

echo ""
echo "=========================================="
echo "✓ Despliegue Completado"
echo "=========================================="
echo ""
echo "ℹ️  Información importante:"
echo ""
echo "1. COLD START:"
echo "   - Primera invocación: ~30-45 segundos (descarga modelo FinBERT)"
echo "   - Invocaciones posteriores: ~2-3 segundos"
echo ""
echo "2. STORAGE:"
echo "   - Layer: ~450MB"
echo "   - Modelo descargado: ~400MB (almacenado en /tmp, efímero)"
echo "   - Total en Lambda: ~850MB (dentro del límite de 2GB)"
echo ""
echo "3. COSTO:"
echo "   - Memoria: 2048MB"
echo "   - Tiempo promedio: 30-40 segundos/ejecución"
echo "   - Costo por 1000 Headlines: ~$0.02-0.03 (vs $0.30 con Bedrock)"
echo ""
echo "4. PRÓXIMOS PASOS:"
echo "   - Invocar manualmente para activar el modelo:"
echo "     aws lambda invoke --function-name lambda_sentiment --region $AWS_REGION response.json"
echo "   - Ver logs: aws logs tail /aws/lambda/lambda_sentiment --follow"
echo ""
echo "5. OPTIMIZACIÓN (opcional):"
echo "   - Usar Lambda Container Image para control completo del modelo"
echo "   - Usar S3 + Lambda Layer con modelo pre-descargado"
echo "   - Usar EFS para compartir modelo entre instancias"
echo ""
