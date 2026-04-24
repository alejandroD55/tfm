# Procedimiento Completo: De Ingesta a Resultados

Guía práctica paso a paso para ejecutar el pipeline TFM completo, desde la ingesta de datos hasta la lectura de resultados.

---

## 🎯 Resumen del Procedimiento

```
PASO 1: Configuración Inicial (AWS Infrastructure)
    ↓
PASO 2: Setup Base de Datos (Aurora PostgreSQL)
    ↓
PASO 3: Configurar Secretos (Secrets Manager)
    ↓
PASO 4: Preparar S3 (buckets y archivos)
    ↓
PASO 5: Desplegar Lambdas (5 funciones)
    ↓
PASO 6: Ejecutar Pipeline (Step Functions o manual)
    ↓
PASO 7: Monitorear Ejecución (CloudWatch)
    ↓
PASO 8: Leer Resultados (queries a Aurora + S3)
```

**Tiempo total:** 2-3 horas la primera vez

---

## 📋 VARIABLES DE CONFIGURACIÓN

Copia esto y rellena con tus valores:

```bash
# ═══════════════════════════════════════════════════════════
# VARIABLES DE CONFIGURACIÓN - EDITA ESTOS VALORES
# ═══════════════════════════════════════════════════════════

# AWS Account Information
export AWS_ACCOUNT_ID="123456789012"              # Tu Account ID
export AWS_REGION="us-east-1"                     # Tu región
export AWS_PROFILE="default"                      # Tu perfil de AWS CLI

# Aurora Database
export AURORA_ENDPOINT="tfm-cluster.xxxxx.us-east-1.rds.amazonaws.com"
export AURORA_PORT="5432"
export AURORA_USER="postgres"
export AURORA_PASSWORD="TuPasswordSeguro123!"     # ⚠️ Cambiar
export AURORA_DATABASE="tfm_db"

# Finnhub API
export FINNHUB_API_KEY="tu_api_key_finnhub_aqui" # ⚠️ Obtener en https://finnhub.io

# Lambda Execution Role
export LAMBDA_ROLE_NAME="tfm-lambda-execution-role"
export LAMBDA_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}"

# S3 Buckets
export S3_CONFIG_BUCKET="tfm-config"
export S3_DATALAKE_BUCKET="tfm-datalake"

# ETFs a analizar
export ETFS="SPY,QQQ,IWM,EEM,VWO,AGG,TLT,GLD,DBC,USO"

# ═══════════════════════════════════════════════════════════
```

**Guarda esto en un archivo:** `~/.tfm_config.sh`

Luego cárgalo:
```bash
source ~/.tfm_config.sh
```

---

# 🔧 PASO 1: CONFIGURACIÓN INICIAL (AWS Infrastructure)

## 1.1 Verificar AWS CLI

```bash
# Verificar instalación
aws --version

# Configurar credenciales (si no las tienes)
aws configure

# Verificar que funciona
aws sts get-caller-identity
```

**Salida esperada:**
```json
{
    "UserId": "AIDAJ45Q7YFFAREXAMPLE",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/alejandro"
}
```

## 1.2 Crear IAM Role para Lambda

```bash
# Crear trust policy
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

# Crear role
aws iam create-role \
    --role-name $LAMBDA_ROLE_NAME \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --region $AWS_REGION

# Adjuntar policy de permisos
aws iam put-role-policy \
    --role-name $LAMBDA_ROLE_NAME \
    --policy-name tfm-lambda-policy \
    --policy-document file://iam_policy.json \
    --region $AWS_REGION

# Opcional: Adjuntar permisos de VPC (si Aurora está en VPC privada)
aws iam attach-role-policy \
    --role-name $LAMBDA_ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole \
    --region $AWS_REGION

echo "✓ Role creado: $LAMBDA_ROLE_ARN"
```

---

# 🗄️ PASO 2: SETUP BASE DE DATOS (Aurora PostgreSQL)

## 2.1 Crear Cluster Aurora (opcional si ya existe)

```bash
# Si ya tienes Aurora, salta a 2.2

# Crear cluster
aws rds create-db-cluster \
    --db-cluster-identifier tfm-cluster \
    --engine aurora-postgresql \
    --master-username postgres \
    --master-user-password "$AURORA_PASSWORD" \
    --database-name $AURORA_DATABASE \
    --region $AWS_REGION

# Esperar a que se cree (~5-10 minutos)
aws rds describe-db-clusters \
    --db-cluster-identifier tfm-cluster \
    --region $AWS_REGION \
    --query 'DBClusters[0].Status'
```

## 2.2 Conectarse a Aurora

```bash
# Instalar psql si no lo tienes
# macOS:
brew install postgresql

# Ubuntu/Debian:
sudo apt-get install postgresql-client

# Windows: descargar de https://www.postgresql.org/download/windows/

# Conectarse
psql -h $AURORA_ENDPOINT \
     -U $AURORA_USER \
     -d $AURORA_DATABASE \
     -p $AURORA_PORT
```

## 2.3 Crear Schema de Base de Datos

Una vez conectado en psql:

```sql
-- Copiar y ejecutar TODO el contenido de database_schema.sql

-- Crear tabla batch_log
CREATE TABLE IF NOT EXISTS batch_log (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL,
    tickers_processed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT status_check CHECK (status IN ('STARTED', 'COMPLETED', 'FAILED'))
);

-- Crear tabla sentiment_scores
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    headline TEXT NOT NULL,
    sentiment VARCHAR(20) NOT NULL,
    confidence FLOAT NOT NULL,
    justification TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sentiment_check CHECK (sentiment IN ('bullish', 'bearish', 'neutral')),
    CONSTRAINT confidence_check CHECK (confidence >= 0 AND confidence <= 1),
    UNIQUE(batch_date, ticker, headline)
);

-- ... (copiar resto de database_schema.sql)

-- Verificar que se crearon
\dt
```

**Salida esperada:**
```
                    List of relations
 Schema |         Name          | Type  |  Owner
────────┼───────────────────────┼───────┼──────────
 public | batch_log             | table | postgres
 public | sentiment_scores      | table | postgres
 public | technical_indicators  | table | postgres
 public | trading_signals       | table | postgres
```

**Salir de psql:**
```sql
\q
```

---

# 🔐 PASO 3: CONFIGURAR SECRETOS (AWS Secrets Manager)

## 3.1 Aurora Credentials

```bash
# Crear secreto para Aurora
aws secretsmanager create-secret \
    --name aurora/credentials \
    --description "Aurora PostgreSQL credentials for TFM" \
    --secret-string "{
        \"host\": \"$AURORA_ENDPOINT\",
        \"port\": $AURORA_PORT,
        \"username\": \"$AURORA_USER\",
        \"password\": \"$AURORA_PASSWORD\",
        \"dbname\": \"$AURORA_DATABASE\"
    }" \
    --region $AWS_REGION

# Verificar
aws secretsmanager describe-secret \
    --secret-id aurora/credentials \
    --region $AWS_REGION

echo "✓ Secreto Aurora creado"
```

## 3.2 Finnhub API Key

```bash
# Crear secreto para Finnhub
aws secretsmanager create-secret \
    --name finnhub/api_key \
    --description "Finnhub API key for financial news" \
    --secret-string "{
        \"api_key\": \"$FINNHUB_API_KEY\"
    }" \
    --region $AWS_REGION

# Verificar
aws secretsmanager describe-secret \
    --secret-id finnhub/api_key \
    --region $AWS_REGION

echo "✓ Secreto Finnhub creado"
```

---

# 📦 PASO 4: PREPARAR S3 (Buckets y Archivos)

## 4.1 Crear Buckets S3

```bash
# Crear bucket de configuración
aws s3 mb s3://$S3_CONFIG_BUCKET \
    --region $AWS_REGION

# Crear bucket de datalake
aws s3 mb s3://$S3_DATALAKE_BUCKET \
    --region $AWS_REGION

# Verificar
aws s3 ls | grep tfm

echo "✓ Buckets S3 creados"
```

## 4.2 Subir Configuración de ETFs

```bash
# Crear archivo etf_universe.json si no existe
cat > /tmp/etf_universe.json << EOF
{
  "tickers": [
    "SPY", "QQQ", "IWM", "EEM", "VWO",
    "AGG", "TLT", "GLD", "DBC", "USO"
  ],
  "description": "ETFs para análisis TFM"
}
EOF

# Subir a S3
aws s3 cp /tmp/etf_universe.json \
    s3://$S3_CONFIG_BUCKET/etf_universe.json \
    --region $AWS_REGION

# Verificar
aws s3 ls s3://$S3_CONFIG_BUCKET/

echo "✓ Configuración de ETFs subida a S3"
```

---

# 🚀 PASO 5: DESPLEGAR LAMBDAS (5 Funciones)

## 5.1 λ1 - Ingestion

```bash
cd tfm/lambda_ingestion

# Empacar
zip lambda_ingestion.zip lambda_ingestion.py
pip install -r requirements.txt -t .
zip -r lambda_ingestion.zip .

# Crear función
aws lambda create-function \
    --function-name lambda_ingestion \
    --runtime python3.11 \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_ingestion.handler \
    --zip-file fileb://lambda_ingestion.zip \
    --timeout 300 \
    --memory-size 512 \
    --region $AWS_REGION

echo "✓ λ1 - lambda_ingestion desplegada"
```

## 5.2 λ2 - Sentiment (FinBERT)

```bash
cd tfm/lambda_sentiment

# Hacer script ejecutable
chmod +x deploy_finbert.sh

# Desplegar automáticamente
./deploy_finbert.sh $AWS_REGION $AWS_ACCOUNT_ID $LAMBDA_ROLE_ARN

echo "✓ λ2 - lambda_sentiment desplegada (FinBERT)"
```

## 5.3 λ3 - Indicators

```bash
cd tfm/lambda_indicators

zip lambda_indicators.zip lambda_indicators.py
pip install -r requirements.txt -t .
zip -r lambda_indicators.zip .

aws lambda create-function \
    --function-name lambda_indicators \
    --runtime python3.11 \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_indicators.handler \
    --zip-file fileb://lambda_indicators.zip \
    --timeout 300 \
    --memory-size 512 \
    --region $AWS_REGION

echo "✓ λ3 - lambda_indicators desplegada"
```

## 5.4 λ4 - Bayesian

```bash
cd tfm/lambda_bayesian

zip lambda_bayesian.zip lambda_bayesian.py
pip install -r requirements.txt -t .
zip -r lambda_bayesian.zip .

aws lambda create-function \
    --function-name lambda_bayesian \
    --runtime python3.11 \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_bayesian.handler \
    --zip-file fileb://lambda_bayesian.zip \
    --timeout 300 \
    --memory-size 1024 \
    --region $AWS_REGION

echo "✓ λ4 - lambda_bayesian desplegada"
```

## 5.5 λ5 - Report

```bash
cd tfm/lambda_report

zip lambda_report.zip lambda_report.py
pip install -r requirements.txt -t .
zip -r lambda_report.zip .

aws lambda create-function \
    --function-name lambda_report \
    --runtime python3.11 \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_report.handler \
    --zip-file fileb://lambda_report.zip \
    --timeout 300 \
    --memory-size 512 \
    --region $AWS_REGION

echo "✓ λ5 - lambda_report desplegada"
```

## 5.6 Verificar todas las lambdas

```bash
# Listar todas las lambdas
aws lambda list-functions \
    --region $AWS_REGION \
    --query 'Functions[?starts_with(FunctionName, `lambda_`)].[FunctionName]'

# Salida esperada:
# lambda_bayesian
# lambda_indicators
# lambda_ingestion
# lambda_report
# lambda_sentiment
```

---

# ⚙️ PASO 6: EJECUTAR PIPELINE

## Opción A: Ejecución Manual Paso a Paso

```bash
# ═══════════════════════════════════════════════════════════
# PASO 6.1 - Ejecutar λ1 (Ingestion)
# ═══════════════════════════════════════════════════════════

echo "► Iniciando λ1 - Ingestion..."
aws lambda invoke \
    --function-name lambda_ingestion \
    --region $AWS_REGION \
    /tmp/lambda1_response.json

# Ver respuesta
cat /tmp/lambda1_response.json | jq .

# Esperar confirmación
read -p "¿λ1 completada? [y/n] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "✓ Continuando..."
fi

# ═══════════════════════════════════════════════════════════
# PASO 6.2 - Ejecutar λ2 + λ3 (Paralelo)
# ═══════════════════════════════════════════════════════════

echo "► Iniciando λ2 - Sentiment Analysis..."
aws lambda invoke \
    --function-name lambda_sentiment \
    --region $AWS_REGION \
    /tmp/lambda2_response.json &

echo "► Iniciando λ3 - Technical Indicators..."
aws lambda invoke \
    --function-name lambda_indicators \
    --region $AWS_REGION \
    /tmp/lambda3_response.json &

# Esperar a que ambas terminen
wait

# Ver respuestas
echo "λ2 Response:"
cat /tmp/lambda2_response.json | jq .
echo ""
echo "λ3 Response:"
cat /tmp/lambda3_response.json | jq .

# ═══════════════════════════════════════════════════════════
# PASO 6.3 - Ejecutar λ4 (Bayesian)
# ═══════════════════════════════════════════════════════════

echo "► Iniciando λ4 - Bayesian Inference..."
aws lambda invoke \
    --function-name lambda_bayesian \
    --region $AWS_REGION \
    /tmp/lambda4_response.json

cat /tmp/lambda4_response.json | jq .

# ═══════════════════════════════════════════════════════════
# PASO 6.4 - Ejecutar λ5 (Report)
# ═══════════════════════════════════════════════════════════

echo "► Iniciando λ5 - Report Generation..."
aws lambda invoke \
    --function-name lambda_report \
    --region $AWS_REGION \
    /tmp/lambda5_response.json

cat /tmp/lambda5_response.json | jq .

echo "✓ Pipeline completado!"
```

## Opción B: Ejecución Automática con Step Functions (Opcional)

```bash
# Crear state machine
aws stepfunctions create-state-machine \
    --name tfm-trading-pipeline \
    --definition file://stepfunctions_definition.json \
    --role-arn arn:aws:iam::$AWS_ACCOUNT_ID:role/step-functions-execution-role \
    --region $AWS_REGION

# Ejecutar pipeline
EXECUTION_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:$AWS_REGION:$AWS_ACCOUNT_ID:stateMachine:tfm-trading-pipeline \
    --region $AWS_REGION \
    --query 'executionArn' \
    --output text)

echo "Execution started: $EXECUTION_ARN"

# Ver progreso
aws stepfunctions describe-execution \
    --execution-arn $EXECUTION_ARN \
    --region $AWS_REGION
```

---

# 📊 PASO 7: MONITOREAR EJECUCIÓN

## 7.1 Ver Logs en Tiempo Real

```bash
# Monitorear λ1 - Ingestion
echo "► Logs de λ1 - Ingestion"
aws logs tail /aws/lambda/lambda_ingestion --follow --region $AWS_REGION

# En otra terminal, monitorear λ2
echo "► Logs de λ2 - Sentiment"
aws logs tail /aws/lambda/lambda_sentiment --follow --region $AWS_REGION

# En otra terminal, monitorear λ3
echo "► Logs de λ3 - Indicators"
aws logs tail /aws/lambda/lambda_indicators --follow --region $AWS_REGION

# Etc...
```

## 7.2 Métricas en CloudWatch

```bash
# Ver invocaciones
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=lambda_ingestion \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S)Z \
    --period 300 \
    --statistics Sum \
    --region $AWS_REGION

# Ver duración promedio
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value=lambda_sentiment \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)Z \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S)Z \
    --period 300 \
    --statistics Average,Maximum \
    --region $AWS_REGION
```

---

# 📈 PASO 8: LEER RESULTADOS

## 8.1 Conectarse a Aurora y Consultar Datos

```bash
# Conectarse a Aurora
psql -h $AURORA_ENDPOINT \
     -U $AURORA_USER \
     -d $AURORA_DATABASE \
     -p $AURORA_PORT
```

### 8.1a - Ver logs de batch

```sql
-- Ver status del batch de hoy
SELECT * FROM batch_log 
WHERE batch_date = CURRENT_DATE
ORDER BY created_at DESC;

-- Resultado esperado:
--  id | batch_date | status  | tickers_processed |      created_at
-- ────┼────────────┼─────────┼────────────────────┼─────────────────────
--   1 | 2024-04-24 | STARTED |                 10 | 2024-04-24 09:01:00
--   2 | 2024-04-24 | STARTED |                 10 | 2024-04-24 09:05:00
--   3 | 2024-04-24 | COMPLETED |              10 | 2024-04-24 09:20:00
```

### 8.1b - Ver análisis de sentimiento

```sql
-- Ver sentimientos de hoy
SELECT ticker, sentiment, AVG(confidence) as avg_confidence, COUNT(*) as headlines_count
FROM sentiment_scores
WHERE batch_date = CURRENT_DATE
GROUP BY ticker, sentiment
ORDER BY ticker, sentiment;

-- Resultado esperado:
--  ticker | sentiment | avg_confidence | headlines_count
-- ────────┼───────────┼────────────────┼─────────────────
--  AGG    | bearish   |      0.76      |        2
--  AGG    | bullish   |      0.89      |        5
--  AGG    | neutral   |      0.82      |        3
--  DBC    | bullish   |      0.91      |        4
```

### 8.1c - Ver indicadores técnicos

```sql
-- Ver últimos indicadores
SELECT ticker, close_price, rsi_14, sma_20, sma_50, bb_upper, bb_lower
FROM technical_indicators
WHERE batch_date = CURRENT_DATE
ORDER BY ticker;

-- Resultado esperado:
--  ticker | close_price | rsi_14 | sma_20  | sma_50  | bb_upper | bb_lower
-- ────────┼─────────────┼────────┼─────────┼─────────┼──────────┼──────────
--  AGG    |    96.43    |  58.2  |  96.10  |  95.80  |  97.50   |  94.70
--  DBC    |   102.15    |  72.8  | 101.50  | 100.20  | 105.30   |  97.70
```

### 8.1d - Ver señales de trading

```sql
-- Ver señales de trading
SELECT ticker, signal, prob_up, prob_down
FROM trading_signals
WHERE batch_date = CURRENT_DATE
ORDER BY ticker;

-- Resultado esperado:
--  ticker | signal | prob_up | prob_down
-- ────────┼────────┼─────────┼──────────
--  AGG    | HOLD   |  0.58   |  0.42
--  DBC    | BUY    |  0.72   |  0.28
--  EEM    | SELL   |  0.31   |  0.69
--  GLD    | HOLD   |  0.54   |  0.46
--  IWM    | BUY    |  0.68   |  0.32
```

### 8.1e - Ver metrics de backtesting

```sql
-- Ver resultados de backtesting (últimos 90 días)
SELECT * FROM batch_log 
WHERE status = 'COMPLETED' 
AND batch_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY batch_date DESC
LIMIT 5;
```

### 8.1f - Consultas Avanzadas

```sql
-- Dashboard: Sentimiento actual por ticker
SELECT 
    ticker,
    sentiment,
    ROUND(AVG(confidence)::numeric, 3) as avg_confidence,
    COUNT(*) as count
FROM sentiment_scores
WHERE batch_date = CURRENT_DATE
GROUP BY ticker, sentiment
PIVOT (
    COUNT(*)
    FOR sentiment IN ('bullish' as bullish, 'neutral' as neutral, 'bearish' as bearish)
);

-- Dashboard: Señales de hoy
SELECT 
    ticker,
    signal,
    ROUND(prob_up::numeric, 3) as prob_up,
    ROUND(prob_down::numeric, 3) as prob_down,
    CASE 
        WHEN signal = 'BUY' THEN '🟢 COMPRAR'
        WHEN signal = 'SELL' THEN '🔴 VENDER'
        ELSE '🟡 ESPERAR'
    END as acción
FROM trading_signals
WHERE batch_date = CURRENT_DATE
ORDER BY prob_up DESC;
```

**Salir de psql:**
```sql
\q
```

## 8.2 Descargar Reporte desde S3

```bash
# Listar reportes disponibles
aws s3 ls s3://$S3_DATALAKE_BUCKET/results/ --recursive --region $AWS_REGION

# Descargar último reporte
FECHA=$(date +%Y-%m-%d)
aws s3 cp s3://$S3_DATALAKE_BUCKET/results/$FECHA/report.json \
    /tmp/report.json \
    --region $AWS_REGION

# Ver reporte
cat /tmp/report.json | jq .

# Resultado esperado:
# {
#   "report_date": "2024-04-24",
#   "data_period_days": 90,
#   "backtesting_metrics": {
#     "SPY": {
#       "cumulative_return": 0.187,
#       "sharpe_ratio": 1.45,
#       "max_drawdown": -0.082,
#       "final_equity": 11870,
#       "starting_capital": 10000
#     },
#     ...
#   },
#   "summary": {
#     "total_tickers": 10,
#     "avg_cumulative_return": 0.156,
#     "avg_sharpe_ratio": 1.32,
#     "avg_max_drawdown": -0.095
#   }
# }
```

---

# 📋 RESUMEN DE COMANDOS RÁPIDO

```bash
# ═══════════════════════════════════════════════════════════
# TODA LA CONFIGURACIÓN EN 1 BLOQUE
# ═══════════════════════════════════════════════════════════

# 1. CARGAR VARIABLES
source ~/.tfm_config.sh

# 2. CREAR SECRETOS
aws secretsmanager create-secret \
    --name aurora/credentials \
    --secret-string "..." --region $AWS_REGION

aws secretsmanager create-secret \
    --name finnhub/api_key \
    --secret-string "..." --region $AWS_REGION

# 3. CREAR S3
aws s3 mb s3://$S3_CONFIG_BUCKET --region $AWS_REGION
aws s3 mb s3://$S3_DATALAKE_BUCKET --region $AWS_REGION
aws s3 cp /tmp/etf_universe.json s3://$S3_CONFIG_BUCKET/

# 4. CREAR SCHEMA EN AURORA
psql -h $AURORA_ENDPOINT -U $AURORA_USER -d $AURORA_DATABASE < database_schema.sql

# 5. DESPLEGAR LAMBDAS
./deploy.sh $AWS_REGION $AWS_ACCOUNT_ID $LAMBDA_ROLE_ARN

# 6. EJECUTAR PIPELINE
aws lambda invoke --function-name lambda_ingestion response.json

# 7. LEER RESULTADOS
psql -h $AURORA_ENDPOINT -U $AURORA_USER -d $AURORA_DATABASE \
    -c "SELECT * FROM trading_signals WHERE batch_date = CURRENT_DATE;"

aws s3 cp s3://$S3_DATALAKE_BUCKET/results/$(date +%Y-%m-%d)/report.json /tmp/ && \
    cat /tmp/report.json | jq .
```

---

# ✅ CHECKLIST DE VALIDACIÓN

```
ANTES DE EJECUTAR EL PIPELINE:
[ ] Cargaste el archivo ~/.tfm_config.sh
[ ] AWS CLI está configurado y funciona
[ ] Aurora está creada y accesible
[ ] Aurora tiene el schema creado
[ ] Secretos están en Secrets Manager
[ ] S3 buckets creados
[ ] etf_universe.json está en S3
[ ] Todas las 5 lambdas desplegadas
[ ] Logs en CloudWatch se ven normales

DURANTE LA EJECUCIÓN:
[ ] Monitorear logs de cada lambda
[ ] Verificar que no hay errores
[ ] Confirmar que se crean registros en Aurora
[ ] Verificar archivos en S3

DESPUÉS DE LA EJECUCIÓN:
[ ] Datos en batch_log: status = 'COMPLETED'
[ ] Datos en sentiment_scores: 100+ registros
[ ] Datos en technical_indicators: 10 registros (1 por ticker)
[ ] Datos en trading_signals: 10 registros con señales
[ ] report.json en S3 con métricas de backtesting
```

---

# 🆘 TROUBLESHOOTING RÁPIDO

### Error: "Unable to locate credentials"

```bash
# Configurar AWS CLI
aws configure

# O usar profile específico
export AWS_PROFILE=your_profile
```

### Error: "Connection refused" en Aurora

```bash
# Verificar que Aurora está corriendo
aws rds describe-db-clusters --region $AWS_REGION

# Verificar security group permite conexiones
aws ec2 describe-security-groups --region $AWS_REGION | grep tfm
```

### Error: "Lambda function not found"

```bash
# Verificar que todas las lambdas existen
aws lambda list-functions --region $AWS_REGION | grep lambda_

# Si falta alguna, desplegar con deploy.sh
```

### Error: "Access Denied" en S3

```bash
# Verificar permisos IAM
aws iam get-user
aws iam list-attached-user-policies --user-name $USER_NAME
```

### Lambda timeout

```bash
# Aumentar timeout
aws lambda update-function-configuration \
    --function-name lambda_sentiment \
    --timeout 300 \
    --region $AWS_REGION
```

---

**¡Listo! Sigue estos pasos en orden y todo debería funcionar.** 🚀
