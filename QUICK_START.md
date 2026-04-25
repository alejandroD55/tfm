# Quick Start Guide - TFM Trading System

Guía rápida para desplegar el sistema de trading TFM en AWS.

## Prerrequisitos

- Cuenta de AWS con permisos suficientes
- AWS CLI configurado
- Python 3.11+
- VPC con Aurora PostgreSQL accesible

## Pasos de Configuración

### 1. Preparar Aurora PostgreSQL

```bash
# Conectarse a Aurora
psql -h your-cluster-endpoint.rds.amazonaws.com -U postgres -d your_database

# Ejecutar el script de schema
\i database_schema.sql

# Verificar las tablas creadas
\dt
```

### 2. Configurar AWS Secrets Manager

```bash
# Crear secreto para Aurora
aws secretsmanager create-secret \
  --name aurora/credentials \
  --secret-string '{
    "host": "your-aurora-endpoint.rds.amazonaws.com",
    "port": 5432,
    "username": "postgres",
    "password": "your-password",
    "dbname": "your-database"
  }'

# Crear secreto para Finnhub
aws secretsmanager create-secret \
  --name finnhub/api_key \
  --secret-string '{
    "api_key": "your-finnhub-api-key"
  }'
```

### 3. Preparar S3

```bash
# Crear buckets
aws s3 mb s3://tfm-unir-config --region us-east-1
aws s3 mb s3://tfm-unir-datalake --region us-east-1

# Subir configuración de ETFs
aws s3 cp etf_universe.json s3://tfm-unir-config/etf_universe.json
```

### 4. Crear Rol IAM para Lambda

```bash
# Crear rol
aws iam create-role \
  --role-name tfm-unir-lambda-execution-role \
  --assume-role-policy-document '{
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
  }'

# Adjuntar política
aws iam put-role-policy \
  --role-name tfm-unir-lambda-execution-role \
  --policy-name tfm-unir-lambda-policy \
  --policy-document file://iam_policy.json

# Opcionalmente, adjuntar rol para VPC (si es necesario)
aws iam attach-role-policy \
  --role-name tfm-unir-lambda-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
```

### 5. Desplegar Funciones Lambda

```bash
# Hacer script ejecutable
chmod +x deploy.sh

# Desplegar todas las lambdas
# Reemplazar ACCOUNT_ID con tu ID de cuenta AWS
./deploy.sh us-east-1 123456789012 arn:aws:iam::123456789012:role/tfm-unir-lambda-execution-role
```

### 6. Configurar Lambda VPC (si es necesario)

Si Aurora está en una VPC privada:

```bash
# Para cada lambda
aws lambda update-function-configuration \
  --function-name lambda_ingestion \
  --vpc-config SubnetIds=subnet-xxxxx,subnet-xxxxx SecurityGroupIds=sg-xxxxx
```

### 7. Crear Step Functions State Machine

```bash
# Crear state machine
aws stepfunctions create-state-machine \
  --name tfm-unir-trading-pipeline \
  --definition file://stepfunctions_definition.json \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/stepfunctions-execution-role

# Nota: Primero crear la rol para Step Functions con permisos para invocar Lambdas
```

### 8. Ejecutar el Pipeline

**Opción A: Manual**
```bash
# Invocar lambda_ingestion manualmente
aws lambda invoke \
  --function-name lambda_ingestion \
  --payload '{}' \
  response.json

# Ver resultado
cat response.json
```

**Opción B: Step Functions**
```bash
# Ejecutar state machine
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:tfm-unir-trading-pipeline
```

**Opción C: EventBridge (Diario a las 9 AM UTC)**
```bash
# Crear regla de EventBridge
aws events put-rule \
  --name tfm-unir-daily-trigger \
  --schedule-expression "cron(0 9 * * ? *)" \
  --state ENABLED

# Agregar target
aws events put-targets \
  --rule tfm-unir-daily-trigger \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:ACCOUNT_ID:function:lambda_ingestion"
```

### 9. Monitorear Ejecución

```bash
# Ver logs de CloudWatch
aws logs tail /aws/lambda/lambda_ingestion --follow

# Ver métricas
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=lambda_ingestion \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600 \
  --statistics Sum
```

## Verificación Post-Despliegue

```bash
# 1. Verificar que las lambdas existan
aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `lambda_`)].[FunctionName]'

# 2. Verificar secrets
aws secretsmanager describe-secret --secret-id aurora/credentials
aws secretsmanager describe-secret --secret-id finnhub/api_key

# 3. Verificar S3
aws s3 ls s3://tfm-unir-config/
aws s3 ls s3://tfm-unir-datalake/

# 4. Verificar Aurora (conectarse y ejecutar)
SELECT * FROM batch_log;
SELECT COUNT(*) FROM sentiment_scores;
SELECT COUNT(*) FROM technical_indicators;
SELECT COUNT(*) FROM trading_signals;
```

## Solución de Problemas

### Error: "Could not connect to Aurora"
- Verificar security group del RDS permite conexiones desde Lambda
- Verificar que Lambda esté en la misma VPC
- Verificar secretos en Secrets Manager

### Error: "Access Denied" (S3)
- Verificar política IAM adjunta al rol
- Verificar nombres de buckets en las políticas

### Error: "Timeout"
- Aumentar timeout en AWS Lambda (máximo 900 segundos)
- Aumentar memoria asignada a la lambda

## Costos Estimados

Por ejecución diaria del pipeline:
- **Lambda**: ~$0.0001 (según invocaciones y duración)
- **Aurora**: ~$1-5 (según cantidad de datos)
- **Hugging Face (FinBERT)**: $0.00
- **S3**: <$0.01 (storage mínimo)

## Próximos Pasos

1. Monitorear los primeros días de ejecución
2. Ajustar CPTs de la red bayesiana según resultados
3. Integrar con sistema de ejecución de órdenes (broker API)
4. Implementar gestión de riesgo y stop-loss
5. Agregar más tickers y datos históricos

## Documentación Completa

Ver `README.md` para documentación detallada de cada componente.
