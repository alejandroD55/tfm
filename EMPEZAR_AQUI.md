# 🚀 EMPEZAR AQUÍ - Guía de Ejecución Rápida

**Antes de nada:** Lee esto para entender qué necesitas hacer.

---

## 📋 Requisitos Previos

```bash
✓ Cuenta AWS con credenciales configuradas
✓ AWS CLI instalado
✓ PostgreSQL client (psql) instalado
✓ Aurora PostgreSQL ya creado
✓ Finnhub API key (obtener en https://finnhub.io)
```

---

## 🎯 Opción 1: INSTALACIÓN AUTOMÁTICA (Recomendado)

**Tiempo:** 15-20 minutos

```bash
# 1. Ir a la carpeta
cd /sessions/sharp-peaceful-goldberg/mnt/tfm

# 2. Ejecutar el script de instalación
./install.sh --full
```

**El script automáticamente va a:**
- ✅ Configurar variables de entorno
- ✅ Crear infraestructura AWS (Role, Secretos, S3)
- ✅ Crear schema en Aurora
- ✅ Desplegar las 5 lambdas
- ✅ Ejecutar el pipeline
- ✅ Leer y mostrar resultados

**Cuando pregunte:**
- AWS Account ID → tu ID (ej: 123456789012)
- AWS Region → tu región (ej: us-east-1)
- Aurora Endpoint → endpoint de tu cluster Aurora
- Aurora Password → contraseña de postgres
- Finnhub API Key → tu API key

---

## 🎯 Opción 2: INSTALACIÓN MANUAL POR PASOS

**Tiempo:** 30 minutos (si prefieres hacerlo manualmente)

### Paso 1: Configurar Variables

```bash
cd /sessions/sharp-peaceful-goldberg/mnt/tfm

# Copiar y editar con tus valores
cat > ~/.tfm_config.sh << 'VARS'
export AWS_ACCOUNT_ID="123456789012"
export AWS_REGION="us-east-1"
export AURORA_ENDPOINT="tfm-cluster.xxxxx.us-east-1.rds.amazonaws.com"
export AURORA_PASSWORD="TuPassword123"
export AURORA_DATABASE="tfm_db"
export FINNHUB_API_KEY="tu_api_key_aqui"
VARS

# Cargar variables
source ~/.tfm_config.sh
```

### Paso 2: Crear Secretos en AWS

```bash
# Crear secreto de Aurora
aws secretsmanager create-secret \
    --name aurora/credentials \
    --secret-string "{
        \"host\": \"$AURORA_ENDPOINT\",
        \"port\": 5432,
        \"username\": \"postgres\",
        \"password\": \"$AURORA_PASSWORD\",
        \"dbname\": \"$AURORA_DATABASE\"
    }" \
    --region $AWS_REGION

# Crear secreto de Finnhub
aws secretsmanager create-secret \
    --name finnhub/api_key \
    --secret-string "{\"api_key\": \"$FINNHUB_API_KEY\"}" \
    --region $AWS_REGION
```

### Paso 3: Crear S3 y Subir Configuración

```bash
# Crear buckets
aws s3 mb s3://tfm-config --region $AWS_REGION
aws s3 mb s3://tfm-datalake --region $AWS_REGION

# Subir lista de ETFs
aws s3 cp etf_universe.json s3://tfm-config/ --region $AWS_REGION
```

### Paso 4: Crear Schema en Aurora

```bash
# Conectarse y ejecutar schema
psql -h $AURORA_ENDPOINT \
     -U postgres \
     -d tfm_db \
     -f database_schema.sql
```

### Paso 5: Desplegar Lambdas

```bash
# Crear Role IAM
aws iam create-role \
    --role-name tfm-lambda-execution-role \
    --assume-role-policy-document file:///tmp/trust-policy.json

# Desplegar lambdas
./install.sh --deploy-lambdas
```

### Paso 6: Ejecutar Pipeline

```bash
# Ejecutar todas las lambdas en orden
./install.sh --run-pipeline
```

### Paso 7: Leer Resultados

```bash
# Leer datos de Aurora
psql -h $AURORA_ENDPOINT \
     -U postgres \
     -d tfm_db \
     -c "SELECT ticker, signal, prob_up FROM trading_signals LIMIT 10;"

# Descargar reporte
FECHA=$(date +%Y-%m-%d)
aws s3 cp s3://tfm-datalake/results/$FECHA/report.json /tmp/report.json && \
    cat /tmp/report.json | jq .
```

---

## 📊 VERIFICACIÓN RÁPIDA

### Verificar que todo está OK

```bash
# 1. Verificar secretos
aws secretsmanager describe-secret --secret-id aurora/credentials

# 2. Verificar S3
aws s3 ls s3://tfm-config/
aws s3 ls s3://tfm-datalake/

# 3. Verificar Lambda
aws lambda list-functions | grep lambda_

# 4. Verificar Aurora (si tienes acceso)
psql -h $AURORA_ENDPOINT -U postgres -d tfm_db -c "\dt"
```

---

## 📈 LEER RESULTADOS FINALES

### En Aurora PostgreSQL

```bash
psql -h $AURORA_ENDPOINT -U postgres -d tfm_db

# Una vez conectado, ejecutar:
```

```sql
-- Ver señales de trading
SELECT ticker, signal, prob_up, prob_down 
FROM trading_signals 
WHERE batch_date = CURRENT_DATE
ORDER BY ticker;

-- Ver análisis de sentimiento
SELECT ticker, sentiment, AVG(confidence) as avg_confidence
FROM sentiment_scores
WHERE batch_date = CURRENT_DATE
GROUP BY ticker, sentiment;

-- Ver indicadores técnicos
SELECT ticker, close_price, rsi_14, sma_20, sma_50
FROM technical_indicators
WHERE batch_date = CURRENT_DATE;

-- Ver status del batch
SELECT * FROM batch_log WHERE batch_date = CURRENT_DATE;
```

### En S3 (Reporte JSON)

```bash
# Descargar
FECHA=$(date +%Y-%m-%d)
aws s3 cp s3://tfm-datalake/results/$FECHA/report.json /tmp/

# Ver el contenido
cat /tmp/report.json | jq '.backtesting_metrics | keys'
```

---

## 🐛 SI ALGO FALLA

### Error: "No credentials configured"

```bash
aws configure
# O
export AWS_PROFILE=your_profile_name
```

### Error: "Connection refused" a Aurora

```bash
# Verificar que Aurora está accesible
aws rds describe-db-clusters --region $AWS_REGION
```

### Error: "Lambda not found"

```bash
# Verificar que las lambdas están desplegadas
aws lambda list-functions --region $AWS_REGION | grep lambda_
```

### Error: "Access Denied" en S3

```bash
# Verificar permisos IAM
aws iam list-attached-user-policies --user-name your_user_name
```

---

## 📚 ARCHIVOS IMPORTANTES

| Archivo | Descripción |
|---------|-------------|
| `PROCEDIMIENTO_COMPLETO.md` | Guía detallada paso a paso |
| `install.sh` | Script de instalación automática |
| `database_schema.sql` | Schema de Aurora |
| `iam_policy.json` | Permisos IAM necesarios |
| `etf_universe.json` | Lista de ETFs a analizar |

---

## ⏱️ TIMELINE ESPERADO

```
Paso 1: Configuración      → 2 minutos
Paso 2: Crear infraestructura → 3 minutos
Paso 3: Setup DB            → 2 minutos
Paso 4: Desplegar lambdas   → 5 minutos
Paso 5: Ejecutar pipeline   → 2 minutos
Paso 6: Leer resultados     → 1 minuto
────────────────────────────────────
TOTAL TIEMPO:               ~15-20 minutos
```

---

## ✅ CHECKLIST FINAL

```
ANTES DE EJECUTAR:
[ ] AWS CLI instalado y configurado
[ ] psql instalado
[ ] Aurora cluster creado
[ ] Finnhub API key obtenida

DURANTE:
[ ] Notar tus valores de configuración
[ ] Verificar que no hay errores
[ ] Monitorear logs en CloudWatch

DESPUÉS:
[ ] Verificar datos en Aurora
[ ] Descargar reporte de S3
[ ] Revisar señales de trading
```

---

## 🎯 PRÓXIMOS PASOS

1. **Inmediato:**
   ```bash
   ./install.sh --full
   ```

2. **Si quieres hacerlo manualmente:**
   ```bash
   # Leer PROCEDIMIENTO_COMPLETO.md
   cat PROCEDIMIENTO_COMPLETO.md | less
   ```

3. **Para debugging:**
   ```bash
   # Ver logs
   aws logs tail /aws/lambda/lambda_ingestion --follow --region $AWS_REGION
   ```

---

**¡Listo! Ahora ejecuta:**

```bash
cd /sessions/sharp-peaceful-goldberg/mnt/tfm
./install.sh --full
```

O si prefieres hacerlo paso a paso, lee `PROCEDIMIENTO_COMPLETO.md` 📖
