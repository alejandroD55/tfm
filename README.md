# TFM - Trading Strategy System with AWS Lambda

Sistema de análisis y trading de ETFs usando AWS Lambda, Aurora PostgreSQL, Hugging Face y análisis bayesiano.

## Estructura del Proyecto

```
tfm/
├── lambda_ingestion/
│   ├── lambda_ingestion.py
│   └── requirements.txt
├── lambda_sentiment/
│   ├── lambda_sentiment.py
│   └── requirements.txt
├── lambda_indicators/
│   ├── lambda_indicators.py
│   └── requirements.txt
├── lambda_bayesian/
│   ├── lambda_bayesian.py
│   └── requirements.txt
├── lambda_report/
│   ├── lambda_report.py
│   └── requirements.txt
└── README.md
```

## Descripción de los Componentes

### λ1 - lambda_ingestion

**Función**: Ingesta de datos de mercado y noticias

**Responsabilidades**:
- Lee configuración de tickers desde `s3://tfm-unir-config/etf_universe.json`
- Descarga 30 días de datos OHLCV usando yfinance
- Obtiene últimas 24 horas de noticias desde Finnhub API
- Guarda OHLCV como CSV en `s3://tfm-unir-datalake/raw/YYYY-MM-DD/ohlcv.csv`
- Guarda noticias como JSON en `s3://tfm-unir-datalake/raw/YYYY-MM-DD/news.json`
- Registra evento en tabla Aurora `batch_log` con estado `STARTED`

**Dependencias**:
- yfinance, requests, psycopg2, pandas

**Secretos requeridos**:
- `aurora/credentials` (host, port, username, password, dbname)
- `finnhub/api_key` (api_key)

---

### λ2 - lambda_sentiment

**Función**: Análisis de sentimiento usando Hugging Face

**Responsabilidades**:
- Lee noticias de `s3://tfm-unir-datalake/raw/YYYY-MM-DD/news.json`
- Analiza sentimiento de cada titular usando FinBERT via Hugging Face Serverless API
- Retorna: sentiment (bullish/bearish/neutral), confidence (0-1), justification
- Inserta resultados en tabla Aurora `sentiment_scores`

**Dependencias**:
- psycopg2

**Secretos requeridos**:
- `aurora/credentials` (host, port, username, password, dbname)

---

### λ3 - lambda_indicators

**Función**: Cálculo de indicadores técnicos

**Responsabilidades**:
- Lee datos OHLCV de `s3://tfm-unir-datalake/raw/YYYY-MM-DD/ohlcv.csv`
- Calcula indicadores usando pandas-ta:
  - RSI (14 períodos)
  - SMA (20 y 50 períodos)
  - Bandas de Bollinger (20 períodos, 2 desv. est.)
- Inserta resultados en tabla Aurora `technical_indicators`

**Dependencias**:
- psycopg2, pandas, pandas-ta

**Secretos requeridos**:
- `aurora/credentials` (host, port, username, password, dbname)

---

### λ4 - lambda_bayesian

**Función**: Inferencia bayesiana para señales de trading

**Responsabilidades**:
- Construye Red Bayesiana con 4 nodos padres:
  - **Sentiment**: bullish, bearish, neutral
  - **RSI**: oversold (<30), neutral (30-70), overbought (>70)
  - **Trend**: uptrend (SMA20 > SMA50), downtrend
  - **Volatility**: low, high (basado en ancho de bandas de Bollinger)
- Nodo hijo: **MarketDirection** (up, down)
- Realiza inferencia Variable Elimination
- Genera señales:
  - **BUY**: P(MarketDirection=up) > 0.65
  - **SELL**: P(MarketDirection=up) < 0.35
  - **HOLD**: otro caso
- Inserta señales en tabla Aurora `trading_signals`

**Dependencias**:
- psycopg2, pgmpy, numpy

**Secretos requeridos**:
- `aurora/credentials` (host, port, username, password, dbname)

---

### λ5 - lambda_report

**Función**: Generación de reportes y backtesting

**Responsabilidades**:
- Recupera señales de trading y sentimientos de últimos 90 días
- Calcula métricas de backtesting:
  - **Cumulative Return**: retorno acumulado asumiendo capital inicial de 10,000 USD
  - **Sharpe Ratio Anualizado**: usando tasa libre de riesgo del 2%
  - **Maximum Drawdown**: mayor caída peak-to-trough
- Ensambla reporte en JSON
- Guarda en `s3://tfm-unir-datalake/results/YYYY-MM-DD/report.json`
- Actualiza `batch_log` con estado `COMPLETED`

**Dependencias**:
- psycopg2, pandas, numpy

**Secretos requeridos**:
- `aurora/credentials` (host, port, username, password, dbname)

---

## Configuración Requerida

### Base de Datos Aurora

Crear las siguientes tablas:

```sql
CREATE TABLE batch_log (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    tickers_processed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sentiment_scores (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    headline TEXT NOT NULL,
    sentiment VARCHAR(20) NOT NULL,
    confidence FLOAT NOT NULL,
    justification TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE technical_indicators (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    close_price FLOAT,
    rsi_14 FLOAT,
    sma_20 FLOAT,
    sma_50 FLOAT,
    bb_upper FLOAT,
    bb_middle FLOAT,
    bb_lower FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trading_signals (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    signal VARCHAR(10) NOT NULL,
    prob_up FLOAT,
    prob_down FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Buckets S3

- `tfm-unir-config`: Contiene `etf_universe.json` con lista de tickers
- `tfm-unir-datalake`: Almacena datos crudos (`raw/YYYY-MM-DD/`) y resultados (`results/YYYY-MM-DD/`)

### Secretos AWS Secrets Manager

**`aurora/credentials`**:
```json
{
  "host": "your-aurora-endpoint.rds.amazonaws.com",
  "port": 5432,
  "username": "postgres",
  "password": "your-password",
  "dbname": "your-database"
}
```

**`finnhub/api_key`**:
```json
{
  "api_key": "your-finnhub-api-key"
}
```

---

## Despliegue en AWS Lambda

### 1. Preparar el paquete de deployment

```bash
# Para cada lambda
cd lambda_ingestion
pip install -r requirements.txt -t .
zip -r ../lambda_ingestion.zip .
cd ..
```

### 2. Crear las funciones Lambda

```bash
# Ejemplo para lambda_ingestion
aws lambda create-function \
  --function-name lambda_ingestion \
  --runtime python3.11 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --handler lambda_ingestion.handler \
  --zip-file fileb://lambda_ingestion.zip \
  --timeout 300 \
  --memory-size 512
```

### 3. Configurar permisos IAM

La rol de ejecución de Lambda necesita permisos para:
- S3 (GetObject, PutObject)
- Secrets Manager (GetSecretValue)
- Aurora (conexión via VPC)
- Bedrock (InvokeModel) - solo para lambda_sentiment

---

## Orquestación

Se recomienda usar AWS Step Functions para orquestar la ejecución:

1. `lambda_ingestion` → Ingesta inicial
2. `lambda_sentiment` → Análisis de sentimiento (paralelo con λ3)
3. `lambda_indicators` → Indicadores técnicos (paralelo con λ2)
4. `lambda_bayesian` → Señales de trading
5. `lambda_report` → Reportes y backtesting

---

## Monitoreo

- CloudWatch Logs: Todos los logs se escriben en grupos de log automáticos
- CloudWatch Metrics: Monitorear invocaciones exitosas/fallidas
- X-Ray: Rastreo distribuido de las transacciones

---

## Notas Importantes

- Los datos de OHLCV se descargan para los últimos 30 días
- Las noticias se obtienen para las últimas 24 horas
- Los análisis de sentimiento usan FinBERT via Hugging Face
- La red bayesiana utiliza Conditional Probability Tables predefinidas
- El backtesting asume 100% del capital en cada posición
- Todas las fechas usan formato YYYY-MM-DD

---

## Troubleshooting

**Error de conexión a Aurora**: Verificar VPC, subnet groups y security groups
**Error de Hugging Face**: Verificar que la API Key está activa y el modelo ProsusAI/finbert no está caído.
**Error de S3**: Verificar nombres de buckets y permisos IAM
**Error de Secretos**: Verificar que los secretos existan en Secrets Manager en la misma región
