# TFM — Sistema de Trading Algorítmico con AWS

Pipeline de análisis y trading de ETFs basado en red bayesiana, FinBERT y AWS.

## Universo monitorizado

El sistema procesa **4 tickers** definidos en `etf_universe.json` (fuente de verdad del pipeline):

| Ticker | Tipo | Rol en el TFM |
| ------ | ---- | -------------- |
| `SPY`  | Índice S&P 500 (large cap) | **Contraejemplo**: índice en tendencia alcista persistente donde la estrategia de entrada/salida destruye alpha frente a buy-and-hold. |
| `IWM`  | Russell 2000 (small caps) | Caso útil: alta volatilidad, sensible a tasas y macro, comportamiento mean-reverting. |
| `XLE`  | Sector energía | Caso útil: news-driven (OPEC, crudo, geopolítica), ciclos claros y RSI eficaz. |
| `GLD`  | Oro (refugio) | Caso útil: sensible a inflación, tasas reales y geopolítica; volatilidad y noticias macro relevantes. |

> Las Lambdas y la API leen `etf_universe.json` empaquetado en la imagen o, en AWS,
> `s3://tfm-unir-config/etf_universe.json`. Tras editar el fichero en el repo,
> súbelo a S3 y redespliega `lambda_ingestion` para aplicar cambios.

---

## Arquitectura

```
                    ┌─────────────────────────────────────────┐
                    │           Step Functions                 │
                    │  (orquesta el pipeline diariamente)      │
                    └──────────────┬──────────────────────────┘
                                   │
          ┌────────────────────────▼─────────────────────────┐
          │                 λ1 Ingesta                        │
          │  yfinance (OHLCV 30d) + Finnhub (noticias 24h)   │
          │         → S3: raw/{DATE}/ohlcv.csv                │
          │         → S3: raw/{DATE}/news.json                │
          └────────────┬────────────────────┬────────────────┘
                       │   (paralelo)       │
          ┌────────────▼──────┐   ┌─────────▼──────────────┐
          │  λ2 Sentimiento   │   │  λ3 Indicadores        │
          │  FinBERT (HF API) │   │  RSI14 · SMA20/50 · BB │
          │  → Aurora:        │   │  → Aurora:             │
          │  sentiment_scores │   │  technical_indicators  │
          └────────────┬──────┘   └─────────┬──────────────┘
                       └──────────┬──────────┘
                                  │
                    ┌─────────────▼────────────────────────┐
                    │        λ4 Red Bayesiana               │
                    │  pgmpy · Variable Elimination         │
                    │  Evidencias: Sentiment·RSI·Trend·Vol  │
                    │  → BUY si P(up)>65% / SELL si <35%   │
                    │  → Aurora: trading_signals            │
                    │  → Aurora: signal_explanations        │
                    └─────────────┬────────────────────────┘
                                  │
                    ┌─────────────▼────────────────────────┐
                    │         λ5 Reporte                    │
                    │  Backtesting 90d · Sharpe · Drawdown  │
                    │  → S3: results/{DATE}/report.json     │
                    └──────────────────────────────────────┘

  ┌───────────────────────────────────────────────────────────┐
  │                      EKS Cluster                         │
  │                                                           │
  │  [Pod: frontend]  nginx:80                                │
  │    /              → Angular SPA (dashboard)               │
  │    /api/          → proxy → [Pod: api] FastAPI:8000       │
  │                                  │                        │
  │                                  └──► S3 tfm-unir-datalake│
  └───────────────────────────────────────────────────────────┘
```

---

## Estructura del repositorio

```
tfm/
├── lambda_ingestion/          λ1 — Ingesta OHLCV y noticias
├── lambda_sentiment/          λ2 — Análisis sentimiento FinBERT
├── lambda_indicators/         λ3 — Indicadores técnicos
├── lambda_bayesian/           λ4 — Red bayesiana pgmpy
├── lambda_report/             λ5 — Backtesting y report.json
│
├── infrastructure/
│   ├── lambdas/               Dockerfiles para cada Lambda (ECR)
│   └── k8s/
│       ├── api/               Pod FastAPI (Dockerfile + manifests)
│       ├── frontend/          Pod nginx+Angular (Dockerfile + nginx.conf + manifests)
│       └── jobs/              K8s Job de migración Aurora
│
├── Sentiment analysis/
│   └── pipeline-dashboard/   Dashboard Angular 17
│
├── database_schema.sql        Schema Aurora PostgreSQL
├── etf_universe.json          Universo TFM (SPY · IWM · XLE · GLD)
├── stepfunctions_definition.json  Orquestación Step Functions
├── iam_policy.json            Política IAM de las Lambdas
├── docker-compose.yml         Entorno de desarrollo local
├── .env.example               Plantilla de variables de entorno
│
├── deploy_aurora.sh           Crear Aurora Serverless v2 + Secrets Manager
├── deploy_lambdas.sh          Build+push ECR + versioning + alias 'live'
├── deploy_k8s.sh              Build+push + kubectl apply (frontend + api)
└── deploy_all.sh              Orquestador completo (Aurora→Lambdas→K8s→Migrate)
```

---

## Despliegue

### Prerrequisitos

- AWS CLI configurado con permisos suficientes
- Docker Desktop en ejecución
- `kubectl` instalado
- Node.js 20+ (para el build Angular)

### Primera vez — despliegue completo

```bash
cp .env.example .env   # editar con tus valores AWS

./deploy_all.sh \
  --region     eu-north-1 \
  --account    123456789012 \
  --role-arn   arn:aws:iam::123456789012:role/tfm-lambda-role \
  --vpc-id     vpc-0abc1234 \
  --subnets    subnet-aaa,subnet-bbb,subnet-ccc \
  --eks-sg     sg-0eks12345 \
  --cluster    tfm-eks-cluster \
  --api-key    mi-api-key-segura
```

### Despliegues parciales

```bash
# Solo las Lambdas (todas):
./deploy_lambdas.sh eu-north-1 123456789012 arn:aws:iam::123456789012:role/tfm-lambda-role

# Solo una Lambda concreta:
./deploy_lambdas.sh eu-north-1 123456789012 arn:aws:iam::... lambda_bayesian

# Solo el dashboard EKS (frontend + api):
./deploy_k8s.sh eu-north-1 123456789012 tfm-eks-cluster mi-api-key

# Solo Aurora (primera vez):
./deploy_aurora.sh eu-north-1 123456789012 vpc-0abc sg-0eks subnet-a,subnet-b
```

### Desarrollo local

```bash
cp .env.example .env                # rellenar credenciales AWS para S3
docker compose up --build           # PostgreSQL + API + Frontend

# http://localhost:8080  → Dashboard Angular
# http://localhost:8000  → FastAPI (Swagger en /docs)
# localhost:5432         → PostgreSQL (schema ya aplicado)

docker compose --profile tools up   # añade pgAdmin en http://localhost:5050
```

---

## CI/CD

Cada push a `master` despliega automáticamente solo lo que cambió:

| Ficheros modificados                       | Job                      |
| ------------------------------------------ | ------------------------ |
| `lambda_ingestion/**`                      | deploy-lambda-ingestion  |
| `lambda_sentiment/**`                      | deploy-lambda-sentiment  |
| `lambda_indicators/**`                     | deploy-lambda-indicators |
| `lambda_bayesian/**`                       | deploy-lambda-bayesian   |
| `lambda_report/**`                         | deploy-lambda-report     |
| `infrastructure/k8s/api/**`                | deploy-api               |
| `Sentiment analysis/pipeline-dashboard/**` | deploy-frontend          |
| `database_schema.sql`                      | migrate (K8s Job en VPC) |

**Secrets en GitHub** (`Settings → Secrets → Actions`):

| Secret               | Descripción                     |
| -------------------- | ------------------------------- |
| `AWS_ROLE_TO_ASSUME` | ARN del rol OIDC                |
| `LAMBDA_ROLE_ARN`    | ARN del rol de ejecución Lambda |
| `DASHBOARD_API_KEY`  | API Key del pod API             |

**Variables** (`Settings → Variables → Actions`): `AWS_REGION`, `AWS_ACCOUNT_ID`, `EKS_CLUSTER_NAME`

---

## Almacenamiento

| Recurso                                             | Contenido                       |
| --------------------------------------------------- | ------------------------------- |
| `s3://tfm-unir-config/etf_universe.json`            | Universo TFM (SPY, IWM, XLE, GLD) |
| `s3://tfm-unir-datalake/raw/{DATE}/ohlcv.csv`       | Datos OHLCV diarios             |
| `s3://tfm-unir-datalake/raw/{DATE}/news.json`       | Noticias financieras            |
| `s3://tfm-unir-datalake/results/{DATE}/report.json` | Report diario                   |
| Aurora: `sentiment_scores`                          | Puntuaciones FinBERT por ticker |
| Aurora: `technical_indicators`                      | RSI, SMA, Bollinger             |
| Aurora: `trading_signals`                           | BUY/SELL/HOLD + P(up)/P(down)   |
| Aurora: `signal_explanations`                       | Evidencias bayesianas           |
| Aurora: `pipeline_kpis`                             | Métricas por etapa Lambda       |
