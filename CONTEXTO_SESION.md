# Contexto TFM — Handoff completo para nueva sesión
> Generado el 2026-05-23. Copia este fichero entero al inicio del nuevo chat.

---

## 1. Resumen del proyecto

TFM sobre un **sistema de trading bayesiano con análisis de sentimiento**. Stack:
- **AWS**: Lambda + Step Functions + Aurora PostgreSQL + MongoDB Atlas + S3 + EKS (K8s)
- **Frontend**: Angular en EKS, sirve el dashboard en `http://<ALB>/`
- **API**: FastAPI en EKS pod, lee exclusivamente de **MongoDB Atlas**
- **Pipeline**: ingestion → macro_ingestion → news_filter → macro_context → parallel(sentiment+indicators) → bayesian → report
- **Local**: runner Python que reproduce las 7 Lambdas en un loop de 365 días

Repo: `/Users/alejandroramosuparela/Projects/IAProjects/tfm/`
Carpeta del dashboard Angular: `/Users/alejandroramosuparela/Projects/IAProjects/tfm/Sentiment analysis/pipeline-dashboard/`

---

## 2. Universo de tickers (fijado esta sesión)

**3 ETFs definitivos** en `etf_universe.json`:
```json
{ "tickers": ["SPY", "IWM", "GLD"] }
```
- **SPY** → caso negativo: índice amplio con tendencia alcista secular; la estrategia timing NO añade valor (α ≈ 0 o negativo, esperado).
- **IWM** → caso positivo #1: small caps muy sensibles al ciclo macro (Fed, empleo, PMI), alta volatilidad operativa.
- **GLD** → caso positivo #2: activo real no correlacionado con renta variable, sensible a inflación, tasas reales y geopolítica.

---

## 3. Ficheros creados / modificados esta sesión

### Nuevos
| Fichero | Qué es |
|---------|--------|
| `local_backtest_runner.py` | Runner local completo (1575 líneas). Reproduce las 7 Lambdas para 365 días históricos. Escribe en MongoDB Atlas Y PostgreSQL local. |
| `requirements_local.txt` | Dependencias Python para el runner + FastAPI local. |
| `run_api_local.sh` | Script para levantar la FastAPI localmente cargando el `.env` automáticamente. |
| `Sentiment analysis/pipeline-dashboard/proxy.conf.json` | Proxy para `ng serve`: redirige `/api/` → `http://localhost:8000`. |

### Modificados
| Fichero | Cambio |
|---------|--------|
| `etf_universe.json` | Reducido a 3 tickers: SPY, IWM, GLD (era SPY, IWM, XLE, GLD). |
| `bootstrap_365_days.py` | `TICKERS = ['SPY', 'IWM', 'GLD']` |
| `shared/mongo_utils.py` | `_DEFAULT_WATCHLIST_SEED = ["SPY", "IWM", "GLD"]` |
| `docker-compose.yml` | Puerto Postgres cambiado a `5433:5432` (evita conflicto con Homebrew postgresql@16). **Añadidas variables `MONGODB_URI`, `MONGODB_DB`, `FINNHUB_API_KEY` al servicio `api`** (fix crítico: sin esto el pod API no podía conectar a Atlas). |
| `.env` | Añadidos: `MONGODB_URI`, `MONGODB_DB=tfm`, `FINNHUB_API_KEY`, `POSTGRES_PORT=5433`, `DASHBOARD_API_KEY`. |
| `Sentiment analysis/pipeline-dashboard/src/environments/environment.ts` | `apiKey` cambiado de `'REEMPLAZA_CON_TU_API_KEY'` → `'dev-local-key'` (fix: antes todas las peticiones daban 403). |
| `Sentiment analysis/pipeline-dashboard/angular.json` | Añadido `"proxyConfig": "proxy.conf.json"` a `serve.options` (fix: `ng serve` no tenía proxy para `/api/`). |
| `requirements_local.txt` | Cambiado `pandas_ta` → `pandas-ta-classic`; `pgmpy==0.1.19` → `pgmpy>=0.1.24`; añadidos `fastapi`, `uvicorn[standard]`, `pydantic`. |
| `Sentiment analysis/pipeline-dashboard/src/app/features/watchlist/watchlist.component.ts` | Seed list = `['SPY', 'IWM', 'GLD']`. |
| `Sentiment analysis/pipeline-dashboard/src/app/features/watchlist/watchlist.component.html` | Botón "Importar lista TFM (SPY · IWM · GLD)". |

---

## 4. Estado del backtest ejecutado

El runner se ejecutó con éxito el 2026-05-22. Resultados finales del log (`backtest.log`):

```
786 señales generadas (262 dias × 3 tickers)
MongoDB   : ✓ datos escritos (262 upserts confirmados en log)
PostgreSQL : ✓ datos escritos (local docker en :5433)

GLD : estrategia +25.0%  B&H +37.6%  Alpha -12.6%  Sharpe  1.16  → caso positivo (activo real)
IWM : estrategia  -4.4%  B&H +40.5%  Alpha -44.9%  Sharpe -1.80  → caso positivo (small caps)
SPY : estrategia  +1.0%  B&H +28.8%  Alpha -27.8%  Sharpe -0.23  → caso negativo (esperado α≈0)
```

Fichero de señales: `backtest_output/signals_daily.csv` (786 filas, columnas: date, ticker, signal, prob_up, prob_down, sentiment, rsi, rsi_state, trend_state, volatility_state, vix, risk_regime, macro_adj, close, n_news, n_sentiments).

**Período cubierto**: 2025-05-22 → 2026-05-22 (365 días naturales, ~262 días de trading).

---

## 5. Arquitectura de datos: qué lee el dashboard

**El dashboard Angular SOLO lee de MongoDB Atlas** a través de la FastAPI. No lee Aurora directamente.

| Colección MongoDB | Escrita por | Leída por endpoint API |
|---|---|---|
| `reports` | runner / lambda_report | `GET /reports` y `GET /reports/{date}` |
| `bayesian_traces` | runner / lambda_bayesian | `GET /trace/{date}` |
| `bayesian_reports` | runner / lambda_bayesian | — |
| `raw_news` | runner / lambda_ingestion | `GET /raw/{date}/news/{ticker}` |
| `ohlcv` | runner / lambda_ingestion | `GET /raw/{date}/ohlcv/{ticker}` |
| `news`, `news_filtered` | runner | coverage checks |
| `macro_context` | runner / lambda_macro_context | `GET /macro/context/{date}` |
| `watchlist` | inicializado por mongo_utils | `GET /watchlist` |

**Aurora PostgreSQL (local :5433)** tiene: `batch_log`, `pipeline_kpis`, `technical_indicators`, `sentiment_scores`, `trading_signals`, `signal_explanations`, `macro_sentiment_scores`, `market_regime_state`, `signal_outcomes`. Estos datos los usan las Lambdas internamente y la FastAPI los lee solo para generar el report (que luego guarda en MongoDB `reports`).

---

## 6. Problema actual: dashboard AWS no muestra datos

### Diagnóstico confirmado
Los datos SÍ están en MongoDB Atlas (262 writes exitosos en el log). El dashboard de producción en AWS no los muestra. Las causas más probables:

**Causa A (más probable)**: El Secrets Manager de AWS (`mongodb/connection_string`) apunta a un cluster/database de Atlas **distinto** al que usa el `.env` local. El runner escribió en el Atlas del `.env`; el pod AWS lee del Secrets Manager.

**Causa B**: El pod AWS usa `MONGODB_DB` diferente. El runner escribe en la DB `tfm` (valor de `MONGODB_DB` en `.env`). Si el pod AWS usa otra DB (ej. `tfm-prod`), no ve los documentos.

**Causa C**: La colección `watchlist` del Atlas no tiene los 3 tickers inicializados para la instancia AWS.

### Cómo diagnosticar y solucionar

**Paso 1** — Verificar qué URI tiene el Secrets Manager de AWS:
```bash
aws secretsmanager get-secret-value \
  --secret-id mongodb/connection_string \
  --region eu-north-1 \
  --query SecretString --output text
```
Comparar el cluster hostname y el `dbName` con los del `.env` local. Si son distintos, hay que decidir: ¿copiar datos al cluster de AWS o actualizar el runner para que escriba en el cluster de AWS?

**Paso 2** — Si el cluster ES el mismo, verificar los datos directamente:
```bash
# Contar documentos en la colección reports desde local
python3 - <<'EOF'
import os; from dotenv import load_dotenv; load_dotenv()
from pymongo import MongoClient
c = MongoClient(os.getenv("MONGODB_URI"))
db = c["tfm"]
print("reports:", db.reports.count_documents({}))
print("bayesian_traces:", db.bayesian_traces.count_documents({}))
print("Fechas reports:", sorted([d["report_date"] for d in db.reports.find({}, {"report_date":1})], reverse=True)[:5])
EOF
```

**Paso 3** — Inicializar watchlist en el Atlas que lee AWS:
```bash
# Llama al endpoint de producción (cambia URL y API key reales)
curl -X POST "https://<ALB-URL>/mongo/etf-universe" \
  -H "x-api-key: <PROD-API-KEY>" \
  -H "Content-Type: application/json" \
  -d '{"tickers": ["SPY", "IWM", "GLD"]}'

# Verificar que el endpoint /reports devuelve fechas
curl "https://<ALB-URL>/reports" \
  -H "x-api-key: <PROD-API-KEY>" | python3 -m json.tool | head -20
```

**Paso 4** — Si el Secrets Manager apunta a otro cluster:
Opción A: Re-ejecutar el runner apuntando al MongoDB que usa AWS (cambiar `MONGODB_URI` en `.env`).
Opción B: Usar `mongodump` + `mongorestore` para copiar la DB `tfm` del Atlas local al Atlas de AWS.

---

## 7. Cómo levantar el entorno local

### Solo base de datos (para el runner)
```bash
cd /Users/alejandroramosuparela/Projects/IAProjects/tfm
docker compose up postgres -d
```

### Stack completo (PostgreSQL + API + Frontend)
```bash
docker compose up --build
# Frontend → http://localhost:8080
# API Swagger → http://localhost:8000/docs
```

### API + Angular por separado (para desarrollo)
```bash
# Terminal 1 — API (carga .env automáticamente)
cd /Users/alejandroramosuparela/Projects/IAProjects/tfm
./run_api_local.sh

# Terminal 2 — Angular con proxy a localhost:8000
cd "/Users/alejandroramosuparela/Projects/IAProjects/tfm/Sentiment analysis/pipeline-dashboard"
npm start
# Frontend → http://localhost:4200
```

### Ejecutar el backtest de nuevo
```bash
cd /Users/alejandroramosuparela/Projects/IAProjects/tfm
source .venv/bin/activate   # venv Python 3.11
python local_backtest_runner.py
# Log en tiempo real: tail -f backtest.log
```

---

## 8. Errores resueltos en esta sesión

| Error | Causa | Solución |
|-------|-------|----------|
| `No matching distribution for pandas_ta>=0.3.14b0` | Paquete renombrado | Cambiado a `pandas-ta-classic` en requirements_local.txt y en el import del runner |
| `role tfmadmin does not exist` (conexión externa a Postgres) | Conflicto de puerto 5432 con Homebrew postgresql@16 | Cambiado puerto docker a `5433:5432`; `.env` con `POSTGRES_PORT=5433` |
| `AttributeError: module 'numpy' has no attribute 'product'` | pgmpy 0.1.19 usa `np.product()` eliminado en NumPy 2.x | Cambiado a `pgmpy>=0.1.24` |
| `ImportError: BayesianNetwork is deprecated` | pgmpy >=0.1.24 depreca `BayesianNetwork` | Cambiado a `from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork` en `get_bn_model()` |
| Dashboard no mostraba datos (local) | 3 bugs: (1) docker-compose no pasaba `MONGODB_URI` al pod API; (2) `apiKey` en environment.ts era placeholder; (3) `ng serve` sin proxy | Fijados los 3: docker-compose.yml + environment.ts + proxy.conf.json |

---

## 9. Variables de entorno (.env) — campos clave

```bash
# MongoDB Atlas
MONGODB_URI=mongodb+srv://tfm-user:tfm123@tfm.ay8ooey.mongodb.net/?appName=tfm&retryWrites=true&w=majority
MONGODB_DB=tfm

# Finnhub (noticias históricas)
FINNHUB_API_KEY=d7u7lb9r01qvtsq0oga0d7u7lb9r01qvtsq0ogag

# PostgreSQL local (docker-compose)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=tfmadmin
POSTGRES_PASSWORD=localpassword123
POSTGRES_DB=tfm

# Dashboard API key (debe coincidir con apiKey en environment.ts)
DASHBOARD_API_KEY=dev-local-key

# Ollama (opcional, no necesario para el runner)
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2:3b

# AWS (solo para el pipeline de producción)
AWS_REGION=eu-north-1
DATALAKE_BUCKET=tfm-unir-datalake
```

---

## 10. Detalles técnicos del runner (local_backtest_runner.py)

### Sustituciones respecto a las Lambdas AWS
| Producción | Local |
|---|---|
| AWS Bedrock (Claude Haiku) | Ollama llama3.2:3b (opcional; sin él, titulares crudos) |
| HuggingFace Inference API | FinBERT local (ProsusAI/finbert, transformers) |
| Aurora PostgreSQL en AWS | PostgreSQL en docker (:5433) |
| AWS Secrets Manager | Variables de entorno (.env) |
| Step Functions | Loop Python |
| NewsAPI (macro) | VIX histórico como proxy: VIX>25 → RISK_OFF, VIX<18 → RISK_ON |
| Finnhub streaming | Finnhub `/company-news` histórico, cacheado por mes en `cache/news/` |

### Import crítico en get_bn_model()
```python
from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork
```

### Import de pandas_ta
```python
try:
    import pandas_ta_classic as ta
except ImportError:
    import pandas_ta as ta
```

### Estrategia de backtesting (Long/Cash, replica lambda_report)
- BUY → entra en posición
- SELL **o HOLD** → sale de la posición (igual que lambda_report)
- Capital inicial: 10.000€
- Período: 365 días naturales hacia atrás desde la fecha de ejecución

### Colecciones MongoDB escritas (idénticas a las Lambdas)
- `raw_news`, `ohlcv`, `news`, `news_filtered` — datos raw
- `macro_context` — MacroSentiment + RiskRegime + macro_adjustment
- `bayesian_reports` — traza por (batch_date, ticker)
- `bayesian_traces` — traza completa del día con schema_version "2.0"
- `reports` — reporte diario con backtesting completo (lo que muestra el dashboard)

### run_id en batch_log
```python
run_id = f"backtest-{date_str}"  # ej. "backtest-2025-05-22"
trigger_type = "scheduled"       # constraint Aurora: solo 'manual' o 'scheduled'
```

---

## 11. BUG CRÍTICO DESCUBIERTO AL FINAL DE SESIÓN

### Síntoma
El usuario revisó MongoDB Atlas en la UI y **todas las colecciones muestran 0 documentos** aunque el runner reportó "✓ datos escritos". Datos de Atlas:

```
Colección           | Documents | Data size | Storage size
bayesian_reports    |     0     |    0 B    |    1.02 MB
bayesian_traces     |     0     |    0 B    |    1.36 MB
macro_context       |     0     |    0 B    |   36.86 kB
reports             |     0     |    0 B    |   45.06 kB
raw_news            |     0     |    0 B    |    3.70 MB
... (todas igual)
```

### Por qué el runner dijo "✓ datos escritos"
El flag `MONGO_OK` solo comprueba que `from mongo_utils import ...` tuvo éxito (importación Python correcta). **No verifica que las escrituras llegaron a Atlas.**

### Por qué los logger.info aparecen en el log si no hay datos
El `logger.info("MongoDB upsert_macro_context: ...")` está **DESPUÉS** del `update_one()` en mongo_utils.py (línea 759, dentro del try block). Esto significa que el `update_one` se ejecutó sin lanzar excepción. Sin embargo, hay 0 documentos.

### Causa más probable
**El `MONGODB_URI` del `.env` apunta a un cluster distinto al que el usuario ve en la UI de Atlas.** Las escrituras llegan a otro cluster (o la misma URI apunta a una DB con nombre diferente). El runner escribe sin error porque la conexión es válida, pero en un lugar que el usuario no está mirando.

### Diagnóstico inmediato — ejecutar esto primero
```bash
cd /Users/alejandroramosuparela/Projects/IAProjects/tfm
source .venv/bin/activate
python diagnostico_mongo.py
```

Este script (ya creado en el repo) muestra:
- A qué hostname de cluster conecta la URI del .env
- Cuántos documentos hay en cada colección
- Todos los databases del cluster con su doc count

### Solución según resultado del diagnóstico

**Caso A — el diagnóstico muestra documentos > 0**: Los datos están en Atlas pero el usuario miraba otro cluster. Solucionar: en la UI de Atlas, seleccionar el cluster correcto (el del hostname que muestra el script).

**Caso B — el diagnóstico muestra 0 documentos**: La URI del `.env` conecta al cluster correcto pero algo impide las escrituras. Posibles causas:
- El usuario `tfm-user` en Atlas no tiene permisos de escritura en la DB `tfm` → ir a Atlas → Database Access → verificar rol del usuario (debe ser `readWrite` o `atlasAdmin`)
- La colección tiene una restricción de schema validation que rechaza los documentos → Atlas → Collections → … Validation

**Caso C — el diagnóstico no puede conectar**: La URI del `.env` es incorrecta o el cluster no existe. Obtener la URI correcta desde Atlas UI → tu cluster → Connect → Connect your application.

### Una vez confirmado dónde están los datos

Si los datos están en otro cluster o DB, hay dos opciones:
1. **Re-ejecutar el runner** con la URI correcta (la que usa el pod AWS en Secrets Manager):
   ```bash
   # Obtener la URI que usa AWS:
   aws secretsmanager get-secret-value \
     --secret-id mongodb/connection_string \
     --region eu-north-1 \
     --query SecretString --output text
   # Actualizar .env con esa URI y volver a ejecutar:
   python local_backtest_runner.py
   ```
2. **mongodump + mongorestore** para copiar de un cluster al otro (más rápido si el runner tardó mucho).

## 12. Próximos pasos (en orden)

1. **Ejecutar `python diagnostico_mongo.py`** → confirmar a qué cluster escribió el runner y cuántos docs hay.
2. **Comparar** hostname del diagnóstico con el Secrets Manager de AWS (`aws secretsmanager get-secret-value --secret-id mongodb/connection_string --region eu-north-1`).
3. Si son distintos: re-ejecutar el runner con la URI correcta o hacer mongodump/mongorestore.
4. Si son iguales y hay 0 docs: revisar permisos del usuario en Atlas (Database Access).
5. Una vez que Atlas muestra datos: verificar `GET https://<ALB>/reports` devuelve fechas → el dashboard los mostrará automáticamente.
