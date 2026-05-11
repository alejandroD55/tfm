-- Schema para Aurora PostgreSQL - TFM Trading System
-- Ejecutar como superuser o con permisos CREATE TABLE

-- Tabla de registro de lotes (batches)
CREATE TABLE IF NOT EXISTS batch_log (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    run_id TEXT,
    trigger_type VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    execution_name TEXT,
    requested_tickers JSONB NOT NULL DEFAULT '[]'::jsonb,
    status VARCHAR(50) NOT NULL,
    tickers_processed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT batch_log_trigger_type_check CHECK (trigger_type IN ('manual', 'scheduled')),
    CONSTRAINT status_check CHECK (status IN ('STARTED', 'COMPLETED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_batch_log_date ON batch_log(batch_date);
CREATE INDEX IF NOT EXISTS idx_batch_log_status ON batch_log(status);

-- Tabla de análisis de sentimiento
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
    CONSTRAINT ticker_length CHECK (LENGTH(ticker) > 0),
    UNIQUE(batch_date, ticker, headline)
);

CREATE INDEX IF NOT EXISTS idx_sentiment_date_ticker ON sentiment_scores(batch_date, ticker);
CREATE INDEX IF NOT EXISTS idx_sentiment_sentiment ON sentiment_scores(sentiment);

-- Tabla de indicadores técnicos
CREATE TABLE IF NOT EXISTS technical_indicators (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT rsi_check CHECK (rsi_14 >= 0 AND rsi_14 <= 100),
    UNIQUE(batch_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_indicators_date_ticker ON technical_indicators(batch_date, ticker);

-- Tabla de señales de trading
CREATE TABLE IF NOT EXISTS trading_signals (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    signal VARCHAR(10) NOT NULL,
    prob_up FLOAT,
    prob_down FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT signal_check CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    CONSTRAINT probability_check CHECK (prob_up >= 0 AND prob_up <= 1 AND prob_down >= 0 AND prob_down <= 1),
    UNIQUE(batch_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_signals_date_ticker ON trading_signals(batch_date, ticker);
CREATE INDEX IF NOT EXISTS idx_signals_signal ON trading_signals(signal);

-- Tabla de explicabilidad de senales
CREATE TABLE IF NOT EXISTS signal_explanations (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    sentiment_state VARCHAR(20) NOT NULL,
    rsi_state VARCHAR(20) NOT NULL,
    trend_state VARCHAR(20) NOT NULL,
    volatility_state VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batch_date, ticker),
    CONSTRAINT explain_sentiment_check CHECK (sentiment_state IN ('bullish', 'bearish', 'neutral')),
    CONSTRAINT explain_rsi_check CHECK (rsi_state IN ('oversold', 'neutral', 'overbought')),
    CONSTRAINT explain_trend_check CHECK (trend_state IN ('uptrend', 'downtrend')),
    CONSTRAINT explain_volatility_check CHECK (volatility_state IN ('low', 'high'))
);

CREATE INDEX IF NOT EXISTS idx_signal_explanations_date_ticker ON signal_explanations(batch_date, ticker);

-- Tabla de KPIs operativos por etapa de pipeline
CREATE TABLE IF NOT EXISTS pipeline_kpis (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    run_id TEXT,
    trigger_type VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    stage VARCHAR(50) NOT NULL,
    metrics JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pipeline_kpis_trigger_type_check CHECK (trigger_type IN ('manual', 'scheduled')),
    UNIQUE(run_id, stage)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_kpis_date_stage ON pipeline_kpis(batch_date, stage);
CREATE INDEX IF NOT EXISTS idx_batch_log_date_trigger ON batch_log(batch_date, trigger_type);
CREATE INDEX IF NOT EXISTS idx_batch_log_run_id ON batch_log(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_kpis_run_id ON pipeline_kpis(run_id);

-- Vista para reportes: Últimas señales por ticker
CREATE OR REPLACE VIEW latest_signals AS
SELECT
    ts.batch_date,
    ts.ticker,
    ts.signal,
    ts.prob_up,
    ts.prob_down,
    ss.sentiment,
    ss.confidence,
    ti.close_price,
    ti.rsi_14,
    ti.sma_20,
    ti.sma_50
FROM trading_signals ts
LEFT JOIN sentiment_scores ss ON ts.batch_date = ss.batch_date AND ts.ticker = ss.ticker
LEFT JOIN technical_indicators ti ON ts.batch_date = ti.batch_date AND ts.ticker = ti.ticker
WHERE ts.batch_date = (SELECT MAX(batch_date) FROM trading_signals);

-- Vista para análisis histórico
CREATE OR REPLACE VIEW signal_history AS
SELECT
    ts.batch_date,
    ts.ticker,
    ts.signal,
    ts.prob_up,
    COUNT(*) OVER (PARTITION BY ts.ticker ORDER BY ts.batch_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as consecutive_signals
FROM trading_signals ts
ORDER BY ts.batch_date DESC, ts.ticker;

-- Función para actualizar updated_at en batch_log
CREATE OR REPLACE FUNCTION update_batch_log_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para actualizar timestamp
DROP TRIGGER IF EXISTS batch_log_update_timestamp ON batch_log;
CREATE TRIGGER batch_log_update_timestamp
BEFORE UPDATE ON batch_log
FOR EACH ROW
EXECUTE FUNCTION update_batch_log_timestamp();

-- Función para validar datos de trading_signals
CREATE OR REPLACE FUNCTION validate_probabilities()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.prob_up + NEW.prob_down > 1.01 OR NEW.prob_up + NEW.prob_down < 0.99 THEN
        RAISE EXCEPTION 'prob_up + prob_down must equal 1.0';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para validar probabilidades
DROP TRIGGER IF EXISTS validate_probabilities_trigger ON trading_signals;
CREATE TRIGGER validate_probabilities_trigger
BEFORE INSERT OR UPDATE ON trading_signals
FOR EACH ROW
EXECUTE FUNCTION validate_probabilities();

-- Trigger para timestamp en pipeline_kpis
DROP TRIGGER IF EXISTS pipeline_kpis_update_timestamp ON pipeline_kpis;
CREATE TRIGGER pipeline_kpis_update_timestamp
BEFORE UPDATE ON pipeline_kpis
FOR EACH ROW
EXECUTE FUNCTION update_batch_log_timestamp();

-- Compatibilidad incremental para esquemas ya existentes
ALTER TABLE batch_log ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE batch_log ADD COLUMN IF NOT EXISTS trigger_type VARCHAR(20) NOT NULL DEFAULT 'scheduled';
ALTER TABLE batch_log ADD COLUMN IF NOT EXISTS execution_name TEXT;
ALTER TABLE batch_log ADD COLUMN IF NOT EXISTS requested_tickers JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE batch_log DROP CONSTRAINT IF EXISTS batch_log_batch_date_key;
ALTER TABLE batch_log DROP CONSTRAINT IF EXISTS batch_log_trigger_type_check;
ALTER TABLE batch_log
  ADD CONSTRAINT batch_log_trigger_type_check CHECK (trigger_type IN ('manual', 'scheduled'));

UPDATE batch_log
SET run_id = COALESCE(run_id, 'legacy-' || id::text);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'batch_log_run_id_key'
    ) THEN
        ALTER TABLE batch_log ADD CONSTRAINT batch_log_run_id_key UNIQUE (run_id);
    END IF;
END $$;

ALTER TABLE pipeline_kpis ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE pipeline_kpis ADD COLUMN IF NOT EXISTS trigger_type VARCHAR(20) NOT NULL DEFAULT 'scheduled';
ALTER TABLE pipeline_kpis DROP CONSTRAINT IF EXISTS pipeline_kpis_batch_date_stage_key;
ALTER TABLE pipeline_kpis DROP CONSTRAINT IF EXISTS pipeline_kpis_trigger_type_check;
ALTER TABLE pipeline_kpis
  ADD CONSTRAINT pipeline_kpis_trigger_type_check CHECK (trigger_type IN ('manual', 'scheduled'));

UPDATE pipeline_kpis
SET run_id = COALESCE(run_id, 'legacy-' || id::text);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'pipeline_kpis_run_id_stage_key'
    ) THEN
        ALTER TABLE pipeline_kpis ADD CONSTRAINT pipeline_kpis_run_id_stage_key UNIQUE (run_id, stage);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_batch_log_date_trigger ON batch_log(batch_date, trigger_type);
CREATE INDEX IF NOT EXISTS idx_batch_log_run_id ON batch_log(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_kpis_run_id ON pipeline_kpis(run_id);

-- Reparación preventiva de secuencias SERIAL (evita colisiones de PK tras cargas manuales/restores)
SELECT setval(pg_get_serial_sequence('batch_log', 'id'), COALESCE((SELECT MAX(id) FROM batch_log), 1), true);
SELECT setval(pg_get_serial_sequence('sentiment_scores', 'id'), COALESCE((SELECT MAX(id) FROM sentiment_scores), 1), true);
SELECT setval(pg_get_serial_sequence('technical_indicators', 'id'), COALESCE((SELECT MAX(id) FROM technical_indicators), 1), true);
SELECT setval(pg_get_serial_sequence('trading_signals', 'id'), COALESCE((SELECT MAX(id) FROM trading_signals), 1), true);
SELECT setval(pg_get_serial_sequence('signal_explanations', 'id'), COALESCE((SELECT MAX(id) FROM signal_explanations), 1), true);
SELECT setval(pg_get_serial_sequence('pipeline_kpis', 'id'), COALESCE((SELECT MAX(id) FROM pipeline_kpis), 1), true);
