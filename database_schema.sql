-- Schema para Aurora PostgreSQL - TFM Trading System
-- Ejecutar como superuser o con permisos CREATE TABLE

-- Tabla de registro de lotes (batches)
CREATE TABLE IF NOT EXISTS batch_log (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL,
    tickers_processed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

-- Permisos (ajustar según usuario)
-- GRANT SELECT, INSERT, UPDATE ON batch_log TO lambda_user;
-- GRANT SELECT, INSERT ON sentiment_scores TO lambda_user;
-- GRANT SELECT, INSERT ON technical_indicators TO lambda_user;
-- GRANT SELECT, INSERT ON trading_signals TO lambda_user;
-- GRANT SELECT ON latest_signals TO lambda_user;
-- GRANT SELECT ON signal_history TO lambda_user;
