-- =============================================================================
-- Migración: signal_outcomes
-- Propósito: registrar el resultado real de cada señal generada para permitir
--            la calibración estadística de las CPTs bayesianas a los 90 días.
--
-- Para cada señal se almacena:
--   - El estado de los 4 nodos de evidencia en el momento de la señal
--   - El estado de los 2 nodos macro (cuando estén disponibles)
--   - El precio de cierre del día D y de los días D+1, D+3, D+5
--   - El outcome resultante (UP/DOWN/FLAT) en cada horizonte
--
-- lambda_report rellena price_d0, signal, prob_up, nodos de evidencia el día D.
-- El día D+1, D+3 y D+5 lambda_report actualiza price_dN y outcome_dN
-- consultando yfinance para los precios de cierre correspondientes.
-- =============================================================================

CREATE TABLE IF NOT EXISTS signal_outcomes (
    id                SERIAL PRIMARY KEY,

    -- Identificación
    batch_date        DATE        NOT NULL,
    ticker            VARCHAR(20) NOT NULL,
    run_id            VARCHAR(255),

    -- Señal generada
    signal            VARCHAR(10) NOT NULL CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    prob_up           FLOAT       NOT NULL,
    prob_down         FLOAT,

    -- Nodos de evidencia en el momento de la señal (4 nodos actuales)
    sentiment_state   VARCHAR(20),   -- bullish / neutral / bearish
    rsi_state         VARCHAR(20),   -- oversold / neutral / overbought
    trend_state       VARCHAR(20),   -- uptrend / downtrend
    volatility_state  VARCHAR(20),   -- low / high

    -- Nodos macro (se rellenan cuando lambda_macro_context esté operativo)
    macro_sentiment   VARCHAR(20),   -- bullish / neutral / bearish
    risk_regime       VARCHAR(20),   -- RISK_ON / NEUTRAL / RISK_OFF
    macro_adjustment  FLOAT,         -- ajuste aplicado a prob_up (ej: +0.08)

    -- Precios de referencia
    price_d0          FLOAT,         -- cierre del día de la señal
    price_d1          FLOAT,         -- cierre D+1
    price_d3          FLOAT,         -- cierre D+3
    price_d5          FLOAT,         -- cierre D+5

    -- Outcomes calculados (UP si >+0.5%, DOWN si <-0.5%, FLAT en otro caso)
    outcome_d1        VARCHAR(10) CHECK (outcome_d1 IN ('UP', 'DOWN', 'FLAT', NULL)),
    outcome_d3        VARCHAR(10) CHECK (outcome_d3 IN ('UP', 'DOWN', 'FLAT', NULL)),
    outcome_d5        VARCHAR(10) CHECK (outcome_d5 IN ('UP', 'DOWN', 'FLAT', NULL)),

    -- Señal acertada (la señal BUY/SELL coincide con el outcome real D+1)
    -- True si BUY y outcome_d1=UP, o SELL y outcome_d1=DOWN, o HOLD y FLAT
    correct_d1        BOOLEAN,
    correct_d3        BOOLEAN,
    correct_d5        BOOLEAN,

    -- Metadata
    created_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_signal_outcomes_date_ticker UNIQUE (batch_date, ticker)
);

-- Índices para consultas de calibración
CREATE INDEX IF NOT EXISTS idx_so_date        ON signal_outcomes(batch_date);
CREATE INDEX IF NOT EXISTS idx_so_ticker      ON signal_outcomes(ticker);
CREATE INDEX IF NOT EXISTS idx_so_signal      ON signal_outcomes(signal);
CREATE INDEX IF NOT EXISTS idx_so_macro       ON signal_outcomes(macro_sentiment, risk_regime);
CREATE INDEX IF NOT EXISTS idx_so_outcome_d1  ON signal_outcomes(outcome_d1) WHERE outcome_d1 IS NOT NULL;

-- Trigger para updated_at automático
CREATE OR REPLACE FUNCTION update_signal_outcomes_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS signal_outcomes_update_timestamp ON signal_outcomes;
CREATE TRIGGER signal_outcomes_update_timestamp
    BEFORE UPDATE ON signal_outcomes
    FOR EACH ROW EXECUTE FUNCTION update_signal_outcomes_timestamp();

-- Vista de calibración: frecuencias de outcomes por combinación de nodos
-- Útil para el script de calibración de CPTs a los 90 días
CREATE OR REPLACE VIEW v_cpt_calibration AS
SELECT
    sentiment_state,
    rsi_state,
    trend_state,
    volatility_state,
    macro_sentiment,
    risk_regime,
    signal,
    COUNT(*)                                                    AS total_observations,
    SUM(CASE WHEN outcome_d1 = 'UP'   THEN 1 ELSE 0 END)       AS up_d1,
    SUM(CASE WHEN outcome_d1 = 'DOWN' THEN 1 ELSE 0 END)       AS down_d1,
    SUM(CASE WHEN outcome_d1 = 'FLAT' THEN 1 ELSE 0 END)       AS flat_d1,
    ROUND(AVG(CASE WHEN outcome_d1 = 'UP' THEN 1.0 ELSE 0.0 END)::NUMERIC, 4) AS p_up_d1,
    SUM(CASE WHEN correct_d1 = TRUE  THEN 1 ELSE 0 END)        AS correct_d1,
    ROUND(AVG(CASE WHEN correct_d1 THEN 1.0 ELSE 0.0 END)::NUMERIC, 4)        AS accuracy_d1
FROM signal_outcomes
WHERE outcome_d1 IS NOT NULL
GROUP BY
    sentiment_state, rsi_state, trend_state, volatility_state,
    macro_sentiment, risk_regime, signal
ORDER BY total_observations DESC;

COMMENT ON TABLE signal_outcomes IS
'Registro de señales generadas con sus outcomes reales. '
'Alimenta el script de calibración de CPTs bayesianas a los 90 días de operación.';

COMMENT ON VIEW v_cpt_calibration IS
'Vista de calibración: frecuencias empíricas por combinación de nodos de evidencia. '
'Usar para derivar CPTs estadísticamente válidas cuando total_observations >= 30 por fila.';
