-- =============================================================================
-- Migración: tablas macro — MacroSentiment + RiskRegime
-- =============================================================================

CREATE TABLE IF NOT EXISTS macro_sentiment_scores (
    id               SERIAL PRIMARY KEY,
    batch_date       DATE        NOT NULL,
    run_id           VARCHAR(255),
    macro_sentiment  VARCHAR(20) NOT NULL CHECK (macro_sentiment IN ('bullish','neutral','bearish')),
    score            FLOAT       NOT NULL,
    n_articles       INTEGER     DEFAULT 0,
    created_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_macro_sentiment_date UNIQUE (batch_date)
);

CREATE TABLE IF NOT EXISTS market_regime_state (
    id               SERIAL PRIMARY KEY,
    batch_date       DATE        NOT NULL,
    run_id           VARCHAR(255),
    risk_regime      VARCHAR(20) NOT NULL CHECK (risk_regime IN ('RISK_ON','NEUTRAL','RISK_OFF')),
    macro_adjustment FLOAT       NOT NULL DEFAULT 0.0,
    vix              FLOAT,
    created_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_market_regime_date UNIQUE (batch_date)
);

CREATE INDEX IF NOT EXISTS idx_macro_sentiment_date ON macro_sentiment_scores(batch_date);
CREATE INDEX IF NOT EXISTS idx_market_regime_date   ON market_regime_state(batch_date);

-- Vista combinada útil para análisis y dashboard
CREATE OR REPLACE VIEW v_macro_daily AS
SELECT
    ms.batch_date,
    ms.macro_sentiment,
    ms.score            AS macro_score,
    ms.n_articles,
    mr.risk_regime,
    mr.macro_adjustment,
    mr.vix
FROM macro_sentiment_scores ms
LEFT JOIN market_regime_state mr USING (batch_date)
ORDER BY ms.batch_date DESC;

COMMENT ON TABLE macro_sentiment_scores IS
'Sentimiento macro agregado calculado por lambda_macro_context (FinBERT ponderado).';

COMMENT ON TABLE market_regime_state IS
'Régimen de mercado diario: RISK_ON/NEUTRAL/RISK_OFF y ajuste aplicado a prob_up.';
